package batch

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/example/ollama-fair-gateway/internal/config"
	"github.com/example/ollama-fair-gateway/internal/state"
)

const (
	StateQueued     = "queued"
	StateRunning    = "running"
	StatePaused     = "paused"
	StatePausing    = "pausing"
	StateCancelling = "cancelling"
	StateCompleted  = "completed"
	StateFailed     = "failed"
	StateCancelled  = "cancelled"
)

var (
	ErrDisabled      = errors.New("durable batch jobs are disabled")
	ErrNotFound      = errors.New("batch job not found")
	ErrInvalidState  = errors.New("batch job state does not allow this operation")
	ErrFull          = errors.New("batch job retention store is full")
	ErrInputTooLarge = errors.New("batch input exceeds configured max_input_bytes")
)

// IdentitySnapshot preserves the authenticated metadata required to re-run a
// durable request through the normal gateway authorization/routing pipeline.
// It intentionally contains no bearer/API-key secret.
type IdentitySnapshot struct {
	Tenant      string                 `json:"tenant"`
	Subject     string                 `json:"subject"`
	Actor       string                 `json:"actor"`
	Application string                 `json:"application,omitempty"`
	AuthType    string                 `json:"auth_type"`
	ClientIP    string                 `json:"client_ip,omitempty"`
	Scopes      []string               `json:"scopes,omitempty"`
	ModelACLSet bool                   `json:"model_acl_set,omitempty"`
	ModelAccess config.ModelAccessRule `json:"model_access,omitempty"`
}

type Job struct {
	ID                  string           `json:"id"`
	Identity            IdentitySnapshot `json:"identity"`
	Path                string           `json:"path"`
	Model               string           `json:"model,omitempty"`
	ServiceClass        string           `json:"service_class"`
	InputRef            string           `json:"input_ref"`
	OutputRef           string           `json:"output_ref,omitempty"`
	State               string           `json:"state"`
	Attempts            int              `json:"attempts"`
	HTTPStatus          int              `json:"http_status,omitempty"`
	ResponseContentType string           `json:"response_content_type,omitempty"`
	ExecutionRequestID  string           `json:"execution_request_id,omitempty"`
	CreatedAt           time.Time        `json:"created_at"`
	UpdatedAt           time.Time        `json:"updated_at"`
	StartedAt           *time.Time       `json:"started_at,omitempty"`
	FinishedAt          *time.Time       `json:"finished_at,omitempty"`
	Error               string           `json:"error,omitempty"`
}

type RunResult struct {
	HTTPStatus          int
	ResponseContentType string
	RequestID           string
	Error               string
}

type Runner func(context.Context, Job, io.Reader, io.Writer) RunResult

type snapshot struct {
	Version int            `json:"version"`
	Jobs    map[string]Job `json:"jobs"`
}

type Manager struct {
	mu     sync.Mutex
	cfg    config.BatchJobsConfig
	file   state.AtomicJSON
	dir    string
	jobs   map[string]Job
	cancel map[string]context.CancelCauseFunc
	wake   chan struct{}
	active int
	runner Runner
	ctx    context.Context
	now    func() time.Time
	wg     sync.WaitGroup
}

var (
	errPause  = errors.New("batch pause requested")
	errCancel = errors.New("batch cancel requested")
)

func New(cfg config.BatchJobsConfig, metadataPath, dir string) (*Manager, error) {
	m := &Manager{cfg: cfg, file: state.AtomicJSON{Path: metadataPath, Mode: 0600}, dir: dir, jobs: map[string]Job{}, cancel: map[string]context.CancelCauseFunc{}, wake: make(chan struct{}, 1), now: time.Now}
	if !cfg.Enabled {
		return m, nil
	}
	if err := os.MkdirAll(filepath.Join(dir, "input"), 0750); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(dir, "output"), 0750); err != nil {
		return nil, err
	}
	var snap snapshot
	if err := m.file.Load(&snap); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	} else {
		if snap.Version != 1 {
			return nil, fmt.Errorf("unsupported batch metadata version %d", snap.Version)
		}
		if snap.Jobs != nil {
			m.jobs = snap.Jobs
		}
	}
	changed := m.recoverLocked()
	changed = m.pruneLocked(m.now().UTC()) || changed
	if changed {
		if err := m.saveLocked(); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func (m *Manager) Enabled() bool { return m != nil && m.cfg.Enabled }

func (m *Manager) Start(ctx context.Context, runner Runner) {
	if !m.Enabled() || runner == nil {
		return
	}
	m.mu.Lock()
	m.ctx = ctx
	m.runner = runner
	m.mu.Unlock()
	go m.loop(ctx)
	m.signal()
}

// Wait blocks until all currently running batch attempts have returned. It is
// intended for graceful shutdown after the root context has been cancelled, so
// no new attempts can be dispatched while waiting. Each finishing attempt
// persists its final restart-safe state before it releases the wait group.
func (m *Manager) Wait(ctx context.Context) error {
	if !m.Enabled() {
		return nil
	}
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) Create(id IdentitySnapshot, path, model string, body []byte) (Job, error) {
	if !m.Enabled() {
		return Job{}, ErrDisabled
	}
	if id.Tenant == "" || id.Actor == "" {
		return Job{}, errors.New("batch identity requires tenant and actor")
	}
	if path == "" || !strings.HasPrefix(path, "/") {
		return Job{}, errors.New("batch path must be an absolute gateway path")
	}
	if int64(len(body)) > m.cfg.MaxInputBytes {
		return Job{}, ErrInputTooLarge
	}
	if len(body) == 0 {
		return Job{}, errors.New("batch input body is empty")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now().UTC()
	m.pruneLocked(now)
	if len(m.jobs) >= m.cfg.MaxJobs {
		return Job{}, ErrFull
	}
	jobID, err := newID()
	if err != nil {
		return Job{}, err
	}
	inputRef := filepath.ToSlash(filepath.Join("input", jobID+".json"))
	if err := m.writeInputLocked(inputRef, body); err != nil {
		return Job{}, err
	}
	j := Job{ID: jobID, Identity: id, Path: path, Model: model, ServiceClass: "batch", InputRef: inputRef, State: StateQueued, CreatedAt: now, UpdatedAt: now}
	m.jobs[j.ID] = j
	if err := m.saveLocked(); err != nil {
		delete(m.jobs, j.ID)
		_ = os.Remove(m.refPath(inputRef))
		return Job{}, err
	}
	m.signal()
	return cloneJob(j), nil
}

func (m *Manager) List(tenant, actor string, all bool) []Job {
	if !m.Enabled() {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pruneLocked(m.now().UTC()) {
		_ = m.saveLocked()
	}
	out := make([]Job, 0, len(m.jobs))
	for _, j := range m.jobs {
		if all || (j.Identity.Tenant == tenant && j.Identity.Actor == actor) {
			out = append(out, cloneJob(j))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out
}

func (m *Manager) Get(id, tenant, actor string, all bool) (Job, bool) {
	if !m.Enabled() {
		return Job{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok || (!all && (j.Identity.Tenant != tenant || j.Identity.Actor != actor)) {
		return Job{}, false
	}
	return cloneJob(j), true
}

func (m *Manager) Pause(id, tenant, actor string, all bool) (Job, error) {
	return m.control(id, tenant, actor, all, "pause")
}

func (m *Manager) Resume(id, tenant, actor string, all bool) (Job, error) {
	return m.control(id, tenant, actor, all, "resume")
}

func (m *Manager) Cancel(id, tenant, actor string, all bool) (Job, error) {
	return m.control(id, tenant, actor, all, "cancel")
}

func (m *Manager) control(id, tenant, actor string, all bool, action string) (Job, error) {
	if !m.Enabled() {
		return Job{}, ErrDisabled
	}
	m.mu.Lock()
	j, ok := m.jobs[id]
	if !ok || (!all && (j.Identity.Tenant != tenant || j.Identity.Actor != actor)) {
		m.mu.Unlock()
		return Job{}, ErrNotFound
	}
	now := m.now().UTC()
	var cancel context.CancelCauseFunc
	switch action {
	case "pause":
		switch j.State {
		case StateQueued:
			j.State = StatePaused
		case StateRunning:
			j.State = StatePausing
			cancel = m.cancel[id]
		case StatePaused, StatePausing:
			// idempotent
		default:
			m.mu.Unlock()
			return Job{}, ErrInvalidState
		}
	case "resume":
		if j.State != StatePaused {
			m.mu.Unlock()
			return Job{}, ErrInvalidState
		}
		j.State = StateQueued
		j.Error = ""
		j.HTTPStatus = 0
		j.ResponseContentType = ""
		j.ExecutionRequestID = ""
		j.OutputRef = ""
		j.StartedAt = nil
		j.FinishedAt = nil
	case "cancel":
		switch j.State {
		case StateQueued, StatePaused:
			j.State = StateCancelled
			j.FinishedAt = ptrTime(now)
		case StateRunning, StatePausing:
			j.State = StateCancelling
			cancel = m.cancel[id]
		case StateCancelling, StateCancelled:
			// idempotent
		default:
			m.mu.Unlock()
			return Job{}, ErrInvalidState
		}
	default:
		m.mu.Unlock()
		return Job{}, errors.New("unknown batch control action")
	}
	j.UpdatedAt = now
	m.jobs[id] = j
	if err := m.saveLocked(); err != nil {
		m.mu.Unlock()
		return Job{}, err
	}
	m.mu.Unlock()
	if cancel != nil {
		if action == "pause" {
			cancel(errPause)
		} else {
			cancel(errCancel)
		}
	}
	if action == "resume" {
		m.signal()
	}
	return cloneJob(j), nil
}

func (m *Manager) OpenOutput(id, tenant, actor string, all bool) (*os.File, Job, error) {
	if !m.Enabled() {
		return nil, Job{}, ErrDisabled
	}
	m.mu.Lock()
	j, ok := m.jobs[id]
	if !ok || (!all && (j.Identity.Tenant != tenant || j.Identity.Actor != actor)) {
		m.mu.Unlock()
		return nil, Job{}, ErrNotFound
	}
	ref := j.OutputRef
	m.mu.Unlock()
	if ref == "" {
		return nil, cloneJob(j), os.ErrNotExist
	}
	f, err := os.Open(m.refPath(ref))
	return f, cloneJob(j), err
}

func (m *Manager) Compact() error {
	if !m.Enabled() {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneLocked(m.now().UTC())
	return m.saveLocked()
}

func (m *Manager) loop(ctx context.Context) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.wake:
			m.dispatch(ctx)
		case <-t.C:
			m.mu.Lock()
			changed := m.pruneLocked(m.now().UTC())
			if changed {
				_ = m.saveLocked()
			}
			m.mu.Unlock()
			m.dispatch(ctx)
		}
	}
}

func (m *Manager) dispatch(root context.Context) {
	for {
		m.mu.Lock()
		if m.runner == nil || m.active >= m.cfg.MaxConcurrent || root.Err() != nil {
			m.mu.Unlock()
			return
		}
		var chosen *Job
		for _, j := range m.jobs {
			if j.State != StateQueued {
				continue
			}
			if chosen == nil || j.CreatedAt.Before(chosen.CreatedAt) {
				jc := j
				chosen = &jc
			}
		}
		if chosen == nil {
			m.mu.Unlock()
			return
		}
		now := m.now().UTC()
		j := *chosen
		j.State = StateRunning
		j.Attempts++
		j.StartedAt = ptrTime(now)
		j.FinishedAt = nil
		j.UpdatedAt = now
		j.Error = ""
		m.jobs[j.ID] = j
		ctx, cancel := context.WithCancelCause(root)
		m.cancel[j.ID] = cancel
		m.active++
		if err := m.saveLocked(); err != nil {
			m.active--
			delete(m.cancel, j.ID)
			j.State = StateQueued
			m.jobs[j.ID] = j
			m.mu.Unlock()
			return
		}
		runner := m.runner
		m.wg.Add(1)
		m.mu.Unlock()
		go func() {
			defer m.wg.Done()
			m.runOne(ctx, j, runner)
		}()
	}
}

func (m *Manager) runOne(ctx context.Context, j Job, runner Runner) {
	input, err := os.Open(m.refPath(j.InputRef))
	if err != nil {
		m.finishRun(j.ID, RunResult{Error: "open input: " + err.Error()}, "", true)
		return
	}
	defer input.Close()

	outDir := filepath.Join(m.dir, "output")
	tmp, err := os.CreateTemp(outDir, ".batch-output-*.tmp")
	if err != nil {
		m.finishRun(j.ID, RunResult{Error: "create output: " + err.Error()}, "", true)
		return
	}
	tmpPath := tmp.Name()
	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		m.finishRun(j.ID, RunResult{Error: "chmod output: " + err.Error()}, "", true)
		return
	}
	res := runner(ctx, cloneJob(j), input, tmp)
	if err := tmp.Sync(); err != nil && res.Error == "" {
		res.Error = "sync output: " + err.Error()
	}
	if err := tmp.Close(); err != nil && res.Error == "" {
		res.Error = "close output: " + err.Error()
	}
	m.finishRun(j.ID, res, tmpPath, false)
}

func (m *Manager) finishRun(id string, res RunResult, tmpPath string, setupFailure bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		if tmpPath != "" {
			_ = os.Remove(tmpPath)
		}
		return
	}
	delete(m.cancel, id)
	if m.active > 0 {
		m.active--
	}
	now := m.now().UTC()
	j.UpdatedAt = now
	j.HTTPStatus = res.HTTPStatus
	j.ResponseContentType = res.ResponseContentType
	j.ExecutionRequestID = res.RequestID

	switch {
	case j.State == StateCancelling:
		if tmpPath != "" {
			_ = os.Remove(tmpPath)
		}
		j.State = StateCancelled
		j.FinishedAt = ptrTime(now)
		j.Error = "cancelled"
	case j.State == StatePausing:
		if tmpPath != "" {
			_ = os.Remove(tmpPath)
		}
		j.State = StatePaused
		j.StartedAt = nil
		j.Error = "paused"
	case m.ctx != nil && m.ctx.Err() != nil:
		if tmpPath != "" {
			_ = os.Remove(tmpPath)
		}
		j.State = StateQueued
		j.StartedAt = nil
		j.Error = "interrupted by gateway shutdown; queued for retry"
	case setupFailure:
		j.State = StateFailed
		j.FinishedAt = ptrTime(now)
		j.Error = res.Error
	default:
		finalRef := filepath.ToSlash(filepath.Join("output", id+".response"))
		finalPath := m.refPath(finalRef)
		_ = os.Remove(finalPath)
		if tmpPath != "" {
			if err := os.Rename(tmpPath, finalPath); err != nil {
				j.State = StateFailed
				j.FinishedAt = ptrTime(now)
				j.Error = "commit output: " + err.Error()
				break
			}
			j.OutputRef = finalRef
		}
		if res.Error != "" || res.HTTPStatus < 200 || res.HTTPStatus >= 400 {
			j.State = StateFailed
			j.Error = res.Error
			if j.Error == "" && res.HTTPStatus != 0 {
				j.Error = fmt.Sprintf("gateway HTTP %d", res.HTTPStatus)
			}
		} else {
			j.State = StateCompleted
			j.Error = ""
		}
		j.FinishedAt = ptrTime(now)
	}
	m.jobs[id] = j
	_ = m.saveLocked()
	m.signal()
}

func (m *Manager) recoverLocked() bool {
	changed := false
	now := m.now().UTC()
	for id, j := range m.jobs {
		switch j.State {
		case StateRunning:
			j.State = StateQueued
			j.StartedAt = nil
			j.Error = "recovered after gateway restart; queued for retry"
			j.UpdatedAt = now
			m.jobs[id] = j
			changed = true
		case StatePausing:
			j.State = StatePaused
			j.StartedAt = nil
			j.Error = "pause recovered after gateway restart"
			j.UpdatedAt = now
			m.jobs[id] = j
			changed = true
		case StateCancelling:
			j.State = StateCancelled
			j.FinishedAt = ptrTime(now)
			j.Error = "cancel recovered after gateway restart"
			j.UpdatedAt = now
			m.jobs[id] = j
			changed = true
		}
	}
	return changed
}

func (m *Manager) pruneLocked(now time.Time) bool {
	if m.cfg.Retention.Value() <= 0 {
		return false
	}
	changed := false
	for id, j := range m.jobs {
		if !isTerminal(j.State) || j.FinishedAt == nil || now.Sub(*j.FinishedAt) < m.cfg.Retention.Value() {
			continue
		}
		_ = os.Remove(m.refPath(j.InputRef))
		if j.OutputRef != "" {
			_ = os.Remove(m.refPath(j.OutputRef))
		}
		delete(m.jobs, id)
		changed = true
	}
	return changed
}

func (m *Manager) saveLocked() error {
	copyMap := make(map[string]Job, len(m.jobs))
	for id, j := range m.jobs {
		copyMap[id] = cloneJob(j)
	}
	return m.file.Save(snapshot{Version: 1, Jobs: copyMap})
}

func (m *Manager) writeInputLocked(ref string, body []byte) error {
	path := m.refPath(ref)
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".batch-input-*.tmp")
	if err != nil {
		return err
	}
	name := tmp.Name()
	ok := false
	defer func() {
		_ = tmp.Close()
		if !ok {
			_ = os.Remove(name)
		}
	}()
	if err := tmp.Chmod(0600); err != nil {
		return err
	}
	if _, err := tmp.Write(body); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	ok = true
	return nil
}

func (m *Manager) refPath(ref string) string {
	ref = filepath.Clean(filepath.FromSlash(ref))
	if ref == "." || filepath.IsAbs(ref) || ref == ".." || strings.HasPrefix(ref, ".."+string(filepath.Separator)) {
		return filepath.Join(m.dir, "invalid-ref")
	}
	return filepath.Join(m.dir, ref)
}

func (m *Manager) signal() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func isTerminal(state string) bool {
	switch state {
	case StateCompleted, StateFailed, StateCancelled:
		return true
	default:
		return false
	}
}

func newID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "batch_" + hex.EncodeToString(b), nil
}

func cloneJob(j Job) Job {
	j.Identity.Scopes = append([]string(nil), j.Identity.Scopes...)
	j.Identity.ModelAccess.AllowedModels = append([]string(nil), j.Identity.ModelAccess.AllowedModels...)
	j.Identity.ModelAccess.DeniedModels = append([]string(nil), j.Identity.ModelAccess.DeniedModels...)
	if j.StartedAt != nil {
		t := *j.StartedAt
		j.StartedAt = &t
	}
	if j.FinishedAt != nil {
		t := *j.FinishedAt
		j.FinishedAt = &t
	}
	return j
}

func ptrTime(t time.Time) *time.Time { return &t }
