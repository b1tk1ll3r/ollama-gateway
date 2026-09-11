package server

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"
)

type adminOperation struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Worker    string    `json:"worker"`
	Model     string    `json:"model"`
	Status    string    `json:"status"`
	Message   string    `json:"message,omitempty"`
	Completed int64     `json:"completed,omitempty"`
	Total     int64     `json:"total,omitempty"`
	Progress  float64   `json:"progress"`
	StartedAt time.Time `json:"started_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Error     string    `json:"error,omitempty"`
}

type operationManager struct {
	mu      sync.RWMutex
	ops     map[string]adminOperation
	cancel  map[string]context.CancelFunc
	maxKeep int
	client  *http.Client
}

func newOperationManager() *operationManager {
	return &operationManager{ops: map[string]adminOperation{}, cancel: map[string]context.CancelFunc{}, maxKeep: 100, client: &http.Client{Timeout: 0}}
}

func (m *operationManager) list() []adminOperation {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]adminOperation, 0, len(m.ops))
	for _, op := range m.ops {
		out = append(out, op)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.After(out[j].StartedAt) })
	return out
}

func (m *operationManager) update(id string, fn func(*adminOperation)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	op, ok := m.ops[id]
	if !ok {
		return
	}
	fn(&op)
	op.UpdatedAt = time.Now().UTC()
	m.ops[id] = op
}

func (m *operationManager) add(op adminOperation) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ops[op.ID] = op
	if len(m.ops) <= m.maxKeep {
		return
	}
	var oldest string
	var oldestTime time.Time
	for id, x := range m.ops {
		if x.Status == "running" || x.Status == "queued" {
			continue
		}
		if oldest == "" || x.StartedAt.Before(oldestTime) {
			oldest, oldestTime = id, x.StartedAt
		}
	}
	if oldest != "" {
		delete(m.ops, oldest)
	}
}

func (m *operationManager) startPull(base *url.URL, worker, model string) adminOperation {
	now := time.Now().UTC()
	op := adminOperation{ID: operationID(), Type: "pull", Worker: worker, Model: model, Status: "queued", StartedAt: now, UpdatedAt: now}
	m.add(op)
	ctx, cancel := context.WithCancel(context.Background())
	m.mu.Lock()
	m.cancel[op.ID] = cancel
	m.mu.Unlock()
	go m.runPull(ctx, op.ID, base, model)
	return op
}

func (m *operationManager) runPull(ctx context.Context, id string, base *url.URL, model string) {
	defer func() {
		m.mu.Lock()
		delete(m.cancel, id)
		m.mu.Unlock()
	}()
	m.update(id, func(op *adminOperation) { op.Status = "running"; op.Message = "starting pull" })
	body, _ := json.Marshal(map[string]any{"model": model, "stream": true})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base.String()+"/api/pull", bytes.NewReader(body))
	if err != nil {
		m.fail(id, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := m.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			m.update(id, func(op *adminOperation) { op.Status = "cancelled"; op.Message = "cancelled" })
			return
		}
		m.fail(id, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		m.fail(id, fmt.Errorf("Ollama HTTP %d: %s", resp.StatusCode, string(b)))
		return
	}
	sc := bufio.NewScanner(resp.Body)
	buf := make([]byte, 0, 64<<10)
	sc.Buffer(buf, 2<<20)
	for sc.Scan() {
		var x struct {
			Status    string `json:"status"`
			Digest    string `json:"digest"`
			Total     int64  `json:"total"`
			Completed int64  `json:"completed"`
			Error     string `json:"error"`
		}
		if json.Unmarshal(sc.Bytes(), &x) != nil {
			continue
		}
		if x.Error != "" {
			m.fail(id, fmt.Errorf("%s", x.Error))
			return
		}
		m.update(id, func(op *adminOperation) {
			op.Message = x.Status
			if x.Total > 0 {
				op.Total = x.Total
			}
			if x.Completed > 0 {
				op.Completed = x.Completed
			}
			if op.Total > 0 {
				op.Progress = float64(op.Completed) / float64(op.Total)
				if op.Progress > 1 {
					op.Progress = 1
				}
			}
		})
	}
	if err := sc.Err(); err != nil {
		if ctx.Err() != nil {
			m.update(id, func(op *adminOperation) { op.Status = "cancelled"; op.Message = "cancelled" })
			return
		}
		m.fail(id, err)
		return
	}
	m.update(id, func(op *adminOperation) { op.Status = "completed"; op.Message = "success"; op.Progress = 1 })
}

func (m *operationManager) fail(id string, err error) {
	m.update(id, func(op *adminOperation) { op.Status = "failed"; op.Error = err.Error(); op.Message = "failed" })
}

func (m *operationManager) cancelOperation(id string) bool {
	m.mu.RLock()
	cancel := m.cancel[id]
	m.mu.RUnlock()
	if cancel == nil {
		return false
	}
	cancel()
	return true
}

func (m *operationManager) modelAction(ctx context.Context, base *url.URL, action, model string) error {
	var method, path string
	var payload any
	switch action {
	case "delete":
		method, path = http.MethodDelete, "/api/delete"
		payload = map[string]string{"model": model}
	case "stop":
		// Ollama unloads a resident model by issuing a generate request with
		// keep_alive=0. There is no native /api/stop endpoint.
		method, path = http.MethodPost, "/api/generate"
		payload = map[string]any{"model": model, "keep_alive": 0, "stream": false}
	default:
		return fmt.Errorf("unsupported model action %q", action)
	}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, method, base.String()+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := m.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return fmt.Errorf("Ollama HTTP %d: %s", resp.StatusCode, string(b))
	}
	return nil
}

func operationID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
