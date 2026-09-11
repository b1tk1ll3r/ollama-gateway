package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/example/ollama-fair-gateway/internal/auth"
	"github.com/example/ollama-fair-gateway/internal/batch"
)

type batchCreateRequest struct {
	Path string          `json:"path"`
	Body json.RawMessage `json:"body"`
}

func (s *Server) batchAPI(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	if s.batchJobs == nil || !s.batchJobs.Enabled() {
		writeProtocolError(w, r, http.StatusNotFound, "batch_disabled", "durable batch jobs are disabled")
		return
	}
	base := "/gateway/v1/batches"
	rest := strings.TrimPrefix(r.URL.Path, base)
	if rest == "" || rest == "/" {
		switch r.Method {
		case http.MethodGet:
			writeJSON(w, http.StatusOK, map[string]any{"jobs": s.batchJobs.List(id.Tenant, id.Actor(), false)})
		case http.MethodPost:
			s.batchCreate(w, r, id)
		default:
			writeProtocolError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		}
		return
	}
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		writeProtocolError(w, r, http.StatusNotFound, "not_found", "batch job not found")
		return
	}
	jobID := parts[0]
	if len(parts) == 1 && r.Method == http.MethodGet {
		j, ok := s.batchJobs.Get(jobID, id.Tenant, id.Actor(), false)
		if !ok {
			writeProtocolError(w, r, http.StatusNotFound, "batch_not_found", "batch job not found")
			return
		}
		writeJSON(w, http.StatusOK, j)
		return
	}
	if len(parts) == 2 && parts[1] == "output" && r.Method == http.MethodGet {
		s.batchOutput(w, r, id, jobID, false)
		return
	}
	if len(parts) == 2 && r.Method == http.MethodPost {
		var (
			j   batch.Job
			err error
		)
		switch parts[1] {
		case "pause":
			j, err = s.batchJobs.Pause(jobID, id.Tenant, id.Actor(), false)
		case "resume":
			j, err = s.batchJobs.Resume(jobID, id.Tenant, id.Actor(), false)
		case "cancel":
			j, err = s.batchJobs.Cancel(jobID, id.Tenant, id.Actor(), false)
		default:
			writeProtocolError(w, r, http.StatusNotFound, "not_found", "unknown batch operation")
			return
		}
		if err != nil {
			s.writeBatchError(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, j)
		return
	}
	writeProtocolError(w, r, http.StatusNotFound, "not_found", "unknown batch endpoint")
}

func (s *Server) batchCreate(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	limit := s.cfg.BatchJobs.MaxInputBytes + 64<<10
	if limit <= 0 {
		limit = 16 << 20
	}
	var req batchCreateRequest
	if err := decodeJSON(r, &req, limit); err != nil {
		writeProtocolError(w, r, http.StatusBadRequest, "bad_batch", err.Error())
		return
	}
	if !s.isCompute(http.MethodPost, req.Path) {
		writeProtocolError(w, r, http.StatusBadRequest, "bad_batch_path", "batch path must be a configured compute POST endpoint")
		return
	}
	if len(req.Body) == 0 || string(req.Body) == "null" || !json.Valid(req.Body) {
		writeProtocolError(w, r, http.StatusBadRequest, "bad_batch_body", "batch body must contain a valid JSON request body")
		return
	}
	model := modelFromBody(req.Body)
	if _, _, err := s.resolveModel(r.Context(), id, model); err != nil {
		if errors.Is(err, ErrModelAccessDenied) {
			writeProtocolError(w, r, http.StatusForbidden, "model_access_denied", err.Error())
		} else {
			writeProtocolError(w, r, http.StatusNotFound, "model_alias_unavailable", err.Error())
		}
		return
	}
	j, err := s.batchJobs.Create(batchIdentitySnapshot(id), req.Path, model, req.Body)
	if err != nil {
		s.writeBatchError(w, r, err)
		return
	}
	w.Header().Set("Location", "/gateway/v1/batches/"+j.ID)
	writeJSON(w, http.StatusAccepted, j)
}

func (s *Server) batchOutput(w http.ResponseWriter, r *http.Request, id auth.Identity, jobID string, all bool) {
	f, j, err := s.batchJobs.OpenOutput(jobID, id.Tenant, id.Actor(), all)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			writeProtocolError(w, r, http.StatusConflict, "batch_output_unavailable", "batch output is not available yet")
			return
		}
		s.writeBatchError(w, r, err)
		return
	}
	defer f.Close()
	ct := strings.TrimSpace(j.ResponseContentType)
	if ct == "" {
		ct = "application/octet-stream"
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Cache-Control", "no-store")
	if st, err := f.Stat(); err == nil {
		w.Header().Set("Content-Length", strconv.FormatInt(st.Size(), 10))
	}
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, f)
}

func (s *Server) writeBatchError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, batch.ErrDisabled):
		writeProtocolError(w, r, http.StatusNotFound, "batch_disabled", err.Error())
	case errors.Is(err, batch.ErrNotFound):
		writeProtocolError(w, r, http.StatusNotFound, "batch_not_found", "batch job not found")
	case errors.Is(err, batch.ErrInvalidState):
		writeProtocolError(w, r, http.StatusConflict, "batch_state", err.Error())
	case errors.Is(err, batch.ErrFull):
		writeProtocolError(w, r, http.StatusTooManyRequests, "batch_full", err.Error())
	case errors.Is(err, batch.ErrInputTooLarge):
		writeProtocolError(w, r, http.StatusRequestEntityTooLarge, "batch_input_too_large", err.Error())
	default:
		writeProtocolError(w, r, http.StatusInternalServerError, "batch_error", err.Error())
	}
}

// ExecuteBatch is the runner used by the durable batch manager. The request is
// replayed directly into the normal compute path with the original identity
// metadata but the batch service class, so quotas, ACLs, scheduling, routing,
// metering, alerts and OpenTelemetry stay consistent with interactive traffic.
func (s *Server) ExecuteBatch(ctx context.Context, j batch.Job, input io.Reader, output io.Writer) batch.RunResult {
	id := auth.Identity{
		Tenant:       j.Identity.Tenant,
		Subject:      j.Identity.Subject,
		Application:  j.Identity.Application,
		AuthType:     j.Identity.AuthType,
		ClientIP:     j.Identity.ClientIP,
		Scopes:       make(map[string]bool, len(j.Identity.Scopes)),
		ModelACLSet:  j.Identity.ModelACLSet,
		ModelAccess:  j.Identity.ModelAccess,
		ServiceClass: "batch",
	}
	for _, scope := range j.Identity.Scopes {
		id.Scopes[scope] = true
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://gateway.local"+j.Path, input)
	if err != nil {
		return batch.RunResult{Error: err.Error()}
	}
	r.Header.Set("Content-Type", "application/json")
	r.RemoteAddr = "127.0.0.1:0"
	rw := &batchResponseWriter{header: make(http.Header), out: output}
	s.forward(rw, r, id)
	status := rw.status
	if status == 0 {
		status = http.StatusOK
	}
	return batch.RunResult{HTTPStatus: status, ResponseContentType: rw.header.Get("Content-Type"), RequestID: rw.header.Get("X-Request-ID")}
}

type batchResponseWriter struct {
	header http.Header
	out    io.Writer
	status int
}

func (w *batchResponseWriter) Header() http.Header { return w.header }
func (w *batchResponseWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}
func (w *batchResponseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.out.Write(p)
}
func (w *batchResponseWriter) Flush() {}

func batchIdentitySnapshot(id auth.Identity) batch.IdentitySnapshot {
	scopes := make([]string, 0, len(id.Scopes))
	for scope, ok := range id.Scopes {
		if ok {
			scopes = append(scopes, scope)
		}
	}
	sort.Strings(scopes)
	return batch.IdentitySnapshot{Tenant: id.Tenant, Subject: id.Subject, Actor: id.Actor(), Application: id.Application, AuthType: id.AuthType, ClientIP: id.ClientIP, Scopes: scopes, ModelACLSet: id.ModelACLSet, ModelAccess: id.ModelAccess}
}

func (s *Server) uiBatchJobs(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	base := "/gateway/ui-api/batches"
	if s.batchJobs == nil || !s.batchJobs.Enabled() {
		if r.URL.Path == base && r.Method == http.MethodGet {
			writeJSON(w, http.StatusOK, map[string]any{"enabled": false, "jobs": []batch.Job{}})
			return
		}
		writeProtocolError(w, r, http.StatusNotFound, "batch_disabled", "durable batch jobs are disabled")
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, base)
	if rest == "" || rest == "/" {
		if r.Method != http.MethodGet {
			writeProtocolError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"enabled":         true,
			"jobs":            s.batchJobs.List("", "", true),
			"retention":       s.cfg.BatchJobs.Retention.Value().String(),
			"max_jobs":        s.cfg.BatchJobs.MaxJobs,
			"max_concurrent":  s.cfg.BatchJobs.MaxConcurrent,
			"max_input_bytes": s.cfg.BatchJobs.MaxInputBytes,
		})
		return
	}
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		writeProtocolError(w, r, http.StatusNotFound, "batch_not_found", "batch job not found")
		return
	}
	jobID := parts[0]
	if len(parts) == 1 && r.Method == http.MethodGet {
		j, ok := s.batchJobs.Get(jobID, "", "", true)
		if !ok {
			writeProtocolError(w, r, http.StatusNotFound, "batch_not_found", "batch job not found")
			return
		}
		writeJSON(w, http.StatusOK, j)
		return
	}
	if len(parts) == 2 && parts[1] == "output" && r.Method == http.MethodGet {
		s.batchOutput(w, r, id, jobID, true)
		return
	}
	if len(parts) == 2 && r.Method == http.MethodPost {
		var (
			j   batch.Job
			err error
		)
		switch parts[1] {
		case "pause":
			j, err = s.batchJobs.Pause(jobID, "", "", true)
		case "resume":
			j, err = s.batchJobs.Resume(jobID, "", "", true)
		case "cancel":
			j, err = s.batchJobs.Cancel(jobID, "", "", true)
		default:
			writeProtocolError(w, r, http.StatusNotFound, "not_found", "unknown batch operation")
			return
		}
		if err != nil {
			s.writeBatchError(w, r, err)
			return
		}
		s.log.Info("durable batch control", "batch_id", jobID, "action", parts[1], "admin_subject", id.Subject, "admin_auth_type", id.AuthType)
		writeJSON(w, http.StatusOK, j)
		return
	}
	writeProtocolError(w, r, http.StatusNotFound, "not_found", "unknown batch UI endpoint")
}
