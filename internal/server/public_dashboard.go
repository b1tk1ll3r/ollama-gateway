package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/example/ollama-fair-gateway/internal/liveflow"
	"github.com/example/ollama-fair-gateway/internal/publicui"
	"github.com/example/ollama-fair-gateway/internal/worker"
)

type publicDashboardCounts struct {
	Workers        int   `json:"workers"`
	HealthyWorkers int   `json:"healthy_workers"`
	Models         int   `json:"models"`
	Active         int   `json:"active"`
	Queued         int64 `json:"queued"`
	Routing        int   `json:"routing"`
	Running        int64 `json:"running"`
	Streaming      int   `json:"streaming"`
}

type publicResourceMetrics struct {
	MemoryUsedBytes   int64   `json:"memory_used_bytes,omitempty"`
	MemoryTotalBytes  int64   `json:"memory_total_bytes,omitempty"`
	VRAMUsedBytes     int64   `json:"vram_used_bytes,omitempty"`
	VRAMTotalBytes    int64   `json:"vram_total_bytes,omitempty"`
	GPUUtilizationPct float64 `json:"gpu_utilization_percent,omitempty"`
	GPUTemperatureC   float64 `json:"gpu_temperature_c,omitempty"`
	GPUPowerWatts     float64 `json:"gpu_power_watts,omitempty"`
}

type publicDashboardWorker struct {
	Name            string                 `json:"name"`
	Healthy         bool                   `json:"healthy"`
	Active          int64                  `json:"active"`
	MaxConcurrent   int                    `json:"max_concurrent"`
	AcceptingNew    bool                   `json:"accepting_new"`
	Maintenance     string                 `json:"maintenance,omitempty"`
	CircuitState    string                 `json:"circuit_state,omitempty"`
	LoadedModels    []string               `json:"loaded_models,omitempty"`
	ResourceMetrics *publicResourceMetrics `json:"resource_metrics,omitempty"`
}

type publicDashboardRequest struct {
	ID               string `json:"id"`
	State            string `json:"state"`
	Model            string `json:"model,omitempty"`
	Worker           string `json:"worker,omitempty"`
	QueueMS          int64  `json:"queue_ms,omitempty"`
	ServiceMS        int64  `json:"service_ms,omitempty"`
	PromptTokens     int64  `json:"prompt_tokens,omitempty"`
	CompletionTokens int64  `json:"completion_tokens,omitempty"`
}

type publicDashboardSnapshot struct {
	SchemaVersion     int                      `json:"schema_version"`
	GeneratedAt       time.Time                `json:"generated_at"`
	Title             string                   `json:"title"`
	Subtitle          string                   `json:"subtitle"`
	RefreshIntervalMS int64                    `json:"refresh_interval_ms"`
	UptimeSeconds     float64                  `json:"uptime_seconds"`
	Counts            publicDashboardCounts    `json:"counts"`
	Workers           []publicDashboardWorker  `json:"workers"`
	Requests          []publicDashboardRequest `json:"requests"`
}

// handlePublicDashboard serves a separate unauthenticated read-only surface.
// It is intentionally evaluated before authentication. The snapshot builder
// only copies an explicit allow-list of fields; never return admin snapshots
// directly from this handler.
func (s *Server) handlePublicDashboard(w http.ResponseWriter, r *http.Request) bool {
	base := s.cfg.PublicDashboard.Path
	if base == "" {
		base = "/status"
	}
	if r.URL.Path != base && !strings.HasPrefix(r.URL.Path, base+"/") {
		return false
	}
	if !s.cfg.PublicDashboard.Enabled {
		http.NotFound(w, r)
		return true
	}
	if r.URL.Path == base {
		http.Redirect(w, r, base+"/", http.StatusTemporaryRedirect)
		return true
	}

	setPublicDashboardHeaders(w)
	rel := strings.TrimPrefix(r.URL.Path, base+"/")
	if rel == "api/snapshot" {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return true
		}
		w.Header().Set("Cache-Control", "public, max-age=1, stale-while-revalidate=2")
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			return true
		}
		writeJSON(w, http.StatusOK, s.publicDashboardSnapshot())
		return true
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return true
	}
	w.Header().Set("Cache-Control", "public, max-age=300")
	h := publicui.Handler()
	r2 := r.Clone(r.Context())
	r2.URL.Path = "/" + rel
	h.ServeHTTP(w, r2)
	return true
}

func setPublicDashboardHeaders(w http.ResponseWriter) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("Permissions-Policy", "camera=(), microphone=(), geolocation=(), payment=(), usb=()")
	w.Header().Set("Content-Security-Policy", "default-src 'self'; connect-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; script-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
}

func (s *Server) publicDashboardSnapshot() publicDashboardSnapshot {
	cfg := s.cfg.PublicDashboard
	st := s.sched.Stats(context.Background())
	ws := s.workers.Snapshots()
	live := s.live.Snapshot()
	sort.Slice(ws, func(i, j int) bool { return ws[i].Name < ws[j].Name })

	workerNames := make(map[string]string, len(ws))
	for i, w := range ws {
		workerNames[w.Name] = publicWorkerDisplayName(cfg.ShowWorkerNames, cfg.WorkerDisplayNames, w.Name, i)
	}
	modelNames := publicModelDisplayNames(cfg.ShowModelNames, ws, live.Requests)

	out := publicDashboardSnapshot{
		SchemaVersion:     1,
		GeneratedAt:       time.Now().UTC(),
		Title:             cfg.Title,
		Subtitle:          cfg.Subtitle,
		RefreshIntervalMS: cfg.RefreshInterval.Value().Milliseconds(),
		UptimeSeconds:     time.Since(s.startedAt).Seconds(),
		Counts: publicDashboardCounts{
			Workers:   len(ws),
			Active:    live.Counts.Active,
			Queued:    st.Queued,
			Routing:   live.Counts.Routing,
			Running:   st.Running,
			Streaming: live.Counts.Streaming,
		},
	}

	modelSet := make(map[string]struct{})
	for _, w := range ws {
		pw := publicDashboardWorker{
			Name:          workerNames[w.Name],
			Healthy:       w.Healthy,
			Active:        w.Active,
			MaxConcurrent: w.MaxConcurrent,
			AcceptingNew:  w.AcceptingNew,
			Maintenance:   publicMaintenance(w.Maintenance),
			CircuitState:  publicCircuitState(w.CircuitState),
		}
		if w.Healthy {
			out.Counts.HealthyWorkers++
		}
		for _, m := range w.LoadedModels {
			if m.Name == "" {
				continue
			}
			modelSet[m.Name] = struct{}{}
			pw.LoadedModels = append(pw.LoadedModels, modelNames[m.Name])
		}
		sort.Strings(pw.LoadedModels)
		if cfg.ShowResourceMetrics {
			memTotal := firstPositive(w.MemoryTotalBytes, w.MemoryCapacityBytes)
			vramTotal := firstPositive(w.VRAMTotalBytes, w.VRAMCapacityBytes)
			pw.ResourceMetrics = &publicResourceMetrics{
				MemoryUsedBytes:   w.MemoryUsedBytes,
				MemoryTotalBytes:  memTotal,
				VRAMUsedBytes:     w.VRAMUsedBytes,
				VRAMTotalBytes:    vramTotal,
				GPUUtilizationPct: w.GPUUtilizationPct,
				GPUTemperatureC:   w.GPUTemperatureC,
				GPUPowerWatts:     w.GPUPowerWatts,
			}
		}
		out.Workers = append(out.Workers, pw)
	}
	out.Counts.Models = len(modelSet)

	requests := publicVisibleRequests(live.Requests, cfg.MaxLiveRequests)
	for _, r := range requests {
		pr := publicDashboardRequest{
			ID:               publicRequestID(r.ID),
			State:            publicRequestState(r.State),
			QueueMS:          maxInt64(0, r.QueueMS),
			ServiceMS:        maxInt64(0, r.ServiceMS),
			PromptTokens:     maxInt64(0, r.PromptTokens),
			CompletionTokens: maxInt64(0, r.CompletionTokens),
		}
		if r.Model != "" {
			pr.Model = modelNames[r.Model]
		}
		if r.Worker != "" {
			pr.Worker = workerNames[r.Worker]
			if pr.Worker == "" {
				pr.Worker = "Worker"
			}
		}
		out.Requests = append(out.Requests, pr)
	}
	return out
}

func publicWorkerDisplayName(show bool, aliases map[string]string, actual string, index int) string {
	if alias := strings.TrimSpace(aliases[actual]); alias != "" {
		return alias
	}
	if show && strings.TrimSpace(actual) != "" {
		return actual
	}
	return "Worker " + twoDigits(index+1)
}

func publicModelDisplayNames(show bool, ws []worker.Snapshot, requests []liveflow.Request) map[string]string {
	set := map[string]struct{}{}
	for _, w := range ws {
		for _, m := range w.LoadedModels {
			if m.Name != "" {
				set[m.Name] = struct{}{}
			}
		}
	}
	for _, r := range requests {
		if r.Model != "" {
			set[r.Model] = struct{}{}
		}
	}
	names := make([]string, 0, len(set))
	for name := range set {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make(map[string]string, len(names))
	for i, name := range names {
		if show {
			out[name] = name
		} else {
			out[name] = "Model " + twoDigits(i+1)
		}
	}
	return out
}

func publicVisibleRequests(in []liveflow.Request, limit int) []liveflow.Request {
	if limit <= 0 || len(in) <= limit {
		return append([]liveflow.Request(nil), in...)
	}
	active := make([]liveflow.Request, 0, limit)
	recent := make([]liveflow.Request, 0, limit)
	for _, r := range in {
		switch r.State {
		case liveflow.StateCompleted, liveflow.StateCancelled, liveflow.StateFailed:
			recent = append(recent, r)
		default:
			active = append(active, r)
		}
	}
	if len(active) >= limit {
		return active[:limit]
	}
	need := limit - len(active)
	if need > len(recent) {
		need = len(recent)
	}
	return append(active, recent[len(recent)-need:]...)
}

func publicRequestID(id string) string {
	sum := sha256.Sum256([]byte(id))
	return "REQ-" + strings.ToUpper(hex.EncodeToString(sum[:5]))
}

func publicRequestState(state string) string {
	switch state {
	case liveflow.StateQueued, liveflow.StateRouting, liveflow.StateRunning, liveflow.StateStreaming, liveflow.StateCompleted, liveflow.StateCancelled, liveflow.StateFailed:
		return state
	default:
		return "running"
	}
}

func publicMaintenance(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "drain", "draining":
		return "draining"
	case "disabled", "offline":
		return "disabled"
	default:
		return "active"
	}
}

func publicCircuitState(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "open", "half-open", "half_open":
		return strings.ReplaceAll(v, "_", "-")
	default:
		return "closed"
	}
}

func firstPositive(a, b int64) int64 {
	if a > 0 {
		return a
	}
	if b > 0 {
		return b
	}
	return 0
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func twoDigits(n int) string {
	if n < 10 {
		return "0" + string(rune('0'+n))
	}
	return strconv.Itoa(n)
}
