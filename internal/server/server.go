package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/example/ollama-fair-gateway/internal/alerts"
	"github.com/example/ollama-fair-gateway/internal/auth"
	"github.com/example/ollama-fair-gateway/internal/autotune"
	"github.com/example/ollama-fair-gateway/internal/batch"
	"github.com/example/ollama-fair-gateway/internal/config"
	"github.com/example/ollama-fair-gateway/internal/conversation"
	"github.com/example/ollama-fair-gateway/internal/cost"
	"github.com/example/ollama-fair-gateway/internal/infrastructure"
	"github.com/example/ollama-fair-gateway/internal/liveflow"
	"github.com/example/ollama-fair-gateway/internal/metrics"
	"github.com/example/ollama-fair-gateway/internal/policy"
	"github.com/example/ollama-fair-gateway/internal/proxy"
	"github.com/example/ollama-fair-gateway/internal/quota"
	"github.com/example/ollama-fair-gateway/internal/scheduler"
	"github.com/example/ollama-fair-gateway/internal/session"
	"github.com/example/ollama-fair-gateway/internal/telemetry"
	"github.com/example/ollama-fair-gateway/internal/usage"
	"github.com/example/ollama-fair-gateway/internal/warm"
	"github.com/example/ollama-fair-gateway/internal/worker"
)

type ConfigStore interface {
	Save(*config.Config) error
	Delete() error
	Path() string
}

type WorkerStateStore interface {
	List(context.Context) (map[string]string, error)
	Put(context.Context, string, string) error
	Delete(context.Context, string) error
	Health(context.Context) error
}

type ModelPlacementStore interface {
	Get(context.Context, string) (config.ModelPlacementRule, bool, error)
	List(context.Context) (map[string]config.ModelPlacementRule, error)
	Put(context.Context, string, config.ModelPlacementRule) error
	Delete(context.Context, string) error
	Health(context.Context) error
}

func unixOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

type Server struct {
	cfg              *config.Config
	auth             *auth.Authenticator
	sched            scheduler.Scheduler
	quota            quota.Ledger
	estimator        *cost.Estimator
	workers          *worker.Pool
	proxy            *proxy.Proxy
	usage            *usage.Recorder
	metrics          *metrics.Registry
	live             *liveflow.Tracker
	infrastructure   *infrastructure.Hub
	policies         policy.Store
	sessions         session.Store
	ops              *operationManager
	jobs             *jobManager
	startedAt        time.Time
	log              *slog.Logger
	configStore      ConfigStore
	placementStore   ModelPlacementStore
	workerStateStore WorkerStateStore
	autoTune         *autotune.Manager
	otel             *telemetry.Exporter
	warm             *warm.Manager
	alerts           *alerts.Manager
	conversations    *conversation.Store
	batchJobs        *batch.Manager
	aliasMu          sync.Mutex
	aliases          atomic.Value // immutable map[string]config.ModelAliasConfig
	modelAccessMu    sync.Mutex
	modelAccess      atomic.Value // immutable config.ModelAccessConfig
}

type Dependencies struct {
	Auth             *auth.Authenticator
	Scheduler        scheduler.Scheduler
	Quota            quota.Ledger
	Estimator        *cost.Estimator
	Workers          *worker.Pool
	Proxy            *proxy.Proxy
	Usage            *usage.Recorder
	Metrics          *metrics.Registry
	Live             *liveflow.Tracker
	Infrastructure   *infrastructure.Hub
	Policies         policy.Store
	Sessions         session.Store
	Logger           *slog.Logger
	ConfigStore      ConfigStore
	PlacementStore   ModelPlacementStore
	WorkerStateStore WorkerStateStore
	AutoTune         *autotune.Manager
	OpenTelemetry    *telemetry.Exporter
	WarmModels       *warm.Manager
	Alerts           *alerts.Manager
	Conversations    *conversation.Store
	BatchJobs        *batch.Manager
}

func New(cfg *config.Config, d Dependencies) *Server {
	s := &Server{cfg: cfg, auth: d.Auth, sched: d.Scheduler, quota: d.Quota, estimator: d.Estimator, workers: d.Workers, proxy: d.Proxy, usage: d.Usage, metrics: d.Metrics, live: d.Live, infrastructure: d.Infrastructure, policies: d.Policies, sessions: d.Sessions, ops: newOperationManager(), jobs: newJobManager(), startedAt: time.Now(), log: d.Logger, configStore: d.ConfigStore, placementStore: d.PlacementStore, workerStateStore: d.WorkerStateStore, autoTune: d.AutoTune, otel: d.OpenTelemetry, warm: d.WarmModels, alerts: d.Alerts, conversations: d.Conversations, batchJobs: d.BatchJobs}
	s.aliases.Store(cloneModelAliases(cfg.ModelAliases))
	s.modelAccess.Store(cloneModelAccess(cfg.ModelAccess))
	if s.live == nil {
		s.live = liveflow.New(10*time.Second, 512)
	}
	if s.policies == nil {
		s.policies = policy.NewMemory()
	}
	if s.sessions == nil {
		s.sessions = session.NewMemory()
	}
	s.metrics.SetDynamic(func() metrics.Dynamic {
		st := s.sched.Stats(context.Background())
		ws := s.workers.Snapshots()
		classes := make(map[string]metrics.ServiceClassMetric, len(st.Classes))
		for name, cs := range st.Classes {
			classes[name] = metrics.ServiceClassMetric{Queued: cs.Queued, Running: cs.Running}
		}
		wm := make([]metrics.WorkerMetric, 0, len(ws))
		for _, w := range ws {
			perf := make([]metrics.ModelPerformanceMetric, 0, len(w.Performance))
			for _, p := range w.Performance {
				perf = append(perf, metrics.ModelPerformanceMetric{Model: p.Model, PromptTPS: p.PromptTPS, OutputTPS: p.OutputTPS, Samples: p.Samples})
			}
			wm = append(wm, metrics.WorkerMetric{Name: w.Name, Healthy: w.Healthy, Active: w.Active, Max: w.MaxConcurrent, MemoryUsedBytes: w.MemoryUsedBytes, MemoryTotalBytes: w.MemoryTotalBytes, VRAMUsedBytes: w.VRAMUsedBytes, VRAMTotalBytes: w.VRAMTotalBytes, GPUUtilizationPct: w.GPUUtilizationPct, GPUTemperatureC: w.GPUTemperatureC, GPUPowerWatts: w.GPUPowerWatts, ModelActive: w.ModelActive, Performance: perf, CircuitState: w.CircuitState, Maintenance: w.Maintenance})
		}
		ret := s.usage.RetentionStatus()
		d := metrics.Dynamic{Queued: st.Queued, Running: st.Running, OldestQueueWaitSeconds: st.OldestWait.Seconds(), Workers: wm, UsageRawFiles: ret.RawFiles, UsageDailyFiles: ret.DailyFiles, UsageMonthlyFiles: ret.MonthlyFiles, UsageRawBytes: ret.RawBytes, UsageDailyBytes: ret.DailyBytes, UsageMonthlyBytes: ret.MonthlyBytes, UsageLastCompactionUnix: unixOrZero(ret.LastCompaction), UsageLastReclaimedBytes: ret.LastReclaimedBytes, ServiceClasses: classes}
		if s.otel != nil {
			d.OTelExportedSpans = s.otel.Exported()
			d.OTelFailedSpans = s.otel.Failed()
			d.OTelDroppedSpans = s.otel.Dropped()
		}
		if s.warm != nil {
			ws := s.warm.Status()
			d.WarmEvictionSuggestions = len(ws.Suggestions)
			for _, a := range ws.Actions {
				if a.Status == "running" {
					d.WarmActionsRunning++
				}
			}
		}
		if s.alerts != nil {
			as := s.alerts.Status()
			d.AlertsActive = len(as.Active)
			d.AlertsLastEvaluateUnix = unixOrZero(as.LastEvaluate)
		}
		return d
	})
	return s
}
func (s *Server) Handler() http.Handler { return http.HandlerFunc(s.serveHTTP) }

func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if s.handlePublicDashboard(w, r) {
		return
	}
	if s.handleUIPublic(w, r) {
		return
	}
	switch r.URL.Path {
	case "/healthz":
		writeJSON(w, 200, map[string]any{"status": "ok"})
		return
	case "/readyz":
		s.ready(w, r)
		return
	case "/metrics":
		if s.cfg.Server.MetricsPublic {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
			s.metrics.WritePrometheus(w)
			return
		}
	}
	sessionCookieAuth := s.injectUISession(r)
	id, err := s.auth.Authenticate(r)
	if err != nil {
		clientIP := s.auth.ClientIP(r)
		w.Header().Set("WWW-Authenticate", `Bearer realm="ollama-gateway"`)
		w.Header().Set("X-Gateway-Client-IP", clientIP)
		s.log.Warn("authentication rejected", "client_ip", clientIP, "remote_addr", r.RemoteAddr, "method", r.Method, "path", r.URL.Path, "user_agent", r.UserAgent())
		writeProtocolError(w, r, 401, "unauthorized", "authentication required")
		return
	}
	r = r.WithContext(auth.WithIdentity(r.Context(), id))
	if s.handleModelDiscovery(w, r, id) {
		return
	}
	if r.URL.Path == "/metrics" {
		if !id.IsAdmin() {
			writeProtocolError(w, r, 403, "forbidden", "gateway:admin scope required")
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		s.metrics.WritePrometheus(w)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/gateway/ui-api/") {
		s.uiAPI(w, r, id, sessionCookieAuth)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/gateway/v1/batches") {
		s.batchAPI(w, r, id)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/gateway/v1/") {
		s.gatewayAPI(w, r, id)
		return
	}
	if !strings.HasPrefix(r.URL.Path, "/api/") && !strings.HasPrefix(r.URL.Path, "/v1/") {
		writeProtocolError(w, r, 404, "not_found", "unknown endpoint")
		return
	}
	if s.cfg.Native.ManagementRequiresAdmin && isManagement(r.Method, r.URL.Path) && !id.IsAdmin() {
		writeProtocolError(w, r, 403, "forbidden", "model management requires gateway:admin scope")
		return
	}
	s.forward(w, r, id)
}

func (s *Server) handleModelDiscovery(w http.ResponseWriter, r *http.Request, id auth.Identity) bool {
	if r.Method != http.MethodGet {
		return false
	}
	switch r.URL.Path {
	case "/api/tags":
		models, errs := s.workers.Tags(r.Context())
		if len(errs) > 0 {
			w.Header().Set("X-Gateway-Partial-Errors", strconv.Itoa(len(errs)))
			s.log.Warn("partial model discovery", "endpoint", r.URL.Path, "errors", strings.Join(errs, "; "))
			if len(models) == 0 {
				writeProtocolError(w, r, 503, "worker_unavailable", "unable to retrieve Ollama model tags")
				return true
			}
		}
		filtered := make([]worker.TagModel, 0, len(models)+len(s.aliasSnapshot()))
		for _, m := range models {
			if s.modelAllowed(id, m.Model) {
				filtered = append(filtered, m)
			}
		}
		for _, alias := range s.visibleAliases(r.Context(), id) {
			filtered = append(filtered, worker.TagModel{Name: alias, Model: alias, Digest: "virtual"})
		}
		sort.Slice(filtered, func(i, j int) bool { return filtered[i].Model < filtered[j].Model })
		writeJSON(w, http.StatusOK, map[string]any{"models": filtered})
		return true
	case "/api/ps":
		loaded := s.workers.Loaded()
		filtered := loaded[:0]
		for _, m := range loaded {
			if s.modelAllowed(id, m.Model) {
				filtered = append(filtered, m)
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"models": filtered})
		return true
	case "/v1/models":
		models, errs := s.workers.Tags(r.Context())
		if len(errs) > 0 {
			w.Header().Set("X-Gateway-Partial-Errors", strconv.Itoa(len(errs)))
			s.log.Warn("partial model discovery", "endpoint", r.URL.Path, "errors", strings.Join(errs, "; "))
			if len(models) == 0 {
				writeProtocolError(w, r, 503, "worker_unavailable", "unable to retrieve Ollama models")
				return true
			}
		}
		data := make([]map[string]any, 0, len(models)+len(s.aliasSnapshot()))
		for _, m := range models {
			if !s.modelAllowed(id, m.Model) {
				continue
			}
			created := int64(0)
			if !m.ModifiedAt.IsZero() {
				created = m.ModifiedAt.Unix()
			}
			data = append(data, map[string]any{"id": m.Model, "object": "model", "created": created, "owned_by": "ollama"})
		}
		for _, alias := range s.visibleAliases(r.Context(), id) {
			data = append(data, map[string]any{"id": alias, "object": "model", "created": int64(0), "owned_by": "ollama-gateway"})
		}
		sort.Slice(data, func(i, j int) bool { return data[i]["id"].(string) < data[j]["id"].(string) })
		writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": data})
		return true
	}
	return false
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	checks := map[string]string{}
	ok := true
	checksFn := map[string]func(context.Context) error{"workers": s.workers.Health, "scheduler": s.sched.Health, "quota": s.quota.Health, "usage": s.usage.Health, "policy": s.policies.Health, "api_keys": s.auth.RuntimeStoreHealth}
	if s.placementStore != nil {
		checksFn["model_placement"] = s.placementStore.Health
	}
	if s.workerStateStore != nil {
		checksFn["worker_state"] = s.workerStateStore.Health
	}
	for name, fn := range checksFn {
		if err := fn(ctx); err != nil {
			checks[name] = err.Error()
			ok = false
		} else {
			checks[name] = "ok"
		}
	}
	status := 200
	if !ok {
		status = 503
	}
	writeJSON(w, status, map[string]any{"status": map[bool]string{true: "ready", false: "not_ready"}[ok], "checks": checks})
}
func (s *Server) gatewayAPI(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	switch r.URL.Path {
	case "/gateway/v1/usage/me":
		writeJSON(w, 200, s.usage.Actor(r.Context(), id.Tenant, id.Actor()))
	case "/gateway/v1/status":
		if !id.IsAdmin() {
			writeProtocolError(w, r, 403, "forbidden", "gateway:admin scope required")
			return
		}
		st := s.sched.Stats(r.Context())
		writeJSON(w, 200, map[string]any{"scheduler": st, "workers": s.workers.Snapshots()})
	case "/gateway/v1/usage/tenant":
		if !id.IsAdmin() {
			writeProtocolError(w, r, 403, "forbidden", "gateway:admin scope required")
			return
		}
		t := r.URL.Query().Get("tenant")
		if t == "" {
			t = id.Tenant
		}
		writeJSON(w, 200, s.usage.Tenant(r.Context(), t))
	default:
		writeProtocolError(w, r, 404, "not_found", "unknown gateway endpoint")
	}
}

func (s *Server) forward(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	started := time.Now()
	ctx := r.Context()
	if d := s.cfg.Server.MaxRequestDuration.Value(); d > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d)
		defer cancel()
	}
	api := "ollama"
	if r.URL.Path == "/v1/messages" {
		api = "anthropic"
	} else if strings.HasPrefix(r.URL.Path, "/v1/") {
		api = "openai"
	}
	compute := s.isCompute(r.Method, r.URL.Path)
	var body []byte
	var outboundBody io.Reader
	controlModel := ""
	if compute || isModelRoutedControlRequest(r.Method, r.URL.Path) {
		var err error
		body, err = readBody(r, s.cfg.Server.MaxBodyBytes)
		if err != nil {
			writeProtocolError(w, r, 413, "request_too_large", err.Error())
			return
		}
		if body != nil {
			outboundBody = bytes.NewReader(body)
		}
		if !compute {
			controlModel = modelFromBody(body)
		}
	} else if r.Body != nil && r.Body != http.NoBody {
		// Large native management/blob endpoints remain true streaming
		// passthroughs. Only compute and small model-introspection requests are
		// buffered so they can be routed to a worker that actually owns the model.
		outboundBody = r.Body
	}
	var conversationPlan *responseConversationPlan
	if compute && r.URL.Path == "/v1/responses" && s.conversations != nil && s.conversations.Enabled() {
		var err error
		body, conversationPlan, err = s.prepareResponseConversation(body, id)
		if err != nil {
			writeProtocolError(w, r, 400, "invalid_previous_response_id", err.Error())
			return
		}
		r.ContentLength = int64(len(body))
		outboundBody = bytes.NewReader(body)
		w.Header().Set("X-Gateway-Conversations", "enabled")
	}
	requestedModel := modelFromBody(body)
	resolvedModel, aliasName, resolveErr := s.resolveModel(ctx, id, requestedModel)
	if resolveErr != nil {
		if errors.Is(resolveErr, ErrModelAccessDenied) {
			writeProtocolError(w, r, 403, "model_access_denied", resolveErr.Error())
		} else {
			writeProtocolError(w, r, 404, "model_alias_unavailable", resolveErr.Error())
		}
		return
	}
	if resolvedModel != "" && resolvedModel != requestedModel {
		var err error
		body, err = rewriteModelBody(body, resolvedModel)
		if err != nil {
			writeProtocolError(w, r, 400, "bad_model_alias", err.Error())
			return
		}
		r.ContentLength = int64(len(body))
		outboundBody = bytes.NewReader(body)
		if !compute {
			controlModel = resolvedModel
		}
		w.Header().Set("X-Gateway-Model-Alias", aliasName)
		w.Header().Set("X-Gateway-Resolved-Model", resolvedModel)
	} else if !compute {
		controlModel = resolvedModel
	}
	est := cost.Estimate{}
	preflight := modelPreflight{}
	serviceClass := ""
	serviceCfg := config.ServiceClassConfig{}
	if compute {
		est = s.estimator.Estimate(r.URL.Path, body)
		var ok bool
		preflight, ok = s.preflightModel(w, r, body, est)
		if !ok {
			return
		}
		var classErr error
		serviceClass, serviceCfg, classErr = s.serviceClassFor(r, id)
		if classErr != nil {
			writeProtocolError(w, r, 403, "service_class_denied", classErr.Error())
			return
		}
		if h := strings.TrimSpace(s.cfg.ServiceClasses.Header); h != "" {
			r.Header.Del(h)
		}
		w.Header().Set("X-Gateway-Service-Class", serviceClass)
	}
	requestID := newID()
	w.Header().Set("X-Request-ID", requestID)
	traceCtx := telemetry.Context{}
	if compute && s.otel != nil {
		traceCtx = s.otel.NewTrace(r.Header.Get("traceparent"))
		if traceCtx.Sampled {
			w.Header().Set("X-Gateway-Trace-ID", traceCtx.TraceID)
			if tp := s.otel.TraceParent(traceCtx); tp != "" {
				r.Header.Set("traceparent", tp)
			}
		}
	}
	var jobCancel context.CancelCauseFunc
	if compute {
		ctx, jobCancel = context.WithCancelCause(ctx)
		defer jobCancel(nil)
	}
	var qlease *scheduler.Lease
	var reservation quota.Reservation
	queueDur := time.Duration(0)
	workerName := ""
	var workerLease *worker.Lease
	var targetURL any
	var queueStart, queueEnd, routeEnd time.Time
	if compute {
		pol := s.policyFor(ctx, id.Tenant)
		limits := quota.Limits{ActorCreditsPerMinute: pol.ActorCreditsPerMinute, ActorBurstCredits: pol.ActorBurstCredits, TenantCreditsPerMinute: pol.TenantCreditsPerMinute, TenantBurstCredits: pol.TenantBurstCredits}
		decision, err := s.quota.Reserve(ctx, id.Tenant, id.Actor(), est.Credits, limits)
		if err != nil {
			writeProtocolError(w, r, 503, "quota_unavailable", err.Error())
			return
		}
		if !decision.Allowed {
			w.Header().Set("Retry-After", proxy.FormatRetryAfter(decision.RetryAfter))
			writeProtocolError(w, r, 429, "quota_exceeded", "compute-credit quota exceeded")
			return
		}
		reservation = decision.Reservation
		if s.alerts != nil {
			s.alerts.ObserveQuota(id.Tenant, id.Actor(), decision.RemainingActor, decision.RemainingTenant, pol.ActorBurstCredits, pol.TenantBurstCredits)
		}
		s.live.Begin(liveflow.Request{ID: requestID, Tenant: id.Tenant, Actor: id.Actor(), Application: id.Application, ServiceClass: serviceClass, API: api, Path: r.URL.Path, Model: est.Model, EstimatedCredits: est.Credits, EstimatedPromptTokens: est.InputTokens})
		s.jobs.register(jobEntry{ID: requestID, Tenant: id.Tenant, Actor: id.Actor(), Application: id.Application, ServiceClass: serviceClass, Model: est.Model, Path: r.URL.Path, API: api, CreatedAt: time.Now().UTC()}, jobCancel)
		defer s.jobs.finish(requestID)
		queueTimeout := s.cfg.Scheduler.QueueTimeout.Value()
		if d := serviceCfg.MaxQueueWait.Value(); d > 0 && (queueTimeout <= 0 || d < queueTimeout) {
			queueTimeout = d
		}
		queueStart = time.Now()
		qlease, err = s.sched.Acquire(ctx, scheduler.Request{Tenant: id.Tenant, Actor: id.Actor(), Cost: est.Credits, TenantWeight: pol.TenantWeight, ActorWeight: pol.ActorWeight, Timeout: queueTimeout, ServiceClass: serviceClass, ClassWeight: serviceCfg.Weight, ClassMaxConcurrent: serviceCfg.MaxConcurrent})
		if err != nil {
			_ = s.quota.Reconcile(context.Background(), reservation, 0)
			status := 503
			if errors.Is(err, scheduler.ErrQueueFull) || errors.Is(err, scheduler.ErrActorQueueFull) {
				status = 429
				w.Header().Set("Retry-After", "1")
				writeProtocolError(w, r, status, "queue_full", err.Error())
			} else if errors.Is(err, context.DeadlineExceeded) {
				writeProtocolError(w, r, status, "queue_timeout", "request exceeded queue timeout")
			} else if errors.Is(err, context.Canceled) {
				status = 499
				if isAdminJobCancel(ctx) {
					writeProtocolError(w, r, status, "request_cancelled", "request cancelled by administrator")
				} else {
					writeProtocolError(w, r, status, "request_cancelled", "request cancelled")
				}
				s.live.Cancel(requestID, status, 0, cost.Usage{}, 0)
				return
			} else {
				writeProtocolError(w, r, status, "scheduler_unavailable", err.Error())
			}
			s.live.Drop(requestID, status)
			return
		}
		defer qlease.Release()
		queueEnd = time.Now()
		s.live.MarkRouting(requestID, "", qlease.Wait)
		workerLease, err = s.workers.AcquireAllowed(ctx, est.Model, preflight.AllowedWorkers, preflight.RequestedContext)
		if err != nil {
			_ = s.quota.Reconcile(context.Background(), reservation, 0)
			if errors.Is(err, context.Canceled) {
				writeProtocolError(w, r, 499, "request_cancelled", "request cancelled")
				s.live.Cancel(requestID, 499, 0, cost.Usage{}, 0)
				return
			}
			if errors.Is(err, worker.ErrModelPlacementBlocked) {
				writeProtocolError(w, r, 403, "model_placement_denied", err.Error())
				s.live.Drop(requestID, 403)
				return
			}
			if errors.Is(err, worker.ErrModelNotInstalled) {
				writeProtocolError(w, r, 404, "model_not_found", err.Error())
				s.live.Drop(requestID, 404)
				return
			}
			writeProtocolError(w, r, 503, "worker_unavailable", err.Error())
			s.live.Drop(requestID, 503)
			return
		}
		defer func() {
			if workerLease != nil {
				workerLease.Release()
			}
		}()
		workerName = workerLease.Name()
		if s.warm != nil {
			s.warm.Touch(workerName, est.Model)
		}
		routeEnd = time.Now()
		s.jobs.setWorker(requestID, workerName)
		targetURL = workerLease.URL()
		queueDur = time.Since(started)
		s.live.MarkRouting(requestID, workerName, queueDur)
		w.Header().Set("X-Gateway-Queue-Ms", strconv.FormatInt(queueDur.Milliseconds(), 10))
		w.Header().Set("X-Gateway-Worker", workerName)
		w.Header().Set("X-Gateway-Estimated-Credits", strconv.FormatFloat(est.Credits, 'f', 4, 64))
	} else {
		u, name, err := s.workers.ControlForModel(controlModel)
		if err != nil {
			if errors.Is(err, worker.ErrModelPlacementBlocked) {
				writeProtocolError(w, r, 403, "model_placement_denied", err.Error())
				return
			}
			if errors.Is(err, worker.ErrModelNotInstalled) {
				writeProtocolError(w, r, 404, "model_not_found", err.Error())
				return
			}
			writeProtocolError(w, r, 503, "worker_unavailable", err.Error())
			return
		}
		targetURL = u
		workerName = name
	}
	target := targetURL.(*url.URL)
	serviceStart := time.Now()
	var progress proxy.ProgressFunc
	if compute {
		s.live.MarkRunning(requestID)
		progress = func(bytesOut int64, u cost.Usage) { s.live.Progress(requestID, bytesOut, u) }
	}
	attempts := 1
	excluded := map[string]bool{}
	reportedFailures := map[string]bool{}
	forwardBody := outboundBody
	if compute && body != nil {
		forwardBody = bytes.NewReader(body)
	}
	forwardRequest := func(target *url.URL, requestBody io.Reader) proxy.Result {
		if conversationPlan != nil && conversationPlan.Store {
			return s.proxy.ForwardCapture(ctx, w, r, target, requestBody, api, est.InputTokens, s.cfg.Conversations.MaxContentBytes, progress)
		}
		return s.proxy.Forward(ctx, w, r, target, requestBody, api, est.InputTokens, progress)
	}
	res := forwardRequest(target, forwardBody)
	for compute && s.cfg.Reliability.Enabled && res.Err != nil && !res.Started && attempts < s.cfg.Reliability.RetryAttempts && ctx.Err() == nil {
		opened := s.workers.ReportResult(workerName, true, res.Err.Error())
		s.metrics.RecordUpstreamFailure(workerName, "transport")
		if opened {
			s.metrics.RecordCircuitOpen(workerName)
		}
		reportedFailures[workerName] = true
		excluded[workerName] = true
		if workerLease != nil {
			workerLease.Release()
			workerLease = nil
		}
		if d := s.cfg.Reliability.RetryBackoff.Value(); d > 0 {
			select {
			case <-ctx.Done():
				break
			case <-time.After(d):
			}
		}
		next, err := s.workers.AcquireAllowedExcluding(ctx, est.Model, preflight.AllowedWorkers, excluded, preflight.RequestedContext)
		if err != nil {
			break
		}
		workerLease = next
		workerName = next.Name()
		if s.warm != nil {
			s.warm.Touch(workerName, est.Model)
		}
		target = next.URL()
		s.jobs.setWorker(requestID, workerName)
		s.live.MarkRouting(requestID, workerName, queueDur)
		attempts++
		s.metrics.RecordRetry(workerName)
		w.Header().Set("X-Gateway-Retry-Count", strconv.Itoa(attempts-1))
		w.Header().Set("X-Gateway-Worker", workerName)
		res = forwardRequest(target, bytes.NewReader(body))
	}
	if compute && workerName != "" {
		failedWorker := (res.Err != nil && !errors.Is(ctx.Err(), context.Canceled)) || res.Status >= 500
		errText := ""
		if res.Err != nil {
			errText = res.Err.Error()
		} else if res.Status >= 500 {
			errText = fmt.Sprintf("HTTP %d", res.Status)
		}
		if !failedWorker || !reportedFailures[workerName] {
			opened := s.workers.ReportResult(workerName, failedWorker, errText)
			if failedWorker {
				class := "http_5xx"
				if res.Err != nil {
					class = "transport"
				}
				s.metrics.RecordUpstreamFailure(workerName, class)
			}
			if opened {
				s.metrics.RecordCircuitOpen(workerName)
			}
		}
	}
	serviceDur := time.Since(serviceStart)
	cancelled := compute && errors.Is(ctx.Err(), context.Canceled)
	if compute && res.Status < 400 && res.Usage.PromptEvalNS+res.Usage.EvalNS == 0 {
		// Ollama's native API exposes exact eval durations. OpenAI-compatible
		// responses generally do not, so use worker-slot wall time as a
		// conservative compute-duration approximation when duration credits are enabled.
		res.Usage.EvalNS = serviceDur.Nanoseconds()
		res.Usage.Approximate = true
	}
	if res.Err != nil && !res.Started {
		if cancelled {
			writeProtocolError(w, r, 499, "request_cancelled", "request cancelled")
		} else {
			writeProtocolError(w, r, 502, "backend_error", proxy.BackendError(res.Err))
		}
	}
	actual := 0.0
	if compute && res.Status < 400 {
		actual = s.estimator.Actual(est.Model, res.Usage)
		if actual <= 0 {
			actual = est.Credits
		}
	}
	if compute {
		if err := s.quota.Reconcile(context.Background(), reservation, actual); err != nil {
			s.log.Warn("quota reconcile failed", "request_id", requestID, "error", err)
		}
	}
	status := res.Status
	if cancelled {
		status = 499
	} else if status == 0 {
		status = 502
	}
	if compute {
		if workerName != "" && status < 500 {
			s.workers.Observe(workerName, est.Model, res.Usage.PromptTokens, res.Usage.CompletionTokens, res.Usage.PromptEvalNS, res.Usage.EvalNS, serviceDur)
		}
		if cancelled {
			s.live.Cancel(requestID, status, actual, res.Usage, serviceDur)
		} else {
			s.live.Finish(requestID, status, actual, res.Usage, serviceDur)
		}
	}
	if compute && r.URL.Path == "/v1/responses" && status < 400 {
		s.persistResponseConversation(conversationPlan, res.Captured, res.CaptureTruncated, id, est.Model)
	}
	s.metrics.Record(api, status, queueDur, serviceDur, res.Usage.PromptTokens, res.Usage.CompletionTokens, actual, res.BytesIn, res.BytesOut)
	if compute && s.otel != nil && traceCtx.Sampled {
		intervals := []telemetry.Interval{}
		if !queueStart.IsZero() {
			intervals = append(intervals, telemetry.Interval{Name: "gateway.admission", Start: started, End: queueStart})
		}
		if !queueStart.IsZero() && !queueEnd.IsZero() {
			intervals = append(intervals, telemetry.Interval{Name: "gateway.queue", Start: queueStart, End: queueEnd, Attrs: map[string]any{"ollama.gateway.service_class": serviceClass}})
		}
		if !queueEnd.IsZero() && !routeEnd.IsZero() {
			intervals = append(intervals, telemetry.Interval{Name: "gateway.route", Start: queueEnd, End: routeEnd, Attrs: map[string]any{"server.address": workerName}})
		}
		intervals = append(intervals, telemetry.Interval{Name: "ollama.upstream", Start: serviceStart, End: serviceStart.Add(serviceDur), Attrs: map[string]any{"server.address": workerName}})
		errText := ""
		if res.Err != nil {
			errText = res.Err.Error()
		}
		s.otel.Record(telemetry.Record{Trace: traceCtx, RequestID: requestID, API: api, Path: r.URL.Path, Tenant: id.Tenant, Actor: id.Actor(), Application: id.Application, ServiceClass: serviceClass, Model: est.Model, Alias: aliasName, Worker: workerName, Started: started, Finished: time.Now().UTC(), FirstByte: res.FirstByte, Status: status, PromptTokens: res.Usage.PromptTokens, OutputTokens: res.Usage.CompletionTokens, CachedTokens: res.Usage.CachedPromptTokens, Credits: actual, Error: errText, Intervals: intervals})
	}
	s.usage.Record(usage.Event{ID: requestID, Time: time.Now().UTC(), Tenant: id.Tenant, Subject: id.Subject, Actor: id.Actor(), Application: id.Application, ServiceClass: serviceClass, AuthType: id.AuthType, ClientIP: id.ClientIP, API: api, Path: r.URL.Path, Model: est.Model, Worker: workerName, Status: status, QueueMS: queueDur.Milliseconds(), ServiceMS: serviceDur.Milliseconds(), EstimatedCredits: est.Credits, ActualCredits: actual, Usage: res.Usage, BytesIn: res.BytesIn, BytesOut: res.BytesOut})
	s.log.Info("request", "request_id", requestID, "tenant", id.Tenant, "subject", id.Subject, "api", api, "path", r.URL.Path, "model", est.Model, "worker", workerName, "service_class", serviceClass, "status", status, "queue_ms", queueDur.Milliseconds(), "service_ms", serviceDur.Milliseconds(), "credits", actual)
}

func (s *Server) serviceClassFor(r *http.Request, id auth.Identity) (string, config.ServiceClassConfig, error) {
	name := strings.TrimSpace(id.ServiceClass)
	if name == "" {
		name = strings.TrimSpace(s.cfg.ServiceClasses.Default)
	}
	if name == "" {
		name = "interactive"
	}
	if len(s.cfg.ServiceClasses.Classes) == 0 {
		return name, config.ServiceClassConfig{Weight: 1, MaxQueueWait: s.cfg.Scheduler.QueueTimeout}, nil
	}
	header := strings.TrimSpace(s.cfg.ServiceClasses.Header)
	if header != "" {
		if requested := strings.TrimSpace(r.Header.Get(header)); requested != "" && requested != name {
			if !id.HasScope(s.cfg.ServiceClasses.OverrideScope) {
				return "", config.ServiceClassConfig{}, fmt.Errorf("service class override requires scope %s", s.cfg.ServiceClasses.OverrideScope)
			}
			name = requested
		}
	}
	cfg, ok := s.cfg.ServiceClasses.Classes[name]
	if !ok {
		return "", config.ServiceClassConfig{}, fmt.Errorf("unknown service class %q", name)
	}
	return name, cfg, nil
}

func readBody(r *http.Request, maxBytes int64) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}
	defer r.Body.Close()
	b, err := io.ReadAll(io.LimitReader(r.Body, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > maxBytes {
		return nil, fmt.Errorf("request body exceeds %d bytes", maxBytes)
	}
	return b, nil
}
func (s *Server) isCompute(method, path string) bool {
	if method != http.MethodPost {
		return false
	}
	for _, p := range s.cfg.Scheduler.ComputePaths {
		if path == p {
			return true
		}
	}
	return false
}
func isModelRoutedControlRequest(method, path string) bool {
	if method != http.MethodPost && method != http.MethodDelete {
		return false
	}
	switch path {
	case "/api/show", "/api/delete":
		return true
	default:
		return false
	}
}

func modelFromBody(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	var v struct {
		Model string `json:"model"`
		Name  string `json:"name"`
	}
	if json.Unmarshal(body, &v) != nil {
		return ""
	}
	if strings.TrimSpace(v.Model) != "" {
		return strings.TrimSpace(v.Model)
	}
	return strings.TrimSpace(v.Name)
}

func isManagement(method, path string) bool {
	if !strings.HasPrefix(path, "/api/") {
		return false
	}
	for _, p := range []string{"/api/pull", "/api/push", "/api/create", "/api/copy", "/api/delete", "/api/stop", "/api/blobs"} {
		if path == p || strings.HasPrefix(path, p+"/") {
			return true
		}
	}
	return false
}
func writeProtocolError(w http.ResponseWriter, r *http.Request, status int, code, msg string) {
	if r != nil && r.URL.Path == "/v1/messages" {
		typ := "api_error"
		switch status {
		case 400:
			typ = "invalid_request_error"
		case 401:
			typ = "authentication_error"
		case 403:
			typ = "permission_error"
		case 404:
			typ = "not_found_error"
		case 429:
			typ = "rate_limit_error"
		case 500, 502, 503, 504:
			typ = "api_error"
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{"type": "error", "error": map[string]any{"type": typ, "message": msg}})
		return
	}
	// Ollama's native API uses a string-valued "error" field. OpenWebUI
	// relies on that shape when verifying Ollama connections; returning the
	// OpenAI-style nested object causes UI messages such as "[object Object]".
	if r != nil && strings.HasPrefix(r.URL.Path, "/api/") {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": msg})
		return
	}
	proxy.WriteJSONError(w, status, code, msg)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func newID() string { b := make([]byte, 16); _, _ = rand.Read(b); return hex.EncodeToString(b) }
