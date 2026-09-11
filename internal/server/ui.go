package server

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/example/ollama-fair-gateway/internal/auth"
	"github.com/example/ollama-fair-gateway/internal/autotune"
	"github.com/example/ollama-fair-gateway/internal/config"
	"github.com/example/ollama-fair-gateway/internal/proxy"
	"github.com/example/ollama-fair-gateway/internal/webui"
)

const (
	uiSessionCookie = "ofg_session"
	uiStateCookie   = "ofg_oidc_state"
	uiCSRFCookie    = "ofg_csrf"
)

type oidcLoginState struct {
	State    string `json:"state"`
	Verifier string `json:"verifier"`
	Redirect string `json:"redirect"`
	Expires  int64  `json:"expires"`
}

func (s *Server) handleUIPublic(w http.ResponseWriter, r *http.Request) bool {
	if !s.cfg.UI.Enabled {
		return false
	}
	if r.URL.Path == "/gateway/ui-api/bootstrap" {
		writeJSON(w, 200, map[string]any{
			"title":          s.cfg.UI.Title,
			"ui_path":        s.cfg.UI.Path,
			"oidc_enabled":   s.cfg.UI.OIDC.Enabled,
			"oidc_login_url": s.cfg.UI.Path + "/login/oidc",
		})
		return true
	}
	base := s.cfg.UI.Path
	if r.URL.Path == base {
		http.Redirect(w, r, base+"/", http.StatusTemporaryRedirect)
		return true
	}
	if !strings.HasPrefix(r.URL.Path, base+"/") {
		return false
	}
	rel := strings.TrimPrefix(r.URL.Path, base+"/")
	switch rel {
	case "login/oidc":
		s.uiOIDCLogin(w, r)
		return true
	case "callback":
		s.uiOIDCCallback(w, r)
		return true
	case "logout":
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return true
		}
		s.clearUICookies(w, r)
		w.WriteHeader(http.StatusNoContent)
		return true
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return true
	}
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("Content-Security-Policy", "default-src 'self'; connect-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; script-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
	w.Header().Set("Cache-Control", "no-store")
	h := webui.Handler()
	r2 := r.Clone(r.Context())
	r2.URL.Path = "/" + rel
	h.ServeHTTP(w, r2)
	return true
}

func (s *Server) injectUISession(r *http.Request) bool {
	if !s.cfg.UI.Enabled || r.Header.Get("Authorization") != "" || r.Header.Get("X-API-Key") != "" {
		return false
	}
	c, err := r.Cookie(uiSessionCookie)
	if err != nil || c.Value == "" {
		return false
	}
	token, err := s.sessions.Get(r.Context(), c.Value)
	if err != nil || token == "" {
		return false
	}
	r.Header.Set("Authorization", "Bearer "+token)
	return true
}

func (s *Server) uiOIDCLogin(w http.ResponseWriter, r *http.Request) {
	if !s.cfg.UI.OIDC.Enabled || !s.auth.OIDCEnabled() {
		proxy.WriteJSONError(w, 404, "oidc_disabled", "browser OIDC login is disabled")
		return
	}
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	ep, ok := s.auth.OIDCBrowserEndpoints()
	if !ok || ep.Authorization == "" {
		proxy.WriteJSONError(w, 503, "oidc_unavailable", "OIDC authorization endpoint unavailable")
		return
	}
	state := randomURLToken(24)
	verifier := randomURLToken(48)
	redirect := s.uiRedirectURL(r)
	payload := oidcLoginState{State: state, Verifier: verifier, Redirect: redirect, Expires: time.Now().Add(10 * time.Minute).Unix()}
	signed, err := s.signUIState(payload)
	if err != nil {
		proxy.WriteJSONError(w, 500, "oidc_state", err.Error())
		return
	}
	http.SetCookie(w, &http.Cookie{Name: uiStateCookie, Value: signed, Path: s.cfg.UI.Path + "/", MaxAge: 600, HttpOnly: true, Secure: s.uiSecureCookie(r), SameSite: http.SameSiteLaxMode})
	h := sha256.Sum256([]byte(verifier))
	q := url.Values{}
	q.Set("response_type", "code")
	q.Set("client_id", s.cfg.UI.OIDC.ClientID)
	q.Set("redirect_uri", redirect)
	q.Set("scope", strings.Join(s.cfg.UI.OIDC.Scopes, " "))
	q.Set("state", state)
	q.Set("code_challenge", base64.RawURLEncoding.EncodeToString(h[:]))
	q.Set("code_challenge_method", "S256")
	target := ep.Authorization
	sep := "?"
	if strings.Contains(target, "?") {
		sep = "&"
	}
	http.Redirect(w, r, target+sep+q.Encode(), http.StatusFound)
}

func (s *Server) uiOIDCCallback(w http.ResponseWriter, r *http.Request) {
	if !s.cfg.UI.OIDC.Enabled || r.Method != http.MethodGet {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if e := r.URL.Query().Get("error"); e != "" {
		http.Redirect(w, r, s.cfg.UI.Path+"/?login_error="+url.QueryEscape(e), http.StatusFound)
		return
	}
	cookie, err := r.Cookie(uiStateCookie)
	if err != nil {
		proxy.WriteJSONError(w, 400, "oidc_state", "missing OIDC state cookie")
		return
	}
	st, err := s.verifyUIState(cookie.Value)
	if err != nil || st.Expires < time.Now().Unix() || subtle.ConstantTimeCompare([]byte(st.State), []byte(r.URL.Query().Get("state"))) != 1 {
		proxy.WriteJSONError(w, 400, "oidc_state", "invalid or expired OIDC state")
		return
	}
	code := r.URL.Query().Get("code")
	if code == "" {
		proxy.WriteJSONError(w, 400, "oidc_code", "missing authorization code")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	tok, err := s.auth.ExchangeOIDCCode(ctx, code, st.Redirect, s.cfg.UI.OIDC.ClientID, s.cfg.UI.OIDC.ClientSecret, st.Verifier)
	if err != nil {
		proxy.WriteJSONError(w, 502, "oidc_exchange", err.Error())
		return
	}
	if _, err := s.auth.VerifyOIDCToken(ctx, tok.AccessToken); err != nil {
		proxy.WriteJSONError(w, 401, "oidc_token", "OIDC access token is not valid for this gateway")
		return
	}
	maxAge := int(tok.ExpiresIn)
	if maxAge <= 0 || maxAge > 86400 {
		maxAge = 3600
	}
	sessionID, err := s.sessions.Create(ctx, tok.AccessToken, time.Duration(maxAge)*time.Second)
	if err != nil {
		proxy.WriteJSONError(w, 503, "session_store", "could not create UI session")
		return
	}
	http.SetCookie(w, &http.Cookie{Name: uiSessionCookie, Value: sessionID, Path: "/", MaxAge: maxAge, HttpOnly: true, Secure: s.uiSecureCookie(r), SameSite: http.SameSiteLaxMode})
	csrf := randomURLToken(24)
	http.SetCookie(w, &http.Cookie{Name: uiCSRFCookie, Value: csrf, Path: "/", MaxAge: maxAge, HttpOnly: false, Secure: s.uiSecureCookie(r), SameSite: http.SameSiteLaxMode})
	http.SetCookie(w, &http.Cookie{Name: uiStateCookie, Value: "", Path: s.cfg.UI.Path + "/", MaxAge: -1, HttpOnly: true, Secure: s.uiSecureCookie(r), SameSite: http.SameSiteLaxMode})
	http.Redirect(w, r, s.cfg.UI.Path+"/", http.StatusFound)
}

func (s *Server) uiRedirectURL(r *http.Request) string {
	if s.cfg.UI.OIDC.RedirectURL != "" {
		return s.cfg.UI.OIDC.RedirectURL
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + r.Host + s.cfg.UI.Path + "/callback"
}
func (s *Server) uiSecureCookie(r *http.Request) bool { return s.cfg.UI.SecureCookies || r.TLS != nil }
func (s *Server) clearUICookies(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(uiSessionCookie); err == nil && c.Value != "" {
		_ = s.sessions.Delete(r.Context(), c.Value)
	}
	for _, c := range []http.Cookie{
		{Name: uiSessionCookie, Path: "/", MaxAge: -1, HttpOnly: true},
		{Name: uiCSRFCookie, Path: "/", MaxAge: -1},
		{Name: uiStateCookie, Path: s.cfg.UI.Path + "/", MaxAge: -1, HttpOnly: true},
	} {
		c.Secure = s.uiSecureCookie(r)
		c.SameSite = http.SameSiteLaxMode
		http.SetCookie(w, &c)
	}
}
func (s *Server) signUIState(st oidcLoginState) (string, error) {
	b, err := json.Marshal(st)
	if err != nil {
		return "", err
	}
	p := base64.RawURLEncoding.EncodeToString(b)
	m := hmac.New(sha256.New, []byte(s.cfg.UI.SessionSecret))
	_, _ = m.Write([]byte(p))
	return p + "." + base64.RawURLEncoding.EncodeToString(m.Sum(nil)), nil
}
func (s *Server) verifyUIState(v string) (oidcLoginState, error) {
	parts := strings.Split(v, ".")
	if len(parts) != 2 {
		return oidcLoginState{}, errors.New("bad state")
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return oidcLoginState{}, err
	}
	m := hmac.New(sha256.New, []byte(s.cfg.UI.SessionSecret))
	_, _ = m.Write([]byte(parts[0]))
	if !hmac.Equal(sig, m.Sum(nil)) {
		return oidcLoginState{}, errors.New("bad state signature")
	}
	b, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return oidcLoginState{}, err
	}
	var out oidcLoginState
	err = json.Unmarshal(b, &out)
	return out, err
}
func randomURLToken(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

func (s *Server) checkUICSRF(r *http.Request, sessionCookieAuth bool) bool {
	if !sessionCookieAuth || r.Method == http.MethodGet || r.Method == http.MethodHead || r.Method == http.MethodOptions {
		return true
	}
	c, err := r.Cookie(uiCSRFCookie)
	if err != nil || c.Value == "" || subtle.ConstantTimeCompare([]byte(c.Value), []byte(r.Header.Get("X-CSRF-Token"))) != 1 {
		return false
	}
	if origin := r.Header.Get("Origin"); origin != "" {
		u, err := url.Parse(origin)
		if err != nil || !strings.EqualFold(u.Host, r.Host) {
			return false
		}
	}
	return true
}

func (s *Server) uiAPI(w http.ResponseWriter, r *http.Request, id auth.Identity, sessionCookieAuth bool) {
	if !s.cfg.UI.Enabled {
		proxy.WriteJSONError(w, 404, "not_found", "web UI disabled")
		return
	}
	if !s.checkUICSRF(r, sessionCookieAuth) {
		proxy.WriteJSONError(w, 403, "csrf", "invalid CSRF token")
		return
	}
	if r.URL.Path == "/gateway/ui-api/session" {
		scopes := make([]string, 0, len(id.Scopes))
		for x := range id.Scopes {
			scopes = append(scopes, x)
		}
		sort.Strings(scopes)
		writeJSON(w, 200, map[string]any{"tenant": id.Tenant, "subject": id.Subject, "application": id.Application, "auth_type": id.AuthType, "actor": id.Actor(), "admin": id.IsAdmin(), "scopes": scopes, "client_ip": id.ClientIP})
		return
	}
	if !id.IsAdmin() {
		proxy.WriteJSONError(w, 403, "forbidden", "gateway:admin scope required")
		return
	}
	switch {
	case r.URL.Path == "/gateway/ui-api/live" && r.Method == http.MethodGet:
		writeJSON(w, 200, s.live.Snapshot())
	case r.URL.Path == "/gateway/ui-api/live/stream" && r.Method == http.MethodGet:
		s.uiLiveStream(w, r)
	case r.URL.Path == "/gateway/ui-api/infrastructure" && r.Method == http.MethodGet:
		if s.infrastructure == nil {
			proxy.WriteJSONError(w, 503, "infrastructure_unavailable", "infrastructure live view is unavailable")
			return
		}
		writeJSON(w, 200, s.infrastructure.Snapshot())
	case r.URL.Path == "/gateway/ui-api/infrastructure/stream" && r.Method == http.MethodGet:
		s.uiInfrastructureStream(w, r)
	case r.URL.Path == "/gateway/ui-api/policy-simulator" && r.Method == http.MethodPost:
		var in policySimulationRequest
		if err := decodeJSON(r, &in, 1<<20); err != nil {
			proxy.WriteJSONError(w, 400, "bad_request", err.Error())
			return
		}
		result, err := s.simulatePolicy(r.Context(), in)
		if err != nil {
			proxy.WriteJSONError(w, 400, "simulation_failed", err.Error())
			return
		}
		writeJSON(w, 200, result)
	case r.URL.Path == "/gateway/ui-api/overview" && r.Method == http.MethodGet:
		st := s.sched.Stats(r.Context())
		writeJSON(w, 200, map[string]any{
			"scheduler":        st,
			"scheduler_config": map[string]any{"global_concurrency": s.cfg.Scheduler.GlobalConcurrency, "max_queue": s.cfg.Scheduler.MaxQueue, "max_queue_per_actor": s.cfg.Scheduler.MaxQueuePerActor, "queue_timeout": s.cfg.Scheduler.QueueTimeout.Value().String(), "mode": "in-memory", "routing": s.cfg.Routing, "model_capabilities": s.cfg.ModelCapabilities},
			"workers":          s.workers.Snapshots(), "usage": s.usage.Global(), "tenants": s.usage.LocalTenants(), "uptime_seconds": time.Since(s.startedAt).Seconds(),
			"storage":       map[string]any{"mode": "local-persistent", "data_dir": s.cfg.Storage.DataDir, "usage_journal": s.cfg.Usage.JournalDir},
			"model_aliases": s.aliasSnapshot(), "model_access": s.modelAccessSnapshot(), "reliability": s.cfg.Reliability,
			"service_classes": s.cfg.ServiceClasses, "auto_tuning": s.cfg.AutoTuning,
			"warm_models": func() any {
				if s.warm != nil {
					return s.warm.Status()
				}
				return map[string]any{"enabled": false}
			}(),
			"alerts": func() any {
				if s.alerts != nil {
					return s.alerts.Status()
				}
				return map[string]any{"enabled": false}
			}(),
			"opentelemetry": map[string]any{"enabled": s.cfg.OpenTelemetry.Enabled, "endpoint": s.cfg.OpenTelemetry.Endpoint, "service_name": s.cfg.OpenTelemetry.ServiceName, "sample_ratio": s.cfg.OpenTelemetry.SampleRatio, "exported_spans": func() uint64 {
				if s.otel != nil {
					return s.otel.Exported()
				}
				return 0
			}(), "failed_spans": func() uint64 {
				if s.otel != nil {
					return s.otel.Failed()
				}
				return 0
			}(), "dropped_spans": func() uint64 {
				if s.otel != nil {
					return s.otel.Dropped()
				}
				return 0
			}()},
		})
	case r.URL.Path == "/gateway/ui-api/recent" && r.Method == http.MethodGet:
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if limit <= 0 || limit > s.cfg.UI.RecentEvents {
			limit = min(1000, s.cfg.UI.RecentEvents)
		}
		writeJSON(w, 200, map[string]any{"events": s.usage.Recent(limit), "scope": "persistent-journal"})
	case r.URL.Path == "/gateway/ui-api/usage/rollups" && r.Method == http.MethodGet:
		granularity := r.URL.Query().Get("granularity")
		if granularity == "" {
			granularity = "daily"
		}
		if granularity != "daily" && granularity != "monthly" {
			proxy.WriteJSONError(w, 400, "bad_granularity", "granularity must be daily or monthly")
			return
		}
		dimension := r.URL.Query().Get("dimension")
		if dimension == "" {
			dimension = "global"
		}
		switch dimension {
		case "global", "tenant", "actor", "application", "model", "worker":
		default:
			proxy.WriteJSONError(w, 400, "bad_dimension", "dimension must be global, tenant, actor, application, model, or worker")
			return
		}
		name := r.URL.Query().Get("name")
		if dimension != "global" && name == "" {
			proxy.WriteJSONError(w, 400, "missing_name", "name is required for the selected dimension")
			return
		}
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		writeJSON(w, 200, map[string]any{"granularity": granularity, "dimension": dimension, "name": name, "points": s.usage.Series(granularity, dimension, name, limit), "retention": s.usage.RetentionStatus()})
	case r.URL.Path == "/gateway/ui-api/autotune" && r.Method == http.MethodGet:
		s.uiAutoTuneGet(w, r)
	case r.URL.Path == "/gateway/ui-api/autotune/start" && r.Method == http.MethodPost:
		s.uiAutoTuneStart(w, r, id)
	case strings.HasPrefix(r.URL.Path, "/gateway/ui-api/autotune/"):
		s.uiAutoTuneAction(w, r, id)
	case r.URL.Path == "/gateway/ui-api/warm-models" && r.Method == http.MethodGet:
		s.uiWarmModelsGet(w, r)
	case r.URL.Path == "/gateway/ui-api/warm-models" && r.Method == http.MethodPut:
		s.uiWarmModelsSave(w, r, id)
	case r.URL.Path == "/gateway/ui-api/warm-models" && r.Method == http.MethodDelete:
		s.uiWarmModelsReset(w, r, id)
	case r.URL.Path == "/gateway/ui-api/warm-models/reconcile" && r.Method == http.MethodPost:
		s.uiWarmModelsReconcile(w, r, id)
	case r.URL.Path == "/gateway/ui-api/alerts" && r.Method == http.MethodGet:
		s.uiAlertsGet(w, r)
	case r.URL.Path == "/gateway/ui-api/alerts/test" && r.Method == http.MethodPost:
		s.uiAlertsTest(w, r, id)
	case strings.HasPrefix(r.URL.Path, "/gateway/ui-api/workers/"):
		s.uiWorkerRuntime(w, r, id)
	case r.URL.Path == "/gateway/ui-api/models" && r.Method == http.MethodGet:
		writeJSON(w, 200, map[string]any{"inventories": s.workers.Inventories(r.Context())})
	case strings.HasPrefix(r.URL.Path, "/gateway/ui-api/model-aliases"):
		s.uiModelAliases(w, r, id)
	case strings.HasPrefix(r.URL.Path, "/gateway/ui-api/model-access"):
		s.uiModelAccess(w, r, id)
	case r.URL.Path == "/gateway/ui-api/placement" && r.Method == http.MethodGet:
		s.uiPlacementGet(w, r)
	case strings.HasPrefix(r.URL.Path, "/gateway/ui-api/placement/"):
		s.uiPlacementRule(w, r, id)
	case r.URL.Path == "/gateway/ui-api/models/pull" && r.Method == http.MethodPost:
		s.uiModelPull(w, r)
	case r.URL.Path == "/gateway/ui-api/models/action" && r.Method == http.MethodPost:
		s.uiModelAction(w, r)
	case r.URL.Path == "/gateway/ui-api/jobs" && r.Method == http.MethodGet:
		s.uiJobs(w, r)
	case strings.HasPrefix(r.URL.Path, "/gateway/ui-api/batches"):
		s.uiBatchJobs(w, r, id)
	case strings.HasPrefix(r.URL.Path, "/gateway/ui-api/jobs/") && strings.HasSuffix(r.URL.Path, "/cancel") && r.Method == http.MethodPost:
		jobID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/gateway/ui-api/jobs/"), "/cancel")
		if jobID == "" || !s.jobs.cancelJob(jobID) {
			proxy.WriteJSONError(w, 409, "not_running", "job is not running")
			return
		}
		s.log.Info("job cancellation requested", "request_id", jobID, "admin_subject", id.Subject, "admin_auth_type", id.AuthType)
		writeJSON(w, 202, map[string]any{"status": "cancelling", "id": jobID})
	case r.URL.Path == "/gateway/ui-api/operations" && r.Method == http.MethodGet:
		writeJSON(w, 200, map[string]any{"operations": s.ops.list()})
	case strings.HasPrefix(r.URL.Path, "/gateway/ui-api/operations/") && strings.HasSuffix(r.URL.Path, "/cancel") && r.Method == http.MethodPost:
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/gateway/ui-api/operations/"), "/cancel")
		if !s.ops.cancelOperation(id) {
			proxy.WriteJSONError(w, 409, "not_running", "operation is not running")
			return
		}
		writeJSON(w, 202, map[string]any{"status": "cancelling"})
	case r.URL.Path == "/gateway/ui-api/policies" && r.Method == http.MethodGet:
		overrides, err := s.policies.List(r.Context())
		if err != nil {
			proxy.WriteJSONError(w, 503, "policy_store", err.Error())
			return
		}
		writeJSON(w, 200, map[string]any{"baseline": s.cfg.Scheduler.Policies, "overrides": overrides})
	case strings.HasPrefix(r.URL.Path, "/gateway/ui-api/policies/"):
		tenant, err := url.PathUnescape(strings.TrimPrefix(r.URL.Path, "/gateway/ui-api/policies/"))
		if err != nil || tenant == "" || len(tenant) > 256 {
			proxy.WriteJSONError(w, 400, "bad_tenant", "invalid tenant")
			return
		}
		s.uiPolicy(w, r, tenant)
	case r.URL.Path == "/gateway/ui-api/api-keys" && r.Method == http.MethodGet:
		storage := "in-memory"
		if s.auth.HasPersistentRuntimeStore() {
			storage = "persistent-file"
		}
		writeJSON(w, 200, map[string]any{"keys": s.auth.APIKeys(), "storage": storage})
	case r.URL.Path == "/gateway/ui-api/api-keys" && r.Method == http.MethodPost:
		s.uiAPIKeyCreate(w, r, id)
	case strings.HasPrefix(r.URL.Path, "/gateway/ui-api/api-keys/") && r.Method == http.MethodDelete:
		s.uiAPIKeyDelete(w, r, id)
	case r.URL.Path == "/gateway/ui-api/config" && r.Method == http.MethodGet:
		writeJSON(w, 200, s.redactedConfig())
	case r.URL.Path == "/gateway/ui-api/config" && r.Method == http.MethodPut:
		s.uiConfigSave(w, r, id)
	case r.URL.Path == "/gateway/ui-api/config" && r.Method == http.MethodDelete:
		s.uiConfigReset(w, r, id)
	case r.URL.Path == "/gateway/ui-api/storage" && r.Method == http.MethodGet:
		writeJSON(w, 200, s.storageStatus())
	case r.URL.Path == "/gateway/ui-api/storage/flush" && r.Method == http.MethodPost:
		s.uiStorageFlush(w, r)
	case r.URL.Path == "/gateway/ui-api/storage/compact" && r.Method == http.MethodPost:
		s.uiStorageCompact(w, r)
	case r.URL.Path == "/gateway/ui-api/storage/backup" && r.Method == http.MethodGet:
		s.uiStorageBackup(w, r)
	default:
		proxy.WriteJSONError(w, 404, "not_found", "unknown UI endpoint")
	}
}

func (s *Server) uiWarmModelsGet(w http.ResponseWriter, r *http.Request) {
	if s.warm == nil {
		writeJSON(w, 200, map[string]any{"enabled": false, "policies": map[string]any{}, "baseline": map[string]any{}, "actions": []any{}, "eviction_suggestions": []any{}})
		return
	}
	writeJSON(w, 200, s.warm.Status())
}

func (s *Server) uiWarmModelsSave(w http.ResponseWriter, r *http.Request, actor auth.Identity) {
	if s.warm == nil {
		proxy.WriteJSONError(w, 503, "warm_models_unavailable", "warm model manager is unavailable")
		return
	}
	var in struct {
		Policies map[string]config.WarmModelPolicy `json:"policies"`
	}
	if err := decodeJSON(r, &in, 256<<10); err != nil {
		proxy.WriteJSONError(w, 400, "bad_warm_models", err.Error())
		return
	}
	if in.Policies == nil {
		in.Policies = map[string]config.WarmModelPolicy{}
	}
	if err := s.warm.SetPolicies(in.Policies); err != nil {
		proxy.WriteJSONError(w, 400, "bad_warm_models", err.Error())
		return
	}
	s.log.Info("warm model policies saved", "rules", len(in.Policies), "admin_subject", actor.Subject)
	writeJSON(w, 200, s.warm.Status())
}

func (s *Server) uiWarmModelsReset(w http.ResponseWriter, r *http.Request, actor auth.Identity) {
	if s.warm == nil {
		proxy.WriteJSONError(w, 503, "warm_models_unavailable", "warm model manager is unavailable")
		return
	}
	if err := s.warm.Reset(); err != nil {
		proxy.WriteJSONError(w, 503, "warm_models_store", err.Error())
		return
	}
	s.log.Info("warm model policies reset", "admin_subject", actor.Subject)
	writeJSON(w, 200, s.warm.Status())
}

func (s *Server) uiWarmModelsReconcile(w http.ResponseWriter, r *http.Request, actor auth.Identity) {
	if s.warm == nil {
		proxy.WriteJSONError(w, 503, "warm_models_unavailable", "warm model manager is unavailable")
		return
	}
	s.warm.Reconcile(context.Background())
	s.log.Info("warm model reconciliation requested", "admin_subject", actor.Subject)
	writeJSON(w, 202, s.warm.Status())
}

func (s *Server) uiAlertsGet(w http.ResponseWriter, r *http.Request) {
	if s.alerts == nil {
		writeJSON(w, 200, map[string]any{"enabled": false, "active": []any{}, "history": []any{}, "deliveries": []any{}})
		return
	}
	writeJSON(w, 200, s.alerts.Status())
}

func (s *Server) uiAlertsTest(w http.ResponseWriter, r *http.Request, actor auth.Identity) {
	if s.alerts == nil {
		proxy.WriteJSONError(w, 503, "alerts_unavailable", "alerts manager is unavailable")
		return
	}
	var in struct {
		Name string `json:"name"`
	}
	if err := decodeJSON(r, &in, 16<<10); err != nil {
		proxy.WriteJSONError(w, 400, "bad_webhook", err.Error())
		return
	}
	if err := s.alerts.TestWebhook(strings.TrimSpace(in.Name)); err != nil {
		proxy.WriteJSONError(w, 502, "webhook_test", err.Error())
		return
	}
	s.log.Info("alert webhook test sent", "webhook", in.Name, "admin_subject", actor.Subject)
	writeJSON(w, 200, map[string]any{"sent": true})
}

func (s *Server) uiAutoTuneGet(w http.ResponseWriter, r *http.Request) {
	if s.autoTune == nil {
		writeJSON(w, 200, map[string]any{"enabled": false, "profiles": []any{}, "applied": map[string]any{}, "config": s.cfg.AutoTuning})
		return
	}
	writeJSON(w, 200, map[string]any{"enabled": s.cfg.AutoTuning.Enabled, "profiles": s.autoTune.List(), "applied": s.autoTune.Applied(), "config": s.cfg.AutoTuning})
}

func (s *Server) uiAutoTuneStart(w http.ResponseWriter, r *http.Request, actor auth.Identity) {
	if s.autoTune == nil || !s.cfg.AutoTuning.Enabled {
		proxy.WriteJSONError(w, 409, "autotune_disabled", "auto tuning is disabled")
		return
	}
	var in autotune.StartRequest
	if err := decodeJSON(r, &in, 64<<10); err != nil {
		proxy.WriteJSONError(w, 400, "bad_autotune", err.Error())
		return
	}
	p, err := s.autoTune.Start(context.Background(), in)
	if err != nil {
		proxy.WriteJSONError(w, 400, "bad_autotune", err.Error())
		return
	}
	s.log.Info("auto-tune benchmark started", "profile_id", p.ID, "worker", p.Worker, "model", p.Model, "admin_subject", actor.Subject)
	writeJSON(w, http.StatusAccepted, map[string]any{"profile": p})
}

func (s *Server) uiAutoTuneAction(w http.ResponseWriter, r *http.Request, actor auth.Identity) {
	if s.autoTune == nil {
		proxy.WriteJSONError(w, 404, "autotune_unavailable", "auto tuning is unavailable")
		return
	}
	rest := strings.Trim(strings.TrimPrefix(r.URL.Path, "/gateway/ui-api/autotune/"), "/")
	if rest == "reset" && r.Method == http.MethodPost {
		var in struct {
			Worker string `json:"worker"`
			Model  string `json:"model"`
		}
		if err := decodeJSON(r, &in, 16<<10); err != nil || strings.TrimSpace(in.Worker) == "" || strings.TrimSpace(in.Model) == "" {
			proxy.WriteJSONError(w, 400, "bad_autotune_reset", "worker and model are required")
			return
		}
		if err := s.autoTune.Reset(in.Worker, in.Model); err != nil {
			proxy.WriteJSONError(w, 503, "autotune_store", err.Error())
			return
		}
		if err := s.workers.ResetModelConcurrency(in.Worker, in.Model); err != nil {
			proxy.WriteJSONError(w, 400, "worker_config", err.Error())
			return
		}
		s.log.Info("auto-tune override reset", "worker", in.Worker, "model", in.Model, "admin_subject", actor.Subject)
		writeJSON(w, 200, map[string]any{"status": "reset", "worker": in.Worker, "model": in.Model})
		return
	}
	parts := strings.Split(rest, "/")
	if len(parts) != 2 || parts[0] == "" {
		proxy.WriteJSONError(w, 404, "not_found", "unknown auto-tune action")
		return
	}
	profileID, action := parts[0], parts[1]
	switch {
	case action == "cancel" && r.Method == http.MethodPost:
		if !s.autoTune.Cancel(profileID) {
			proxy.WriteJSONError(w, 409, "not_running", "benchmark is not running")
			return
		}
		s.log.Info("auto-tune benchmark cancellation requested", "profile_id", profileID, "admin_subject", actor.Subject)
		writeJSON(w, http.StatusAccepted, map[string]any{"status": "cancelling", "id": profileID})
	case action == "apply" && r.Method == http.MethodPost:
		p, err := s.autoTune.Apply(profileID)
		if err != nil {
			proxy.WriteJSONError(w, 409, "autotune_apply", err.Error())
			return
		}
		if err := s.workers.SetModelConcurrency(p.Worker, p.Model, p.RecommendedConcurrency); err != nil {
			_ = s.autoTune.Reset(p.Worker, p.Model)
			proxy.WriteJSONError(w, 400, "worker_config", err.Error())
			return
		}
		s.log.Info("auto-tune recommendation applied", "profile_id", profileID, "worker", p.Worker, "model", p.Model, "concurrency", p.RecommendedConcurrency, "admin_subject", actor.Subject)
		writeJSON(w, 200, map[string]any{"profile": p})
	default:
		proxy.WriteJSONError(w, 404, "not_found", "unknown auto-tune action")
	}
}

func (s *Server) uiWorkerRuntime(w http.ResponseWriter, r *http.Request, actor auth.Identity) {
	rest := strings.TrimPrefix(r.URL.Path, "/gateway/ui-api/workers/")
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) < 2 {
		proxy.WriteJSONError(w, 404, "not_found", "unknown worker action")
		return
	}
	name, err := url.PathUnescape(parts[0])
	if err != nil || name == "" {
		proxy.WriteJSONError(w, 400, "bad_worker", "invalid worker")
		return
	}
	if _, ok := s.workers.URLFor(name); !ok {
		proxy.WriteJSONError(w, 404, "worker_not_found", "worker not found")
		return
	}
	switch {
	case len(parts) == 2 && parts[1] == "maintenance" && r.Method == http.MethodPost:
		var in struct {
			Mode string `json:"mode"`
		}
		if err := decodeJSON(r, &in, 16<<10); err != nil {
			proxy.WriteJSONError(w, 400, "bad_worker_mode", err.Error())
			return
		}
		if err := s.workers.SetMaintenance(name, in.Mode); err != nil {
			proxy.WriteJSONError(w, 400, "bad_worker_mode", err.Error())
			return
		}
		if s.workerStateStore != nil {
			if strings.EqualFold(strings.TrimSpace(in.Mode), "active") {
				err = s.workerStateStore.Delete(r.Context(), name)
			} else {
				err = s.workerStateStore.Put(r.Context(), name, strings.ToLower(strings.TrimSpace(in.Mode)))
			}
			if err != nil {
				proxy.WriteJSONError(w, 503, "worker_state_store", err.Error())
				return
			}
		}
		s.log.Info("worker maintenance changed", "worker", name, "mode", in.Mode, "admin_subject", actor.Subject)
		if s.warm != nil {
			s.warm.Wake()
		}
		writeJSON(w, 200, map[string]any{"worker": name, "mode": in.Mode})
	case len(parts) == 3 && parts[1] == "circuit" && parts[2] == "reset" && r.Method == http.MethodPost:
		if err := s.workers.CircuitReset(name); err != nil {
			proxy.WriteJSONError(w, 400, "circuit_reset", err.Error())
			return
		}
		s.log.Info("worker circuit reset", "worker", name, "admin_subject", actor.Subject)
		if s.warm != nil {
			s.warm.Wake()
		}
		writeJSON(w, 200, map[string]any{"worker": name, "circuit_state": "closed"})
	default:
		proxy.WriteJSONError(w, 404, "not_found", "unknown worker action")
	}
}

func (s *Server) uiLiveStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		proxy.WriteJSONError(w, 500, "streaming_unavailable", "response writer does not support streaming")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-store")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("X-Content-Type-Options", "nosniff")

	send := func() error {
		b, err := json.Marshal(s.live.Snapshot())
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "event: snapshot\ndata: %s\n\n", b); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}
	changed := s.live.Changed()
	if err := send(); err != nil {
		return
	}
	// Coalesce bursts of queue/progress changes. The browser animation runs at
	// display refresh rate, so sending more than four metadata snapshots per
	// second adds network/JSON work without making the pulse map smoother.
	flushTicker := time.NewTicker(250 * time.Millisecond)
	heartbeat := time.NewTicker(15 * time.Second)
	defer flushTicker.Stop()
	defer heartbeat.Stop()
	dirty := false
	for {
		select {
		case <-r.Context().Done():
			return
		case <-changed:
			changed = s.live.Changed()
			dirty = true
		case <-flushTicker.C:
			if dirty {
				if err := send(); err != nil {
					return
				}
				dirty = false
			}
		case <-heartbeat.C:
			if _, err := fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *Server) uiInfrastructureStream(w http.ResponseWriter, r *http.Request) {
	if s.infrastructure == nil {
		proxy.WriteJSONError(w, 503, "infrastructure_unavailable", "infrastructure live view is unavailable")
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		proxy.WriteJSONError(w, 500, "streaming_unavailable", "response writer does not support streaming")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-store")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	send := func() error {
		b, err := json.Marshal(s.infrastructure.Snapshot())
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "event: snapshot\ndata: %s\n\n", b); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}
	changed := s.infrastructure.Changed()
	if err := send(); err != nil {
		return
	}
	flushTicker := time.NewTicker(250 * time.Millisecond)
	heartbeat := time.NewTicker(15 * time.Second)
	defer flushTicker.Stop()
	defer heartbeat.Stop()
	dirty := false
	for {
		select {
		case <-r.Context().Done():
			return
		case <-changed:
			changed = s.infrastructure.Changed()
			dirty = true
		case <-flushTicker.C:
			if dirty {
				if err := send(); err != nil {
					return
				}
				dirty = false
			}
		case <-heartbeat.C:
			if _, err := fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *Server) uiAPIKeyCreate(w http.ResponseWriter, r *http.Request, actor auth.Identity) {
	var in struct {
		Name          string   `json:"name"`
		Tenant        string   `json:"tenant"`
		Subject       string   `json:"subject"`
		Application   string   `json:"application"`
		Scopes        []string `json:"scopes"`
		AllowedModels []string `json:"allowed_models"`
		DeniedModels  []string `json:"denied_models"`
		ServiceClass  string   `json:"service_class"`
	}
	if err := decodeJSON(r, &in, 64<<10); err != nil {
		proxy.WriteJSONError(w, 400, "bad_api_key", err.Error())
		return
	}
	if cls := strings.TrimSpace(in.ServiceClass); cls != "" {
		if _, ok := s.cfg.ServiceClasses.Classes[cls]; !ok {
			proxy.WriteJSONError(w, 400, "bad_service_class", "unknown service class")
			return
		}
	}
	info, secret, err := s.auth.CreateAPIKey(auth.APIKeyCreate{Name: in.Name, Tenant: in.Tenant, Subject: in.Subject, Application: in.Application, Scopes: in.Scopes, AllowedModels: in.AllowedModels, DeniedModels: in.DeniedModels, ServiceClass: in.ServiceClass})
	if err != nil {
		proxy.WriteJSONError(w, 400, "bad_api_key", err.Error())
		return
	}
	s.log.Info("API key created", "key_id", info.ID, "key_name", info.Name, "tenant", info.Tenant, "admin_subject", actor.Subject, "admin_auth_type", actor.AuthType)
	writeJSON(w, http.StatusCreated, map[string]any{"key": info, "secret": secret, "warning": "This secret is shown only once. Only its SHA-256 hash is stored persistently."})
}

func (s *Server) uiAPIKeyDelete(w http.ResponseWriter, r *http.Request, actor auth.Identity) {
	raw := strings.TrimPrefix(r.URL.Path, "/gateway/ui-api/api-keys/")
	id, err := url.PathUnescape(raw)
	if err != nil || id == "" || strings.Contains(id, "/") || len(id) > 128 {
		proxy.WriteJSONError(w, 400, "bad_api_key_id", "invalid API key id")
		return
	}
	info, ok, err := s.auth.DeleteAPIKey(id)
	if err != nil {
		proxy.WriteJSONError(w, 503, "api_key_store", err.Error())
		return
	}
	if !ok {
		proxy.WriteJSONError(w, 404, "api_key_not_found", "persistent API key not found")
		return
	}
	s.log.Info("API key deleted", "key_id", info.ID, "key_name", info.Name, "tenant", info.Tenant, "admin_subject", actor.Subject, "admin_auth_type", actor.AuthType)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) uiModelPull(w http.ResponseWriter, r *http.Request) {
	var in struct{ Worker, Model string }
	if err := decodeJSON(r, &in, 64<<10); err != nil || in.Worker == "" || in.Model == "" {
		proxy.WriteJSONError(w, 400, "bad_request", "worker and model are required")
		return
	}
	u, ok := s.workers.URLFor(in.Worker)
	if !ok {
		proxy.WriteJSONError(w, 404, "worker_not_found", "unknown worker")
		return
	}
	op := s.ops.startPull(u, in.Worker, in.Model)
	writeJSON(w, 202, op)
}
func (s *Server) uiModelAction(w http.ResponseWriter, r *http.Request) {
	var in struct{ Action, Worker, Model string }
	if err := decodeJSON(r, &in, 64<<10); err != nil || in.Worker == "" || in.Model == "" {
		proxy.WriteJSONError(w, 400, "bad_request", "action, worker and model are required")
		return
	}
	if in.Action != "stop" && in.Action != "delete" {
		proxy.WriteJSONError(w, 400, "bad_action", "supported actions are stop and delete")
		return
	}
	u, ok := s.workers.URLFor(in.Worker)
	if !ok {
		proxy.WriteJSONError(w, 404, "worker_not_found", "unknown worker")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()
	if err := s.ops.modelAction(ctx, u, in.Action, in.Model); err != nil {
		proxy.WriteJSONError(w, 502, "ollama_error", err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"status": "ok"})
}

func (s *Server) uiPlacementGet(w http.ResponseWriter, r *http.Request) {
	placements := s.workers.PlacementSnapshots()
	installed := make(map[string]map[string]bool, len(placements))
	loaded := make(map[string]map[string]bool, len(placements))
	modelSet := map[string]bool{}
	invErrors := map[string]string{}
	for _, p := range placements {
		iset := map[string]bool{}
		for _, model := range p.InstalledModels {
			iset[model] = true
			modelSet[model] = true
		}
		installed[p.Worker] = iset
		lset := map[string]bool{}
		for _, model := range p.LoadedModels {
			lset[model] = true
			modelSet[model] = true
		}
		loaded[p.Worker] = lset
		if p.InventoryError != "" {
			invErrors[p.Worker] = p.InventoryError
		}
	}
	models := make([]string, 0, len(modelSet))
	for model := range modelSet {
		models = append(models, model)
	}
	sort.Strings(models)
	rows := make([]map[string]any, 0, len(models))
	for _, model := range models {
		cells := map[string]any{}
		for _, p := range placements {
			decision, _ := s.workers.PlacementDecision(p.Worker, model)
			cells[p.Worker] = map[string]any{
				"allowed":        decision.Allowed,
				"source":         decision.Source,
				"pattern":        decision.Pattern,
				"exact_override": decision.ExactOverride,
				"installed":      installed[p.Worker][model],
				"loaded":         loaded[p.Worker][model],
			}
		}
		rows = append(rows, map[string]any{"model": model, "workers": cells})
	}
	writeJSON(w, 200, map[string]any{"workers": placements, "models": rows, "inventory_errors": invErrors, "semantics": map[string]any{"rule_order": "most-specific-match; deny wins ties", "fallback_allow_all": true, "prefix_wildcard": "trailing * only"}})
}

func (s *Server) uiPlacementRule(w http.ResponseWriter, r *http.Request, actor auth.Identity) {
	if s.placementStore == nil {
		proxy.WriteJSONError(w, 503, "placement_store", "persistent model placement store unavailable")
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, "/gateway/ui-api/placement/")
	isModelAction := strings.HasSuffix(rest, "/model")
	if isModelAction {
		rest = strings.TrimSuffix(rest, "/model")
	}
	workerName, err := url.PathUnescape(strings.Trim(rest, "/"))
	if err != nil || workerName == "" || strings.Contains(workerName, "/") || len(workerName) > 256 {
		proxy.WriteJSONError(w, 400, "bad_worker", "invalid worker name")
		return
	}
	if _, ok := s.workers.URLFor(workerName); !ok {
		proxy.WriteJSONError(w, 404, "worker_not_found", "unknown worker")
		return
	}
	if isModelAction {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		s.uiPlacementModelAction(w, r, actor, workerName)
		return
	}
	switch r.Method {
	case http.MethodPut:
		var rule config.ModelPlacementRule
		if err := decodeJSON(r, &rule, 128<<10); err != nil {
			proxy.WriteJSONError(w, 400, "bad_placement", err.Error())
			return
		}
		rule = normalizePlacementForUI(rule)
		if err := config.ValidateModelPlacementRule(rule); err != nil {
			proxy.WriteJSONError(w, 400, "bad_placement", err.Error())
			return
		}
		if err := s.placementStore.Put(r.Context(), workerName, rule); err != nil {
			proxy.WriteJSONError(w, 503, "placement_store", err.Error())
			return
		}
		if err := s.workers.SetPlacement(workerName, rule, true); err != nil {
			proxy.WriteJSONError(w, 500, "placement_runtime", err.Error())
			return
		}
		s.log.Info("model placement override saved", "worker", workerName, "mode", rule.Mode, "allowed", len(rule.AllowedModels), "denied", len(rule.DeniedModels), "admin_subject", actor.Subject)
		if s.warm != nil {
			s.warm.Wake()
		}
		writeJSON(w, 200, map[string]any{"worker": workerName, "rule": rule, "override": true, "applied": true})
	case http.MethodDelete:
		if err := s.placementStore.Delete(r.Context(), workerName); err != nil {
			proxy.WriteJSONError(w, 503, "placement_store", err.Error())
			return
		}
		if err := s.workers.ResetPlacement(workerName); err != nil {
			proxy.WriteJSONError(w, 500, "placement_runtime", err.Error())
			return
		}
		s.log.Info("model placement override reset", "worker", workerName, "admin_subject", actor.Subject)
		if s.warm != nil {
			s.warm.Wake()
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) uiPlacementModelAction(w http.ResponseWriter, r *http.Request, actor auth.Identity, workerName string) {
	var in struct {
		Model  string `json:"model"`
		Action string `json:"action"` // allow | deny | inherit
	}
	if err := decodeJSON(r, &in, 32<<10); err != nil {
		proxy.WriteJSONError(w, 400, "bad_placement_action", err.Error())
		return
	}
	in.Model = strings.TrimSpace(in.Model)
	if in.Model == "" || strings.Contains(in.Model, "*") || len(in.Model) > 512 {
		proxy.WriteJSONError(w, 400, "bad_model", "model must be a non-empty exact model name")
		return
	}
	if in.Action != "allow" && in.Action != "deny" && in.Action != "inherit" {
		proxy.WriteJSONError(w, 400, "bad_action", "action must be allow, deny, or inherit")
		return
	}
	var baseline config.ModelPlacementRule
	var effective config.ModelPlacementRule
	for _, p := range s.workers.PlacementSnapshots() {
		if p.Worker == workerName {
			baseline, effective = p.Baseline, p.Effective
			break
		}
	}
	effective = normalizePlacementForUI(effective)
	effective.AllowedModels = removePlacementExact(effective.AllowedModels, in.Model)
	effective.DeniedModels = removePlacementExact(effective.DeniedModels, in.Model)
	switch in.Action {
	case "allow":
		effective.AllowedModels = append(effective.AllowedModels, in.Model)
	case "deny":
		effective.DeniedModels = append(effective.DeniedModels, in.Model)
	}
	effective = normalizePlacementForUI(effective)
	baseline = normalizePlacementForUI(baseline)
	if placementRulesEqual(effective, baseline) {
		if err := s.placementStore.Delete(r.Context(), workerName); err != nil {
			proxy.WriteJSONError(w, 503, "placement_store", err.Error())
			return
		}
		if err := s.workers.ResetPlacement(workerName); err != nil {
			proxy.WriteJSONError(w, 500, "placement_runtime", err.Error())
			return
		}
	} else {
		if err := s.placementStore.Put(r.Context(), workerName, effective); err != nil {
			proxy.WriteJSONError(w, 503, "placement_store", err.Error())
			return
		}
		if err := s.workers.SetPlacement(workerName, effective, true); err != nil {
			proxy.WriteJSONError(w, 500, "placement_runtime", err.Error())
			return
		}
	}
	s.log.Info("model placement exact rule changed", "worker", workerName, "model", in.Model, "action", in.Action, "admin_subject", actor.Subject)
	if s.warm != nil {
		s.warm.Wake()
	}
	decision, _ := s.workers.PlacementDecision(workerName, in.Model)
	writeJSON(w, 200, map[string]any{"worker": workerName, "model": in.Model, "action": in.Action, "decision": decision, "rule": effective})
}

func normalizePlacementForUI(r config.ModelPlacementRule) config.ModelPlacementRule {
	if strings.TrimSpace(r.Mode) == "" {
		r.Mode = "allow_all"
	}
	r.Mode = strings.TrimSpace(r.Mode)
	norm := func(in []string) []string {
		out := make([]string, 0, len(in))
		seen := map[string]bool{}
		for _, x := range in {
			x = strings.TrimSpace(x)
			if x == "" || seen[x] {
				continue
			}
			seen[x] = true
			out = append(out, x)
		}
		sort.Strings(out)
		return out
	}
	r.AllowedModels = norm(r.AllowedModels)
	r.DeniedModels = norm(r.DeniedModels)
	return r
}

func removePlacementExact(in []string, model string) []string {
	out := in[:0]
	for _, x := range in {
		if strings.TrimSpace(x) != model {
			out = append(out, x)
		}
	}
	return out
}

func placementRulesEqual(a, b config.ModelPlacementRule) bool {
	a, b = normalizePlacementForUI(a), normalizePlacementForUI(b)
	if a.Mode != b.Mode || len(a.AllowedModels) != len(b.AllowedModels) || len(a.DeniedModels) != len(b.DeniedModels) {
		return false
	}
	for i := range a.AllowedModels {
		if a.AllowedModels[i] != b.AllowedModels[i] {
			return false
		}
	}
	for i := range a.DeniedModels {
		if a.DeniedModels[i] != b.DeniedModels[i] {
			return false
		}
	}
	return true
}
func (s *Server) uiJobs(w http.ResponseWriter, r *http.Request) {
	entries := s.jobs.list()
	byID := make(map[string]jobEntry, len(entries))
	for _, j := range entries {
		byID[j.ID] = j
	}
	snap := s.live.Snapshot()
	jobs := make([]jobView, 0, len(entries))
	for _, req := range snap.Requests {
		j, ok := byID[req.ID]
		if !ok {
			continue
		}
		jobs = append(jobs, jobView{Request: req, Cancellable: true, Cancelling: j.Cancelling})
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs, "scope": "in-memory-process"})
}

func (s *Server) uiPolicy(w http.ResponseWriter, r *http.Request, tenant string) {
	switch r.Method {
	case http.MethodPut:
		var p config.TenantPolicy
		if err := decodeJSON(r, &p, 64<<10); err != nil {
			proxy.WriteJSONError(w, 400, "bad_policy", err.Error())
			return
		}
		if p.TenantWeight <= 0 || p.ActorWeight <= 0 || p.ActorCreditsPerMinute < 0 || p.ActorBurstCredits < 0 || p.TenantCreditsPerMinute < 0 || p.TenantBurstCredits < 0 {
			proxy.WriteJSONError(w, 400, "bad_policy", "weights must be > 0 and credit values must be >= 0")
			return
		}
		if err := s.policies.Put(r.Context(), tenant, p); err != nil {
			proxy.WriteJSONError(w, 503, "policy_store", err.Error())
			return
		}
		writeJSON(w, 200, map[string]any{"tenant": tenant, "policy": p})
	case http.MethodDelete:
		if err := s.policies.Delete(r.Context(), tenant); err != nil {
			proxy.WriteJSONError(w, 503, "policy_store", err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func decodeJSON(r *http.Request, out any, maxBytes int64) error {
	defer r.Body.Close()
	dec := json.NewDecoder(io.LimitReader(r.Body, maxBytes))
	dec.DisallowUnknownFields()
	return dec.Decode(out)
}

func (s *Server) policyFor(ctx context.Context, tenant string) config.TenantPolicy {
	if s.policies != nil {
		if p, ok, err := s.policies.Get(ctx, tenant); err == nil && ok {
			return s.normalizePolicy(p)
		}
		if _, exact := s.cfg.Scheduler.Policies[tenant]; !exact {
			if p, ok, err := s.policies.Get(ctx, "*"); err == nil && ok {
				return s.normalizePolicy(p)
			}
		}
	}
	return s.cfg.Policy(tenant)
}
func (s *Server) normalizePolicy(p config.TenantPolicy) config.TenantPolicy {
	if p.TenantWeight <= 0 {
		p.TenantWeight = s.cfg.Scheduler.DefaultTenantWeight
	}
	if p.ActorWeight <= 0 {
		p.ActorWeight = s.cfg.Scheduler.DefaultActorWeight
	}
	return p
}

func (s *Server) redactedConfig() map[string]any {
	b, _ := json.Marshal(s.cfg)
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	out["model_aliases"] = s.aliasSnapshot()
	out["model_access"] = s.modelAccessSnapshot()
	if authMap, ok := out["auth"].(map[string]any); ok {
		if keys, ok := authMap["api_keys"].([]any); ok {
			for _, raw := range keys {
				if key, ok := raw.(map[string]any); ok && key["key"] != nil {
					key["key"] = "<redacted>"
				}
			}
		}
	}
	if uiMap, ok := out["ui"].(map[string]any); ok {
		if _, ok := uiMap["session_secret"]; ok {
			uiMap["session_secret"] = "<redacted>"
		}
		if oidc, ok := uiMap["oidc"].(map[string]any); ok {
			if _, ok := oidc["client_secret"]; ok {
				oidc["client_secret"] = "<redacted>"
			}
		}
	}
	if alertsMap, ok := out["alerts"].(map[string]any); ok {
		if hooks, ok := alertsMap["webhooks"].([]any); ok {
			for _, raw := range hooks {
				if hook, ok := raw.(map[string]any); ok {
					if secret, exists := hook["secret"]; exists && strings.TrimSpace(fmt.Sprint(secret)) != "" {
						hook["secret"] = "<redacted>"
					}
				}
			}
		}
	}
	if otelMap, ok := out["opentelemetry"].(map[string]any); ok {
		if headers, ok := otelMap["headers"].(map[string]any); ok {
			for k := range headers {
				headers[k] = "<redacted>"
			}
		}
	}
	if convMap, ok := out["conversations"].(map[string]any); ok {
		if secret, exists := convMap["encryption_key"]; exists && strings.TrimSpace(fmt.Sprint(secret)) != "" {
			convMap["encryption_key"] = "<redacted>"
		}
	}
	return out
}

func (s *Server) restoreRedactedConfigSecrets(candidate *config.Config) {
	if candidate == nil {
		return
	}
	candidate.Auth.APIKeys = append([]config.APIKeyConfig(nil), s.cfg.Auth.APIKeys...)
	if candidate.UI.SessionSecret == "" || candidate.UI.SessionSecret == "<redacted>" {
		candidate.UI.SessionSecret = s.cfg.UI.SessionSecret
	}
	if candidate.UI.OIDC.ClientSecret == "" || candidate.UI.OIDC.ClientSecret == "<redacted>" {
		candidate.UI.OIDC.ClientSecret = s.cfg.UI.OIDC.ClientSecret
	}
	if candidate.Conversations.EncryptionKey == "" || candidate.Conversations.EncryptionKey == "<redacted>" {
		candidate.Conversations.EncryptionKey = s.cfg.Conversations.EncryptionKey
	}
	for i := range candidate.Alerts.Webhooks {
		if candidate.Alerts.Webhooks[i].Secret != "" && candidate.Alerts.Webhooks[i].Secret != "<redacted>" {
			continue
		}
		name := candidate.Alerts.Webhooks[i].Name
		if i < len(s.cfg.Alerts.Webhooks) && s.cfg.Alerts.Webhooks[i].Name == name {
			candidate.Alerts.Webhooks[i].Secret = s.cfg.Alerts.Webhooks[i].Secret
			continue
		}
		for _, old := range s.cfg.Alerts.Webhooks {
			if old.Name == name {
				candidate.Alerts.Webhooks[i].Secret = old.Secret
				break
			}
		}
	}
	if candidate.OpenTelemetry.Headers == nil {
		candidate.OpenTelemetry.Headers = map[string]string{}
	}
	for k, v := range candidate.OpenTelemetry.Headers {
		if v == "<redacted>" {
			if old, ok := s.cfg.OpenTelemetry.Headers[k]; ok {
				candidate.OpenTelemetry.Headers[k] = old
			}
		}
	}
}

func (s *Server) uiConfigSave(w http.ResponseWriter, r *http.Request, actor auth.Identity) {
	if s.configStore == nil {
		proxy.WriteJSONError(w, 503, "config_store", "persistent configuration store unavailable")
		return
	}
	var raw map[string]any
	if err := decodeJSON(r, &raw, 4<<20); err != nil {
		proxy.WriteJSONError(w, 400, "bad_config", err.Error())
		return
	}
	b, err := json.Marshal(raw)
	if err != nil {
		proxy.WriteJSONError(w, 400, "bad_config", err.Error())
		return
	}
	candidate, err := config.ParseBytes(b)
	if err != nil {
		// The UI never exposes plaintext bootstrap secrets. Reparse after
		// restoring fields that are intentionally immutable through this editor.
		var tmp config.Config
		if json.Unmarshal(b, &tmp) == nil {
			s.restoreRedactedConfigSecrets(&tmp)
			b, _ = json.Marshal(tmp)
			candidate, err = config.ParseBytes(b)
		}
	}
	if err != nil {
		proxy.WriteJSONError(w, 400, "bad_config", err.Error())
		return
	}
	// Secrets are never exposed by the JSON editor. Restore redacted values
	// from the active bootstrap/effective config before persisting.
	s.restoreRedactedConfigSecrets(candidate)
	// Tests and programmatic embedders may construct Config without applying
	// defaults; compare against a default-normalized bootstrap storage value.
	expectedStorage := s.cfg.Storage
	if expectedStorage.WorkerStateFile == "" {
		expectedStorage.WorkerStateFile = "worker-state.json"
	}
	if expectedStorage.AutoTuneFile == "" {
		expectedStorage.AutoTuneFile = "auto-tune.json"
	}
	if expectedStorage.WarmModelsFile == "" {
		expectedStorage.WarmModelsFile = "warm-models.json"
	}
	if expectedStorage.AlertsFile == "" {
		expectedStorage.AlertsFile = "alerts.json"
	}
	if expectedStorage.ConversationsFile == "" {
		expectedStorage.ConversationsFile = "conversations.enc.json"
	}
	if expectedStorage.BatchJobsFile == "" {
		expectedStorage.BatchJobsFile = "batch-jobs.json"
	}
	if expectedStorage.BatchJobsDir == "" {
		expectedStorage.BatchJobsDir = "batch"
	}
	if candidate.Storage != expectedStorage {
		proxy.WriteJSONError(w, 400, "storage_bootstrap_only", "storage settings are bootstrap-only; change them in the startup config")
		return
	}
	if err := candidate.Validate(); err != nil {
		proxy.WriteJSONError(w, 400, "bad_config", err.Error())
		return
	}
	if err := s.configStore.Save(candidate); err != nil {
		proxy.WriteJSONError(w, 503, "config_store", err.Error())
		return
	}
	s.log.Info("persistent configuration saved", "path", s.configStore.Path(), "admin_subject", actor.Subject, "admin_auth_type", actor.AuthType)
	writeJSON(w, 200, map[string]any{"saved": true, "path": s.configStore.Path(), "restart_required": true, "message": "Configuration persisted. Restart the gateway to activate all changes."})
}

func (s *Server) uiConfigReset(w http.ResponseWriter, r *http.Request, actor auth.Identity) {
	if s.configStore == nil {
		proxy.WriteJSONError(w, 503, "config_store", "persistent configuration store unavailable")
		return
	}
	if err := s.configStore.Delete(); err != nil {
		proxy.WriteJSONError(w, 503, "config_store", err.Error())
		return
	}
	s.log.Info("persistent configuration override deleted", "path", s.configStore.Path(), "admin_subject", actor.Subject, "admin_auth_type", actor.AuthType)
	writeJSON(w, 200, map[string]any{"deleted": true, "restart_required": true, "message": "Persistent override removed. Restart to return to the bootstrap configuration."})
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
