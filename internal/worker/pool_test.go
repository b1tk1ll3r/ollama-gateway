package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/example/ollama-fair-gateway/internal/config"
)

func TestMetadataCapabilitiesAndContextCached(t *testing.T) {
	var showCalls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/ps":
			_ = json.NewEncoder(w).Encode(map[string]any{"models": []any{}})
		case "/api/tags":
			_ = json.NewEncoder(w).Encode(map[string]any{"models": []any{map[string]any{"name": "qwen:latest", "model": "qwen:latest"}}})
		case "/api/show":
			showCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"capabilities": []string{"completion", "tools", "thinking"},
				"details":      map[string]any{"family": "qwen"},
				"model_info":   map[string]any{"qwen.context_length": 32768},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer backend.Close()

	p := New([]config.WorkerConfig{{Name: "w", URL: backend.URL, MaxConcurrent: 2, HealthInterval: config.Duration(time.Hour)}}, "w")
	p.SetModelCapabilitiesConfig(config.ModelCapabilitiesConfig{Mode: "enforce", CacheTTL: config.Duration(time.Hour), ContextGuard: "reject"})
	p.Start(context.Background())
	m, worker, err := p.Metadata(context.Background(), "qwen:latest")
	if err != nil {
		t.Fatal(err)
	}
	if worker != "w" || m.ContextLength != 32768 || !HasCapability(m, "tools") {
		t.Fatalf("unexpected metadata: %#v worker=%s", m, worker)
	}
	if _, _, err := p.Metadata(context.Background(), "qwen:latest"); err != nil {
		t.Fatal(err)
	}
	if showCalls.Load() != 1 {
		t.Fatalf("expected one cached /api/show call, got %d", showCalls.Load())
	}
}

func TestPerModelConcurrency(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/ps":
			_ = json.NewEncoder(w).Encode(map[string]any{"models": []any{}})
		case "/api/tags":
			_ = json.NewEncoder(w).Encode(map[string]any{"models": []any{map[string]any{"name": "large:latest", "model": "large:latest"}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer backend.Close()
	p := New([]config.WorkerConfig{{Name: "w", URL: backend.URL, MaxConcurrent: 4, ModelConcurrency: map[string]int{"large:*": 1}, HealthInterval: config.Duration(time.Hour)}}, "w")
	p.Start(context.Background())
	l1, err := p.Acquire(context.Background(), "large:latest")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	if _, err := p.Acquire(ctx, "large:latest"); err == nil {
		t.Fatal("expected second large request to wait for model slot")
	}
	l1.Release()
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	l2, err := p.Acquire(ctx2, "large:latest")
	if err != nil {
		t.Fatal(err)
	}
	l2.Release()
}

func TestObserveLearnsThroughput(t *testing.T) {
	p := New([]config.WorkerConfig{{Name: "w", URL: "http://127.0.0.1:11434", MaxConcurrent: 1}}, "w")
	p.Observe("w", "m:latest", 100, 50, int64(time.Second), int64(2*time.Second), 3*time.Second)
	s := p.Snapshots()[0]
	if len(s.Performance) != 1 {
		t.Fatalf("performance missing: %#v", s.Performance)
	}
	if s.Performance[0].PromptTPS < 99 || s.Performance[0].OutputTPS < 24 {
		t.Fatalf("unexpected performance: %#v", s.Performance[0])
	}
}

func TestAdaptiveRoutingPrefersFasterEquivalentWorker(t *testing.T) {
	p := New([]config.WorkerConfig{
		{Name: "slow", URL: "http://127.0.0.1:11434", MaxConcurrent: 2},
		{Name: "fast", URL: "http://127.0.0.1:11435", MaxConcurrent: 2},
	}, "slow")
	p.SetRoutingConfig(config.RoutingConfig{LoadedBonus: 1, InstalledBonus: 1, ThroughputBonus: 80, VRAMPressurePenalty: 1, GPUUtilizationPenalty: 1, AvoidVRAMPercent: 99})
	for _, w := range p.workers {
		w.mu.Lock()
		w.installed["m"] = true
		w.models["m"] = true
		w.mu.Unlock()
	}
	p.Observe("slow", "m", 100, 100, int64(time.Second), int64(4*time.Second), 4*time.Second) // 25 tok/s
	p.Observe("fast", "m", 100, 100, int64(time.Second), int64(time.Second), time.Second)     // 100 tok/s
	c := p.candidates("m")
	if len(c) != 2 || c[0].cfg.Name != "fast" {
		t.Fatalf("expected faster worker first, got %#v", []string{c[0].cfg.Name, c[1].cfg.Name})
	}
}

func TestModelPlacementFiltersWorkersBeforeAdaptiveRouting(t *testing.T) {
	p := New([]config.WorkerConfig{
		{Name: "node1", URL: "http://127.0.0.1:11434", MaxConcurrent: 2, ModelPlacement: config.ModelPlacementRule{Mode: "whitelist", AllowedModels: []string{"model-a", "model-b"}}},
		{Name: "node2", URL: "http://127.0.0.1:11435", MaxConcurrent: 2, ModelPlacement: config.ModelPlacementRule{Mode: "whitelist", AllowedModels: []string{"model-b"}}},
	}, "node1")
	for _, w := range p.workers {
		w.mu.Lock()
		w.installedKnown = true
		w.installed["model-a"] = true
		w.installed["model-b"] = true
		w.mu.Unlock()
	}
	c := p.candidates("model-a")
	if len(c) != 1 || c[0].cfg.Name != "node1" {
		t.Fatalf("model-a candidates=%v", workerNames(c))
	}
	c = p.candidates("model-b")
	if len(c) != 2 {
		t.Fatalf("model-b candidates=%v", workerNames(c))
	}
	if err := p.SetPlacement("node1", config.ModelPlacementRule{Mode: "whitelist", AllowedModels: []string{"model-a"}}, true); err != nil {
		t.Fatal(err)
	}
	c = p.candidates("model-b")
	if len(c) != 1 || c[0].cfg.Name != "node2" {
		t.Fatalf("model-b after override candidates=%v", workerNames(c))
	}
}

func TestPlacementSpecificityAllowsExactExceptionToPrefixDeny(t *testing.T) {
	r := config.ModelPlacementRule{Mode: "allow_all", DeniedModels: []string{"gemma4:*"}, AllowedModels: []string{"gemma4:latest"}}
	if d := evaluatePlacement(r, "gemma4:latest"); !d.Allowed || d.Pattern != "gemma4:latest" || !d.ExactOverride {
		t.Fatalf("latest decision=%#v", d)
	}
	if d := evaluatePlacement(r, "gemma4:e4b"); d.Allowed || d.Pattern != "gemma4:*" {
		t.Fatalf("e4b decision=%#v", d)
	}
}

func TestPlacementKnownInventoryDoesNotRouteToWorkerWithoutModel(t *testing.T) {
	p := New([]config.WorkerConfig{
		{Name: "a", URL: "http://127.0.0.1:11434", MaxConcurrent: 1},
		{Name: "b", URL: "http://127.0.0.1:11435", MaxConcurrent: 1},
	}, "a")
	for _, w := range p.workers {
		w.mu.Lock()
		w.installedKnown = true
		w.mu.Unlock()
	}
	p.byName["a"].mu.Lock()
	p.byName["a"].installed["m"] = true
	p.byName["a"].mu.Unlock()
	c := p.candidates("m")
	if len(c) != 1 || c[0].cfg.Name != "a" {
		t.Fatalf("candidates=%v", workerNames(c))
	}
	if got := p.candidates("missing"); len(got) != 0 {
		t.Fatalf("missing model candidates=%v", workerNames(got))
	}
}

func workerNames(in []*state) []string {
	out := make([]string, 0, len(in))
	for _, w := range in {
		out = append(out, w.cfg.Name)
	}
	return out
}

func TestTagsExcludeModelsOnlyAvailableOnPlacementBlockedWorkers(t *testing.T) {
	backend := func(model string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/api/ps":
				_ = json.NewEncoder(w).Encode(map[string]any{"models": []any{}})
			case "/api/tags":
				_ = json.NewEncoder(w).Encode(map[string]any{"models": []any{map[string]any{"name": model, "model": model}}})
			case "/api/show":
				_ = json.NewEncoder(w).Encode(map[string]any{"capabilities": []string{"completion"}})
			default:
				http.NotFound(w, r)
			}
		}))
	}
	a := backend("model-a:latest")
	defer a.Close()
	b := backend("model-b:latest")
	defer b.Close()
	p := New([]config.WorkerConfig{
		{Name: "a", URL: a.URL, MaxConcurrent: 1, HealthInterval: config.Duration(time.Hour), ModelPlacement: config.ModelPlacementRule{Mode: "allow_all"}},
		{Name: "b", URL: b.URL, MaxConcurrent: 1, HealthInterval: config.Duration(time.Hour), ModelPlacement: config.ModelPlacementRule{Mode: "whitelist", AllowedModels: []string{"model-a:*"}}},
	}, "a")
	p.Start(context.Background())
	tags, errs := p.Tags(context.Background())
	if len(errs) != 0 {
		t.Fatalf("errs=%v", errs)
	}
	if len(tags) != 1 || tags[0].Model != "model-a:latest" {
		t.Fatalf("tags=%#v", tags)
	}
}

func TestMaintenanceAndCircuitBreakerExcludeWorker(t *testing.T) {
	p := New([]config.WorkerConfig{{Name: "w", URL: "http://127.0.0.1:11434", MaxConcurrent: 1}}, "w")
	p.SetReliabilityConfig(config.ReliabilityConfig{Enabled: true, FailureThreshold: 1, OpenDuration: config.Duration(time.Hour), RetryAttempts: 2})
	w := p.byName["w"]
	w.mu.Lock()
	w.installedKnown = true
	w.installed["m"] = true
	w.mu.Unlock()
	if !p.CanRoute("m") {
		t.Fatal("worker should initially route")
	}
	if err := p.SetMaintenance("w", "draining"); err != nil {
		t.Fatal(err)
	}
	if p.CanRoute("m") {
		t.Fatal("draining worker must not accept new work")
	}
	if err := p.SetMaintenance("w", "active"); err != nil {
		t.Fatal(err)
	}
	p.ReportResult("w", true, "boom")
	if p.CanRoute("m") {
		t.Fatal("open circuit must exclude worker")
	}
	snap := p.Snapshots()[0]
	if snap.CircuitState != "open" || snap.CircuitFailures != 1 {
		t.Fatalf("snapshot=%#v", snap)
	}
	if err := p.CircuitReset("w"); err != nil {
		t.Fatal(err)
	}
	if !p.CanRoute("m") {
		t.Fatal("reset circuit should route")
	}
}

func TestModelMaintenanceBlocksInferenceAcquire(t *testing.T) {
	p := New([]config.WorkerConfig{{Name: "w", URL: "http://127.0.0.1:11434", MaxConcurrent: 1}}, "w")
	release, err := p.BeginModelMaintenance("w", "qwen3:8b")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	if lease, err := p.Acquire(ctx, "qwen3:8b"); err == nil {
		lease.Release()
		t.Fatal("inference acquired worker while model maintenance was active")
	}
	release()
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	lease, err := p.Acquire(ctx2, "qwen3:8b")
	if err != nil {
		t.Fatalf("acquire after maintenance release: %v", err)
	}
	lease.Release()
}

func TestContextWindowsPreferLoadedThenModelfileContext(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/ps":
			_ = json.NewEncoder(w).Encode(map[string]any{"models": []any{map[string]any{"name": "qwen:latest", "model": "qwen:latest", "context_length": 8192}}})
		case "/api/tags":
			_ = json.NewEncoder(w).Encode(map[string]any{"models": []any{map[string]any{"name": "qwen:latest", "model": "qwen:latest"}}})
		case "/api/show":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"capabilities": []string{"completion"},
				"model_info":   map[string]any{"qwen.context_length": 131072},
				"parameters":   "num_ctx 16384\ntemperature 0.7",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer backend.Close()
	p := New([]config.WorkerConfig{{Name: "w", URL: backend.URL, MaxConcurrent: 1, HealthInterval: config.Duration(time.Hour), ContextLimits: map[string]int64{"qwen:*": 32768}}}, "w")
	p.SetModelCapabilitiesConfig(config.ModelCapabilitiesConfig{Mode: "enforce", CacheTTL: config.Duration(time.Hour), ContextGuard: "reject", Context: config.ContextPolicyConfig{DefaultWorkerTokens: 4096, MaxRequestedTokens: 32768}})
	p.Start(context.Background())
	windows := p.ContextWindows(context.Background(), "qwen:latest")
	if len(windows) != 1 {
		t.Fatalf("windows=%#v", windows)
	}
	x := windows[0]
	if x.ModelMaxTokens != 131072 || x.ConfiguredTokens != 16384 || x.LoadedTokens != 8192 || x.EffectiveTokens != 8192 || x.EffectiveSource != "loaded" || x.WorkerLimitTokens != 32768 {
		t.Fatalf("unexpected loaded context window: %#v", x)
	}
	inv := p.Inventories(context.Background())
	if len(inv) != 1 || len(inv[0].Models) != 1 || inv[0].Models[0].ContextLength != 131072 || inv[0].Models[0].ConfiguredContextLength != 16384 || inv[0].Models[0].LoadedContextLength != 8192 {
		t.Fatalf("inventory context fields missing: %#v", inv)
	}
	p.byName["w"].mu.Lock()
	p.byName["w"].loadedModels = nil
	p.byName["w"].models = map[string]bool{}
	p.byName["w"].mu.Unlock()
	windows = p.ContextWindows(context.Background(), "qwen:latest")
	if len(windows) != 1 || windows[0].EffectiveTokens != 16384 || windows[0].EffectiveSource != "modelfile" {
		t.Fatalf("unexpected unloaded context window: %#v", windows)
	}
}

func TestAcquireAllowedContextDoesNotPreferUndersizedLoadedContext(t *testing.T) {
	p := New([]config.WorkerConfig{
		{Name: "smallctx", URL: "http://127.0.0.1:11434", MaxConcurrent: 1},
		{Name: "largectx", URL: "http://127.0.0.1:11435", MaxConcurrent: 1},
	}, "smallctx")
	p.SetRoutingConfig(config.RoutingConfig{LoadedBonus: 100, InstalledBonus: 10, ThroughputBonus: 1, VRAMPressurePenalty: 1, GPUUtilizationPenalty: 1, AvoidVRAMPercent: 99})
	for _, w := range p.workers {
		w.mu.Lock()
		w.installedKnown = true
		w.installed["m"] = true
		w.models["m"] = true
		ctx := int64(4096)
		if w.cfg.Name == "largectx" {
			ctx = 16384
		}
		w.loadedModels = []LoadedModel{{Name: "m", Model: "m", ContextLength: ctx}}
		w.mu.Unlock()
	}
	lease, err := p.AcquireAllowed(context.Background(), "m", map[string]bool{"smallctx": true, "largectx": true}, 8192)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	if lease.Name() != "largectx" {
		t.Fatalf("selected %s, want largectx", lease.Name())
	}
}

func TestContextWindowsUsesPerWorkerDefaultContext(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/ps":
			_ = json.NewEncoder(w).Encode(map[string]any{"models": []any{}})
		case "/api/tags":
			_ = json.NewEncoder(w).Encode(map[string]any{"models": []any{map[string]any{"name": "m", "model": "m"}}})
		case "/api/show":
			_ = json.NewEncoder(w).Encode(map[string]any{"capabilities": []string{"completion"}, "model_info": map[string]any{"m.context_length": 131072}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer backend.Close()
	p := New([]config.WorkerConfig{{Name: "w", URL: backend.URL, MaxConcurrent: 1, HealthInterval: config.Duration(time.Hour), DefaultContextTokens: 12288}}, "w")
	p.SetModelCapabilitiesConfig(config.ModelCapabilitiesConfig{Mode: "enforce", CacheTTL: config.Duration(time.Hour), ContextGuard: "reject", Context: config.ContextPolicyConfig{DefaultWorkerTokens: 4096, MaxRequestedTokens: 32768}})
	p.Start(context.Background())
	windows := p.ContextWindows(context.Background(), "m")
	if len(windows) != 1 || windows[0].EffectiveTokens != 12288 || windows[0].EffectiveSource != "worker_default" {
		t.Fatalf("windows=%#v", windows)
	}
}

func TestParseNumCtxParameterStringAndObject(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want int64
	}{
		{`"num_ctx 8192\ntemperature 0.7"`, 8192},
		{`{"num_ctx":16384}`, 16384},
		{`{"num_ctx":"32768"}`, 32768},
		{`"temperature 0.7"`, 0},
	} {
		if got := parseNumCtxParameter(json.RawMessage(tc.raw)); got != tc.want {
			t.Fatalf("raw=%s got=%d want=%d", tc.raw, got, tc.want)
		}
	}
}
