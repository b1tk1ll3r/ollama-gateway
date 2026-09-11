package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		v, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		*d = Duration(v)
		return nil
	}
	var n int64
	if err := json.Unmarshal(b, &n); err != nil {
		return errors.New("duration must be a Go duration string or nanoseconds")
	}
	*d = Duration(time.Duration(n))
	return nil
}
func (d Duration) Value() time.Duration         { return time.Duration(d) }
func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

type Config struct {
	Server            ServerConfig                `json:"server"`
	Auth              AuthConfig                  `json:"auth"`
	Scheduler         SchedulerConfig             `json:"scheduler"`
	Quota             QuotaConfig                 `json:"quota"`
	Cost              CostConfig                  `json:"cost"`
	Workers           []WorkerConfig              `json:"workers"`
	Usage             UsageConfig                 `json:"usage"`
	Native            NativeConfig                `json:"native"`
	UI                UIConfig                    `json:"ui"`
	PublicDashboard   PublicDashboardConfig       `json:"public_dashboard"`
	Infrastructure    InfrastructureConfig        `json:"infrastructure"`
	ModelCapabilities ModelCapabilitiesConfig     `json:"model_capabilities"`
	Routing           RoutingConfig               `json:"routing"`
	Reliability       ReliabilityConfig           `json:"reliability"`
	ServiceClasses    ServiceClassesConfig        `json:"service_classes"`
	AutoTuning        AutoTuningConfig            `json:"auto_tuning"`
	OpenTelemetry     OpenTelemetryConfig         `json:"opentelemetry"`
	WarmModels        WarmModelsConfig            `json:"warm_models"`
	Alerts            AlertsConfig                `json:"alerts"`
	Conversations     ConversationsConfig         `json:"conversations"`
	BatchJobs         BatchJobsConfig             `json:"batch_jobs"`
	ModelAliases      map[string]ModelAliasConfig `json:"model_aliases"`
	ModelAccess       ModelAccessConfig           `json:"model_access"`
	Storage           StorageConfig               `json:"storage"`
}

type ServerConfig struct {
	Listen             string   `json:"listen"`
	ReadHeaderTimeout  Duration `json:"read_header_timeout"`
	IdleTimeout        Duration `json:"idle_timeout"`
	MaxRequestDuration Duration `json:"max_request_duration"`
	MaxBodyBytes       int64    `json:"max_body_bytes"`
	MetricsPublic      bool     `json:"metrics_public"`
	TLSCert            string   `json:"tls_cert"`
	TLSKey             string   `json:"tls_key"`
}

type AuthConfig struct {
	OIDC                   OIDCConfig       `json:"oidc"`
	APIKeys                []APIKeyConfig   `json:"api_keys"`
	IPBypass               []IPBypassConfig `json:"ip_bypass"`
	TrustedProxies         []string         `json:"trusted_proxies"`
	IPBypassUseForwardedIP bool             `json:"ip_bypass_use_forwarded_ip,omitempty"`
}

type OIDCConfig struct {
	Enabled                bool     `json:"enabled"`
	Issuer                 string   `json:"issuer"`
	Audience               string   `json:"audience"`
	TenantClaim            string   `json:"tenant_claim"`
	ApplicationClaim       string   `json:"application_claim"`
	GroupsClaim            string   `json:"groups_claim"`
	AdminGroups            []string `json:"admin_groups"`
	ClockSkew              Duration `json:"clock_skew"`
	JWKSRefreshMinInterval Duration `json:"jwks_refresh_min_interval"`
	AllowedAlgorithms      []string `json:"allowed_algorithms"`
}

type APIKeyConfig struct {
	Name          string   `json:"name"`
	Key           string   `json:"key"`
	Tenant        string   `json:"tenant"`
	Subject       string   `json:"subject"`
	Application   string   `json:"application"`
	Scopes        []string `json:"scopes"`
	AllowedModels []string `json:"allowed_models,omitempty"`
	DeniedModels  []string `json:"denied_models,omitempty"`
	ServiceClass  string   `json:"service_class,omitempty"`
}

type IPBypassConfig struct {
	CIDRs       []string `json:"cidrs"`
	Tenant      string   `json:"tenant"`
	Subject     string   `json:"subject"`
	Application string   `json:"application"`
	Scopes      []string `json:"scopes"`
}

type SchedulerConfig struct {
	GlobalConcurrency   int                     `json:"global_concurrency"`
	MaxQueue            int                     `json:"max_queue"`
	MaxQueuePerActor    int                     `json:"max_queue_per_actor"`
	QueueTimeout        Duration                `json:"queue_timeout"`
	DefaultTenantWeight float64                 `json:"default_tenant_weight"`
	DefaultActorWeight  float64                 `json:"default_actor_weight"`
	Policies            map[string]TenantPolicy `json:"policies"`
	ComputePaths        []string                `json:"compute_paths"`
}

type TenantPolicy struct {
	TenantWeight           float64 `json:"tenant_weight"`
	ActorWeight            float64 `json:"actor_weight"`
	ActorCreditsPerMinute  float64 `json:"actor_credits_per_minute"`
	ActorBurstCredits      float64 `json:"actor_burst_credits"`
	TenantCreditsPerMinute float64 `json:"tenant_credits_per_minute"`
	TenantBurstCredits     float64 `json:"tenant_burst_credits"`
}

type QuotaConfig struct {
	Enabled bool `json:"enabled"`
}

type CostConfig struct {
	Default                ModelRate            `json:"default"`
	Models                 map[string]ModelRate `json:"models"`
	DefaultMaxOutputTokens int                  `json:"default_max_output_tokens"`
}

type ModelRate struct {
	InputCreditsPer1K             float64 `json:"input_credits_per_1k"`
	CachedInputFactor             float64 `json:"cached_input_factor"`
	OutputCreditsPer1K            float64 `json:"output_credits_per_1k"`
	ComputeCreditsPerSecond       float64 `json:"compute_credits_per_second"`
	ExpectedPromptTokensPerSecond float64 `json:"expected_prompt_tokens_per_second"`
	ExpectedOutputTokensPerSecond float64 `json:"expected_output_tokens_per_second"`
}

type WorkerConfig struct {
	Name                 string             `json:"name"`
	URL                  string             `json:"url"`
	MaxConcurrent        int                `json:"max_concurrent"`
	ModelConcurrency     map[string]int     `json:"model_concurrency,omitempty"`
	ContextLimits        map[string]int64   `json:"context_limits,omitempty"`
	DefaultContextTokens int64              `json:"default_context_tokens,omitempty"`
	ModelPlacement       ModelPlacementRule `json:"model_placement,omitempty"`
	HealthInterval       Duration           `json:"health_interval"`
	MemoryCapacityBytes  int64              `json:"memory_capacity_bytes,omitempty"`
	VRAMCapacityBytes    int64              `json:"vram_capacity_bytes,omitempty"`
	LocalSystemStats     bool               `json:"local_system_stats,omitempty"`
	TelemetryURL         string             `json:"telemetry_url,omitempty"`
	NVIDIASMI            bool               `json:"nvidia_smi,omitempty"`
	NVIDIAGPU            string             `json:"nvidia_gpu,omitempty"`
	Labels               map[string]string  `json:"labels,omitempty"`
}

// ModelPlacementRule is a hard routing constraint applied before adaptive
// worker scoring. "allow_all" permits every model unless a deny rule matches;
// "whitelist" permits only models matched by allowed_models. Exact matches
// are more specific than prefix rules ("gemma4:*") and therefore can be used
// as exceptions to broader rules. At equal specificity, deny wins.
type ModelPlacementRule struct {
	Mode          string   `json:"mode,omitempty"` // allow_all | whitelist
	AllowedModels []string `json:"allowed_models,omitempty"`
	DeniedModels  []string `json:"denied_models,omitempty"`
}

type UsageConfig struct {
	JournalDir    string               `json:"journal_dir"`
	Buffer        int                  `json:"buffer"`
	FlushInterval Duration             `json:"flush_interval"`
	Retention     UsageRetentionConfig `json:"retention"`
}

type UsageRetentionConfig struct {
	DetailDays         int      `json:"detail_days"`
	DailyDays          int      `json:"daily_days"`
	MonthlyMonths      int      `json:"monthly_months"`
	CompactionInterval Duration `json:"compaction_interval"`
}

type NativeConfig struct {
	ManagementRequiresAdmin bool   `json:"management_requires_admin"`
	ControlWorker           string `json:"control_worker"`
}

type UIConfig struct {
	Enabled       bool         `json:"enabled"`
	Path          string       `json:"path"`
	Title         string       `json:"title"`
	RecentEvents  int          `json:"recent_events"`
	SessionSecret string       `json:"session_secret"`
	SecureCookies bool         `json:"secure_cookies"`
	OIDC          UIOIDCConfig `json:"oidc"`
}

type UIOIDCConfig struct {
	Enabled      bool     `json:"enabled"`
	ClientID     string   `json:"client_id"`
	ClientSecret string   `json:"client_secret"`
	Scopes       []string `json:"scopes"`
	RedirectURL  string   `json:"redirect_url"`
}

// PublicDashboardConfig controls the optional unauthenticated, strictly
// read-only status dashboard. The public API is deliberately sanitized and
// never exposes tenant/actor/application identities, worker URLs, labels,
// error strings, API keys, policies, quotas or persistent-state paths.
type PublicDashboardConfig struct {
	Enabled             bool              `json:"enabled"`
	Path                string            `json:"path"`
	Title               string            `json:"title"`
	Subtitle            string            `json:"subtitle"`
	RefreshInterval     Duration          `json:"refresh_interval"`
	MaxLiveRequests     int               `json:"max_live_requests"`
	ShowWorkerNames     bool              `json:"show_worker_names"`
	ShowModelNames      bool              `json:"show_model_names"`
	ShowResourceMetrics bool              `json:"show_resource_metrics"`
	WorkerDisplayNames  map[string]string `json:"worker_display_names,omitempty"`
}

type InfrastructureConfig struct {
	NodeID          string   `json:"node_id"`
	NodeName        string   `json:"node_name"`
	RefreshInterval Duration `json:"refresh_interval"`
	MaxRequests     int      `json:"max_requests"`
}

// ModelCapabilitiesConfig controls model metadata discovery through Ollama
// /api/show and request preflight. Unknown capability metadata is allowed by
// default so a temporarily unavailable metadata call never takes inference
// offline.
type ModelCapabilitiesConfig struct {
	Mode         string              `json:"mode"` // enforce | observe | off
	CacheTTL     Duration            `json:"cache_ttl"`
	ContextGuard string              `json:"context_guard"` // reject | warn | off
	Context      ContextPolicyConfig `json:"context"`
}

// ContextPolicyConfig controls how the gateway turns a model's theoretical
// context window into a safe, routable effective context window. The defaults
// intentionally favor predictable memory use on mixed/local Ollama fleets.
// Set max_requested_tokens to -1 only when an operator explicitly wants to
// remove the gateway-side cap. Set default_worker_tokens to -1 only when the
// gateway should fall back to the model maximum for unloaded models without a
// Modelfile num_ctx.
type ContextPolicyConfig struct {
	MaxRequestedTokens          int64   `json:"max_requested_tokens"`
	DefaultWorkerTokens         int64   `json:"default_worker_tokens"`
	EstimationMarginPercent     float64 `json:"estimation_margin_percent"`
	VisionReserveTokensPerImage int64   `json:"vision_reserve_tokens_per_image"`
}

// RoutingConfig tunes the local worker scoring function. Scores are relative;
// lower is better. Throughput is learned from completed requests in memory.

// ReliabilityConfig controls worker circuit breaking and safe pre-stream retries.
// Retries are only attempted when the upstream request failed before any
// response headers/body were committed to the client.
type ReliabilityConfig struct {
	Enabled          bool     `json:"enabled"`
	FailureThreshold int      `json:"failure_threshold"`
	OpenDuration     Duration `json:"open_duration"`
	RetryAttempts    int      `json:"retry_attempts"`
	RetryBackoff     Duration `json:"retry_backoff"`
}

// ModelAliasConfig exposes a stable virtual model name to clients and resolves
// it to the first currently routable real model in Models.
type ModelAliasConfig struct {
	Models               []string `json:"models"`
	RequiredCapabilities []string `json:"required_capabilities,omitempty"`
	Visible              *bool    `json:"visible,omitempty"`
}

// ModelAccessRule controls which models an identity may request. Exact names
// and one trailing '*' wildcard are supported. At equal specificity deny wins.
type ModelAccessRule struct {
	Mode          string   `json:"mode,omitempty"` // allow_all | whitelist
	AllowedModels []string `json:"allowed_models,omitempty"`
	DeniedModels  []string `json:"denied_models,omitempty"`
}

type ModelAccessConfig struct {
	Default ModelAccessRule            `json:"default"`
	Tenants map[string]ModelAccessRule `json:"tenants,omitempty"`
}

// ServiceClassConfig provides a request-level QoS hint inside the tenant fairness boundary.
type ServiceClassConfig struct {
	Weight        float64  `json:"weight"`
	MaxQueueWait  Duration `json:"max_queue_wait"`
	MaxConcurrent int      `json:"max_concurrent"`
}

type ServiceClassesConfig struct {
	Default       string                        `json:"default"`
	Header        string                        `json:"header"`
	OverrideScope string                        `json:"override_scope"`
	Classes       map[string]ServiceClassConfig `json:"classes"`
}

// AutoTuningConfig controls the explicit admin-triggered benchmark workflow.
// Auto tuning never changes production concurrency unless Apply is called by an admin.
type AutoTuningConfig struct {
	Enabled          bool     `json:"enabled"`
	MaxConcurrency   int      `json:"max_concurrency"`
	SamplesPerLevel  int      `json:"samples_per_level"`
	MaxTokens        int      `json:"max_tokens"`
	Timeout          Duration `json:"timeout"`
	Prompt           string   `json:"prompt"`
	TTFTWeight       float64  `json:"ttft_weight"`
	ThroughputWeight float64  `json:"throughput_weight"`
}

type BatchJobsConfig struct {
	Enabled       bool     `json:"enabled"`
	Retention     Duration `json:"retention"`
	MaxJobs       int      `json:"max_jobs"`
	MaxConcurrent int      `json:"max_concurrent"`
	MaxInputBytes int64    `json:"max_input_bytes"`
}

type ConversationsConfig struct {
	Enabled         bool     `json:"enabled"`
	EncryptionKey   string   `json:"encryption_key,omitempty"`
	Retention       Duration `json:"retention"`
	MaxEntries      int      `json:"max_entries"`
	MaxContentBytes int64    `json:"max_content_bytes"`
}

type OpenTelemetryConfig struct {
	Enabled        bool              `json:"enabled"`
	Endpoint       string            `json:"endpoint"`
	Headers        map[string]string `json:"headers,omitempty"`
	ServiceName    string            `json:"service_name"`
	ServiceVersion string            `json:"service_version,omitempty"`
	SampleRatio    float64           `json:"sample_ratio"`
	BatchSize      int               `json:"batch_size"`
	FlushInterval  Duration          `json:"flush_interval"`
	CaptureContent bool              `json:"capture_content"`
}

// WarmModelsConfig controls proactive model residency and idle unloading.
// Policies use exact model names or a single trailing '*' prefix wildcard.
type WarmModelsConfig struct {
	Enabled           bool                       `json:"enabled"`
	ReconcileInterval Duration                   `json:"reconcile_interval"`
	OperationTimeout  Duration                   `json:"operation_timeout"`
	Policies          map[string]WarmModelPolicy `json:"policies"`
}

type WarmModelPolicy struct {
	Class       string   `json:"class"` // hot | warm | cold
	Workers     []string `json:"workers,omitempty"`
	Replicas    int      `json:"replicas,omitempty"`
	Preload     bool     `json:"preload,omitempty"`
	IdleTimeout Duration `json:"idle_timeout,omitempty"`
}

// AlertsConfig evaluates bounded operational conditions and can deliver a
// signed generic webhook. Payloads never include prompts or model output.
type AlertsConfig struct {
	Enabled              bool            `json:"enabled"`
	EvaluationInterval   Duration        `json:"evaluation_interval"`
	Cooldown             Duration        `json:"cooldown"`
	HistoryLimit         int             `json:"history_limit"`
	WebhookTimeout       Duration        `json:"webhook_timeout"`
	WebhookMaxConcurrent int             `json:"webhook_max_concurrent"`
	WebhookQueue         int             `json:"webhook_queue"`
	WebhookRetryAttempts int             `json:"webhook_retry_attempts"`
	WebhookRetryBackoff  Duration        `json:"webhook_retry_backoff"`
	Webhooks             []WebhookConfig `json:"webhooks"`
	Thresholds           AlertThresholds `json:"thresholds"`
}

type WebhookConfig struct {
	Name    string `json:"name"`
	URL     string `json:"url"`
	Secret  string `json:"secret,omitempty"`
	Enabled bool   `json:"enabled"`
}

type AlertThresholds struct {
	WorkerDownFor     Duration `json:"worker_down_for"`
	CircuitOpen       bool     `json:"circuit_open"`
	QueueDepth        int      `json:"queue_depth"`
	QueueWait         Duration `json:"queue_wait"`
	VRAMPercent       float64  `json:"vram_percent"`
	StorageBytes      int64    `json:"storage_bytes"`
	QuotaRemainingPct float64  `json:"quota_remaining_percent"`
	OOM               bool     `json:"oom"`
}

// StorageConfig defines the local durable state directory. The gateway keeps
// active scheduling state in memory; only restart-worthy state is persisted.
type StorageConfig struct {
	DataDir               string   `json:"data_dir"`
	ConfigFile            string   `json:"config_file"`
	APIKeysFile           string   `json:"api_keys_file"`
	PoliciesFile          string   `json:"policies_file"`
	MetricsFile           string   `json:"metrics_file"`
	QuotaFile             string   `json:"quota_file"`
	WorkerPerformanceFile string   `json:"worker_performance_file"`
	ModelPlacementFile    string   `json:"model_placement_file"`
	WorkerStateFile       string   `json:"worker_state_file"`
	AutoTuneFile          string   `json:"auto_tune_file"`
	WarmModelsFile        string   `json:"warm_models_file"`
	AlertsFile            string   `json:"alerts_file"`
	ConversationsFile     string   `json:"conversations_file"`
	BatchJobsFile         string   `json:"batch_jobs_file"`
	BatchJobsDir          string   `json:"batch_jobs_dir"`
	FlushInterval         Duration `json:"flush_interval"`
}

type RoutingConfig struct {
	LoadedBonus           float64 `json:"loaded_bonus"`
	InstalledBonus        float64 `json:"installed_bonus"`
	ThroughputBonus       float64 `json:"throughput_bonus"`
	VRAMPressurePenalty   float64 `json:"vram_pressure_penalty"`
	GPUUtilizationPenalty float64 `json:"gpu_utilization_penalty"`
	AvoidVRAMPercent      float64 `json:"avoid_vram_percent"`
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParseBytes(b)
}

// ParseBytes parses a complete gateway configuration using the same strict
// validation used at process startup. Environment variables are expanded so
// persisted configurations can continue to reference deployment secrets.
func ParseBytes(b []byte) (*Config, error) {
	b = []byte(os.ExpandEnv(string(b)))
	var c Config
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	c.defaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) defaults() {
	if c.Server.Listen == "" {
		c.Server.Listen = ":8080"
	}
	if c.Server.ReadHeaderTimeout == 0 {
		c.Server.ReadHeaderTimeout = Duration(10 * time.Second)
	}
	if c.Server.IdleTimeout == 0 {
		c.Server.IdleTimeout = Duration(2 * time.Minute)
	}
	if c.Server.MaxRequestDuration == 0 {
		c.Server.MaxRequestDuration = Duration(30 * time.Minute)
	}
	if c.Server.MaxBodyBytes == 0 {
		c.Server.MaxBodyBytes = 64 << 20
	}
	if c.Scheduler.GlobalConcurrency <= 0 {
		for _, w := range c.Workers {
			c.Scheduler.GlobalConcurrency += max(1, w.MaxConcurrent)
		}
		if c.Scheduler.GlobalConcurrency == 0 {
			c.Scheduler.GlobalConcurrency = 1
		}
	}
	if c.Scheduler.MaxQueue <= 0 {
		c.Scheduler.MaxQueue = 1024
	}
	if c.Scheduler.MaxQueuePerActor <= 0 {
		c.Scheduler.MaxQueuePerActor = 64
	}
	if c.Scheduler.QueueTimeout == 0 {
		c.Scheduler.QueueTimeout = Duration(10 * time.Minute)
	}
	if c.Scheduler.DefaultTenantWeight <= 0 {
		c.Scheduler.DefaultTenantWeight = 1
	}
	if c.Scheduler.DefaultActorWeight <= 0 {
		c.Scheduler.DefaultActorWeight = 1
	}
	if len(c.Scheduler.ComputePaths) == 0 {
		c.Scheduler.ComputePaths = []string{"/api/generate", "/api/chat", "/api/embed", "/api/embeddings", "/v1/chat/completions", "/v1/completions", "/v1/embeddings", "/v1/responses", "/v1/messages"}
	}
	if c.Cost.Default.InputCreditsPer1K <= 0 {
		c.Cost.Default.InputCreditsPer1K = 1
	}
	if c.Cost.Default.OutputCreditsPer1K <= 0 {
		c.Cost.Default.OutputCreditsPer1K = 3
	}
	if c.Cost.Default.CachedInputFactor <= 0 {
		c.Cost.Default.CachedInputFactor = 1
	}
	if c.Cost.DefaultMaxOutputTokens <= 0 {
		c.Cost.DefaultMaxOutputTokens = 1024
	}
	if c.Storage.DataDir == "" {
		c.Storage.DataDir = "./data"
	}
	if c.Storage.ConfigFile == "" {
		c.Storage.ConfigFile = "gateway-config.json"
	}
	if c.Storage.APIKeysFile == "" {
		c.Storage.APIKeysFile = "api-keys.json"
	}
	if c.Storage.PoliciesFile == "" {
		c.Storage.PoliciesFile = "policies.json"
	}
	if c.Storage.MetricsFile == "" {
		c.Storage.MetricsFile = "metrics.json"
	}
	if c.Storage.QuotaFile == "" {
		c.Storage.QuotaFile = "quota.json"
	}
	if c.Storage.WorkerPerformanceFile == "" {
		c.Storage.WorkerPerformanceFile = "worker-performance.json"
	}
	if c.Storage.ModelPlacementFile == "" {
		c.Storage.ModelPlacementFile = "model-placement.json"
	}
	if c.Storage.WorkerStateFile == "" {
		c.Storage.WorkerStateFile = "worker-state.json"
	}
	if c.Storage.AutoTuneFile == "" {
		c.Storage.AutoTuneFile = "auto-tune.json"
	}
	if c.Storage.WarmModelsFile == "" {
		c.Storage.WarmModelsFile = "warm-models.json"
	}
	if c.Storage.AlertsFile == "" {
		c.Storage.AlertsFile = "alerts.json"
	}
	if c.Storage.ConversationsFile == "" {
		c.Storage.ConversationsFile = "conversations.enc.json"
	}
	if c.Storage.BatchJobsFile == "" {
		c.Storage.BatchJobsFile = "batch-jobs.json"
	}
	if c.Storage.BatchJobsDir == "" {
		c.Storage.BatchJobsDir = "batch"
	}
	if c.Storage.FlushInterval == 0 {
		c.Storage.FlushInterval = Duration(10 * time.Second)
	}
	if c.Usage.JournalDir == "" {
		c.Usage.JournalDir = filepath.Join(c.Storage.DataDir, "usage")
	}
	if c.Usage.Buffer <= 0 {
		c.Usage.Buffer = 8192
	}
	if c.Usage.FlushInterval == 0 {
		c.Usage.FlushInterval = Duration(time.Second)
	}
	if c.Usage.Retention.DetailDays <= 0 {
		c.Usage.Retention.DetailDays = 30
	}
	if c.Usage.Retention.DailyDays <= 0 {
		c.Usage.Retention.DailyDays = 400
	}
	if c.Usage.Retention.CompactionInterval == 0 {
		c.Usage.Retention.CompactionInterval = Duration(6 * time.Hour)
	}
	if c.Auth.OIDC.ClockSkew == 0 {
		c.Auth.OIDC.ClockSkew = Duration(60 * time.Second)
	}
	if c.Auth.OIDC.JWKSRefreshMinInterval == 0 {
		c.Auth.OIDC.JWKSRefreshMinInterval = Duration(10 * time.Second)
	}
	if c.UI.Path == "" {
		c.UI.Path = "/admin"
	}
	if !strings.HasPrefix(c.UI.Path, "/") {
		c.UI.Path = "/" + c.UI.Path
	}
	c.UI.Path = strings.TrimRight(c.UI.Path, "/")
	if c.UI.Title == "" {
		c.UI.Title = "Ollama Fair Gateway"
	}
	if c.UI.RecentEvents <= 0 {
		c.UI.RecentEvents = 10000
	}
	if c.UI.OIDC.Enabled && len(c.UI.OIDC.Scopes) == 0 {
		c.UI.OIDC.Scopes = []string{"openid", "profile", "email"}
	}
	if c.PublicDashboard.Path == "" {
		c.PublicDashboard.Path = "/status"
	}
	if !strings.HasPrefix(c.PublicDashboard.Path, "/") {
		c.PublicDashboard.Path = "/" + c.PublicDashboard.Path
	}
	c.PublicDashboard.Path = strings.TrimRight(c.PublicDashboard.Path, "/")
	if c.PublicDashboard.Title == "" {
		c.PublicDashboard.Title = c.UI.Title
		if c.PublicDashboard.Title == "" {
			c.PublicDashboard.Title = "Ollama Gateway Status"
		}
	}
	if c.PublicDashboard.Subtitle == "" {
		c.PublicDashboard.Subtitle = "Live-Auslastung und Infrastruktur"
	}
	if c.PublicDashboard.RefreshInterval == 0 {
		c.PublicDashboard.RefreshInterval = Duration(2 * time.Second)
	}
	if c.PublicDashboard.MaxLiveRequests <= 0 {
		c.PublicDashboard.MaxLiveRequests = 64
	}
	if c.Infrastructure.RefreshInterval == 0 {
		c.Infrastructure.RefreshInterval = Duration(250 * time.Millisecond)
	}
	if c.Infrastructure.MaxRequests <= 0 {
		c.Infrastructure.MaxRequests = 256
	}
	if len(c.Auth.OIDC.AllowedAlgorithms) == 0 {
		c.Auth.OIDC.AllowedAlgorithms = []string{"RS256", "PS256", "ES256", "EdDSA"}
	}
	if c.ModelCapabilities.Mode == "" {
		c.ModelCapabilities.Mode = "enforce"
	}
	if c.ModelCapabilities.CacheTTL == 0 {
		c.ModelCapabilities.CacheTTL = Duration(10 * time.Minute)
	}
	if c.ModelCapabilities.ContextGuard == "" {
		c.ModelCapabilities.ContextGuard = "reject"
	}
	if c.ModelCapabilities.Context.MaxRequestedTokens == 0 {
		c.ModelCapabilities.Context.MaxRequestedTokens = 32768
	}
	if c.ModelCapabilities.Context.DefaultWorkerTokens == 0 {
		c.ModelCapabilities.Context.DefaultWorkerTokens = 4096
	}
	if c.ModelCapabilities.Context.EstimationMarginPercent == 0 {
		c.ModelCapabilities.Context.EstimationMarginPercent = 15
	}
	if c.ModelCapabilities.Context.VisionReserveTokensPerImage == 0 {
		c.ModelCapabilities.Context.VisionReserveTokensPerImage = 2048
	}
	if c.Routing.LoadedBonus == 0 {
		c.Routing.LoadedBonus = 60
	}
	if c.Routing.InstalledBonus == 0 {
		c.Routing.InstalledBonus = 30
	}
	if c.Routing.ThroughputBonus == 0 {
		c.Routing.ThroughputBonus = 20
	}
	if c.Routing.VRAMPressurePenalty == 0 {
		c.Routing.VRAMPressurePenalty = 35
	}
	if c.Routing.GPUUtilizationPenalty == 0 {
		c.Routing.GPUUtilizationPenalty = 10
	}
	if c.Routing.AvoidVRAMPercent == 0 {
		c.Routing.AvoidVRAMPercent = 97
	}
	if c.Reliability.FailureThreshold <= 0 {
		c.Reliability.FailureThreshold = 3
	}
	if c.Reliability.OpenDuration == 0 {
		c.Reliability.OpenDuration = Duration(30 * time.Second)
	}
	if c.Reliability.RetryAttempts <= 0 {
		c.Reliability.RetryAttempts = 2
	}
	if c.Reliability.RetryBackoff == 0 {
		c.Reliability.RetryBackoff = Duration(50 * time.Millisecond)
	}
	if c.ServiceClasses.Default == "" {
		c.ServiceClasses.Default = "interactive"
	}
	if c.ServiceClasses.Header == "" {
		c.ServiceClasses.Header = "X-Gateway-Service-Class"
	}
	if c.ServiceClasses.OverrideScope == "" {
		c.ServiceClasses.OverrideScope = "gateway:service-class"
	}
	if len(c.ServiceClasses.Classes) == 0 {
		c.ServiceClasses.Classes = map[string]ServiceClassConfig{
			"interactive": {Weight: 4, MaxQueueWait: Duration(30 * time.Second)},
			"system":      {Weight: 8, MaxQueueWait: Duration(30 * time.Second), MaxConcurrent: 1},
			"background":  {Weight: 1, MaxQueueWait: Duration(10 * time.Minute), MaxConcurrent: 1},
			"batch":       {Weight: .5, MaxQueueWait: Duration(30 * time.Minute), MaxConcurrent: 1},
		}
	}
	for name, sc := range c.ServiceClasses.Classes {
		if sc.Weight <= 0 {
			sc.Weight = 1
		}
		if sc.MaxQueueWait == 0 {
			sc.MaxQueueWait = c.Scheduler.QueueTimeout
		}
		c.ServiceClasses.Classes[name] = sc
	}
	if c.AutoTuning.MaxConcurrency <= 0 {
		c.AutoTuning.MaxConcurrency = 4
	}
	if c.AutoTuning.SamplesPerLevel <= 0 {
		c.AutoTuning.SamplesPerLevel = 2
	}
	if c.AutoTuning.MaxTokens <= 0 {
		c.AutoTuning.MaxTokens = 96
	}
	if c.AutoTuning.Timeout == 0 {
		c.AutoTuning.Timeout = Duration(10 * time.Minute)
	}
	if c.AutoTuning.Prompt == "" {
		c.AutoTuning.Prompt = "Write a concise explanation of why deterministic benchmarking matters for local LLM serving."
	}
	if c.AutoTuning.TTFTWeight <= 0 {
		c.AutoTuning.TTFTWeight = .25
	}
	if c.AutoTuning.ThroughputWeight <= 0 {
		c.AutoTuning.ThroughputWeight = 1
	}
	if c.OpenTelemetry.ServiceName == "" {
		c.OpenTelemetry.ServiceName = "ollama-fair-gateway"
	}
	if c.OpenTelemetry.SampleRatio <= 0 || c.OpenTelemetry.SampleRatio > 1 {
		c.OpenTelemetry.SampleRatio = 1
	}
	if c.OpenTelemetry.BatchSize <= 0 {
		c.OpenTelemetry.BatchSize = 128
	}
	if c.OpenTelemetry.FlushInterval == 0 {
		c.OpenTelemetry.FlushInterval = Duration(2 * time.Second)
	}
	if c.WarmModels.ReconcileInterval == 0 {
		c.WarmModels.ReconcileInterval = Duration(30 * time.Second)
	}
	if c.WarmModels.OperationTimeout == 0 {
		c.WarmModels.OperationTimeout = Duration(2 * time.Minute)
	}
	for pattern, wp := range c.WarmModels.Policies {
		if wp.Class == "" {
			wp.Class = "warm"
		}
		if wp.Replicas <= 0 {
			wp.Replicas = 1
		}
		if wp.IdleTimeout == 0 {
			if wp.Class == "cold" {
				wp.IdleTimeout = Duration(5 * time.Minute)
			} else {
				wp.IdleTimeout = Duration(30 * time.Minute)
			}
		}
		c.WarmModels.Policies[pattern] = wp
	}
	if c.BatchJobs.Retention == 0 {
		c.BatchJobs.Retention = Duration(7 * 24 * time.Hour)
	}
	if c.BatchJobs.MaxJobs <= 0 {
		c.BatchJobs.MaxJobs = 1000
	}
	if c.BatchJobs.MaxConcurrent <= 0 {
		c.BatchJobs.MaxConcurrent = 1
	}
	if c.BatchJobs.MaxInputBytes <= 0 {
		c.BatchJobs.MaxInputBytes = 16 << 20
	}
	if c.Conversations.Retention == 0 {
		c.Conversations.Retention = Duration(24 * time.Hour)
	}
	if c.Conversations.MaxEntries <= 0 {
		c.Conversations.MaxEntries = 1000
	}
	if c.Conversations.MaxContentBytes <= 0 {
		c.Conversations.MaxContentBytes = 2 << 20
	}
	if c.Alerts.EvaluationInterval == 0 {
		c.Alerts.EvaluationInterval = Duration(15 * time.Second)
	}
	if c.Alerts.Cooldown == 0 {
		c.Alerts.Cooldown = Duration(5 * time.Minute)
	}
	if c.Alerts.HistoryLimit <= 0 {
		c.Alerts.HistoryLimit = 500
	}
	if c.Alerts.WebhookTimeout == 0 {
		c.Alerts.WebhookTimeout = Duration(5 * time.Second)
	}
	if c.Alerts.WebhookMaxConcurrent <= 0 {
		c.Alerts.WebhookMaxConcurrent = 4
	}
	if c.Alerts.WebhookQueue <= 0 {
		c.Alerts.WebhookQueue = 1024
	}
	if c.Alerts.WebhookRetryAttempts <= 0 {
		c.Alerts.WebhookRetryAttempts = 3
	}
	if c.Alerts.WebhookRetryBackoff == 0 {
		c.Alerts.WebhookRetryBackoff = Duration(500 * time.Millisecond)
	}
	if c.Alerts.Thresholds.WorkerDownFor == 0 {
		c.Alerts.Thresholds.WorkerDownFor = Duration(30 * time.Second)
	}
	if c.Alerts.Thresholds.VRAMPercent == 0 {
		c.Alerts.Thresholds.VRAMPercent = 95
	}
	if c.Alerts.Thresholds.QuotaRemainingPct == 0 {
		c.Alerts.Thresholds.QuotaRemainingPct = 10
	}
	if c.ModelAccess.Default.Mode == "" {
		c.ModelAccess.Default.Mode = "allow_all"
	}
	for i := range c.Workers {
		if c.Workers[i].Name == "" {
			c.Workers[i].Name = fmt.Sprintf("worker-%d", i+1)
		}
		if c.Workers[i].MaxConcurrent <= 0 {
			c.Workers[i].MaxConcurrent = 1
		}
		if c.Workers[i].HealthInterval == 0 {
			c.Workers[i].HealthInterval = Duration(5 * time.Second)
		}
		if c.Workers[i].ModelPlacement.Mode == "" {
			c.Workers[i].ModelPlacement.Mode = "allow_all"
		}
	}
}

func pathPrefixesOverlap(a, b string) bool {
	a = strings.TrimRight(a, "/")
	b = strings.TrimRight(b, "/")
	if a == "" || b == "" {
		return false
	}
	return a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}

func (c *Config) Validate() error {
	if len(c.Workers) == 0 {
		return errors.New("at least one worker is required")
	}
	names := map[string]bool{}
	for _, w := range c.Workers {
		if w.URL == "" {
			return fmt.Errorf("worker %q has empty url", w.Name)
		}
		if names[w.Name] {
			return fmt.Errorf("duplicate worker name %q", w.Name)
		}
		names[w.Name] = true
	}
	if c.Auth.OIDC.Enabled && (c.Auth.OIDC.Issuer == "" || c.Auth.OIDC.Audience == "") {
		return errors.New("auth.oidc.issuer and auth.oidc.audience are required when OIDC is enabled")
	}
	if !c.Auth.OIDC.Enabled && len(c.Auth.APIKeys) == 0 && len(c.Auth.IPBypass) == 0 {
		return errors.New("no authentication method configured")
	}
	for _, p := range c.Auth.TrustedProxies {
		if _, _, err := net.ParseCIDR(p); err != nil {
			return fmt.Errorf("invalid trusted proxy CIDR %q: %w", p, err)
		}
	}
	for _, b := range c.Auth.IPBypass {
		if b.Tenant == "" {
			return errors.New("ip_bypass tenant must not be empty")
		}
		for _, s := range b.CIDRs {
			if _, _, err := net.ParseCIDR(s); err != nil {
				return fmt.Errorf("invalid bypass CIDR %q: %w", s, err)
			}
		}
	}
	if c.Infrastructure.RefreshInterval.Value() < 50*time.Millisecond {
		return errors.New("infrastructure.refresh_interval must be at least 50ms")
	}
	if c.PublicDashboard.Path == "" || c.PublicDashboard.Path == "/" {
		return errors.New("public_dashboard.path must be a non-root path")
	}
	if c.PublicDashboard.Enabled {
		if pathPrefixesOverlap(c.PublicDashboard.Path, c.UI.Path) {
			return errors.New("public_dashboard.path must not overlap ui.path")
		}
		for _, reserved := range []string{"/healthz", "/readyz", "/metrics", "/api", "/v1", "/gateway"} {
			if pathPrefixesOverlap(c.PublicDashboard.Path, reserved) {
				return errors.New("public_dashboard.path conflicts with a reserved gateway path")
			}
		}
		if c.PublicDashboard.RefreshInterval.Value() < time.Second {
			return errors.New("public_dashboard.refresh_interval must be at least 1s")
		}
		if c.PublicDashboard.MaxLiveRequests < 1 || c.PublicDashboard.MaxLiveRequests > 512 {
			return errors.New("public_dashboard.max_live_requests must be between 1 and 512")
		}
	}
	switch c.ModelCapabilities.Mode {
	case "enforce", "observe", "off":
	default:
		return errors.New("model_capabilities.mode must be enforce, observe, or off")
	}
	switch c.ModelCapabilities.ContextGuard {
	case "reject", "warn", "off":
	default:
		return errors.New("model_capabilities.context_guard must be reject, warn, or off")
	}
	if c.ModelCapabilities.CacheTTL.Value() < time.Second {
		return errors.New("model_capabilities.cache_ttl must be at least 1s")
	}
	if c.ModelCapabilities.Context.MaxRequestedTokens < -1 {
		return errors.New("model_capabilities.context.max_requested_tokens must be -1 or >= 1")
	}
	if c.ModelCapabilities.Context.DefaultWorkerTokens < -1 {
		return errors.New("model_capabilities.context.default_worker_tokens must be -1 or >= 1")
	}
	if c.ModelCapabilities.Context.EstimationMarginPercent < 0 || c.ModelCapabilities.Context.EstimationMarginPercent > 100 {
		return errors.New("model_capabilities.context.estimation_margin_percent must be between 0 and 100")
	}
	if c.ModelCapabilities.Context.VisionReserveTokensPerImage < 0 {
		return errors.New("model_capabilities.context.vision_reserve_tokens_per_image must be >= 0")
	}
	for _, w := range c.Workers {
		if w.DefaultContextTokens < 0 {
			return fmt.Errorf("worker %s default_context_tokens must be >= 0", w.Name)
		}
		for pattern, limit := range w.ContextLimits {
			if strings.TrimSpace(pattern) == "" || limit <= 0 {
				return fmt.Errorf("worker %s context_limits entries require a non-empty pattern and positive token limit", w.Name)
			}
		}
	}
	if c.Routing.AvoidVRAMPercent < 0 || c.Routing.AvoidVRAMPercent > 100 {
		return errors.New("routing.avoid_vram_percent must be between 0 and 100")
	}
	if strings.TrimSpace(c.Storage.DataDir) == "" {
		return errors.New("storage.data_dir must not be empty")
	}
	for name, file := range map[string]string{
		"config_file": c.Storage.ConfigFile, "api_keys_file": c.Storage.APIKeysFile,
		"policies_file": c.Storage.PoliciesFile, "metrics_file": c.Storage.MetricsFile, "quota_file": c.Storage.QuotaFile,
		"worker_performance_file": c.Storage.WorkerPerformanceFile, "model_placement_file": c.Storage.ModelPlacementFile, "worker_state_file": c.Storage.WorkerStateFile, "auto_tune_file": c.Storage.AutoTuneFile, "warm_models_file": c.Storage.WarmModelsFile, "alerts_file": c.Storage.AlertsFile, "conversations_file": c.Storage.ConversationsFile, "batch_jobs_file": c.Storage.BatchJobsFile, "batch_jobs_dir": c.Storage.BatchJobsDir,
	} {
		if file == "" || filepath.IsAbs(file) || filepath.Base(file) != file || file == "." || file == ".." {
			return fmt.Errorf("storage.%s must be a simple relative filename", name)
		}
	}
	if c.Storage.FlushInterval.Value() < time.Second {
		return errors.New("storage.flush_interval must be at least 1s")
	}
	if c.Usage.Retention.DetailDays < 1 {
		return errors.New("usage.retention.detail_days must be at least 1")
	}
	if c.Usage.Retention.DailyDays < c.Usage.Retention.DetailDays {
		return errors.New("usage.retention.daily_days must be >= detail_days")
	}
	if c.Usage.Retention.MonthlyMonths < 0 {
		return errors.New("usage.retention.monthly_months must be >= 0 (0 keeps monthly rollups forever)")
	}
	if c.Usage.Retention.CompactionInterval.Value() < time.Minute {
		return errors.New("usage.retention.compaction_interval must be at least 1m")
	}
	if c.Reliability.FailureThreshold < 1 {
		return errors.New("reliability.failure_threshold must be >= 1")
	}
	if c.Reliability.OpenDuration.Value() < time.Second {
		return errors.New("reliability.open_duration must be at least 1s")
	}
	if c.Reliability.RetryAttempts < 1 || c.Reliability.RetryAttempts > 5 {
		return errors.New("reliability.retry_attempts must be between 1 and 5")
	}
	if c.Reliability.RetryBackoff.Value() < 0 {
		return errors.New("reliability.retry_backoff must be >= 0")
	}
	if err := ValidateModelAccessRule(c.ModelAccess.Default); err != nil {
		return fmt.Errorf("model_access.default: %w", err)
	}
	for tenant, rule := range c.ModelAccess.Tenants {
		if strings.TrimSpace(tenant) == "" {
			return errors.New("model_access.tenants contains empty tenant")
		}
		if err := ValidateModelAccessRule(rule); err != nil {
			return fmt.Errorf("model_access.tenants[%q]: %w", tenant, err)
		}
	}
	if _, ok := c.ServiceClasses.Classes[c.ServiceClasses.Default]; !ok {
		return fmt.Errorf("service_classes.default %q is not defined", c.ServiceClasses.Default)
	}
	for name, sc := range c.ServiceClasses.Classes {
		if strings.TrimSpace(name) == "" || sc.Weight <= 0 || sc.MaxConcurrent < 0 || sc.MaxQueueWait.Value() < 0 {
			return fmt.Errorf("invalid service class %q", name)
		}
	}
	for _, k := range c.Auth.APIKeys {
		if cls := strings.TrimSpace(k.ServiceClass); cls != "" {
			if _, ok := c.ServiceClasses.Classes[cls]; !ok {
				return fmt.Errorf("auth.api_keys[%q].service_class %q is not defined", k.Name, cls)
			}
		}
	}
	if c.AutoTuning.MaxConcurrency < 1 || c.AutoTuning.MaxConcurrency > 32 || c.AutoTuning.SamplesPerLevel < 1 || c.AutoTuning.SamplesPerLevel > 20 || c.AutoTuning.MaxTokens < 1 {
		return errors.New("auto_tuning limits are invalid")
	}
	if c.BatchJobs.Enabled {
		if c.BatchJobs.Retention.Value() < time.Minute {
			return errors.New("batch_jobs.retention must be at least 1m")
		}
		if c.BatchJobs.MaxJobs < 1 || c.BatchJobs.MaxJobs > 1000000 {
			return errors.New("batch_jobs.max_jobs must be between 1 and 1000000")
		}
		if c.BatchJobs.MaxConcurrent < 1 || c.BatchJobs.MaxConcurrent > 32 {
			return errors.New("batch_jobs.max_concurrent must be between 1 and 32")
		}
		if c.BatchJobs.MaxInputBytes < 1024 || c.BatchJobs.MaxInputBytes > 64<<20 {
			return errors.New("batch_jobs.max_input_bytes must be between 1KiB and 64MiB")
		}
		if c.Server.MaxBodyBytes > 0 && c.BatchJobs.MaxInputBytes > c.Server.MaxBodyBytes {
			return errors.New("batch_jobs.max_input_bytes must be <= server.max_body_bytes")
		}
		if _, ok := c.ServiceClasses.Classes["batch"]; len(c.ServiceClasses.Classes) > 0 && !ok {
			return errors.New("batch_jobs.enabled requires service_classes.classes.batch")
		}
	}
	if c.Conversations.Enabled {
		if len(c.Conversations.EncryptionKey) < 32 {
			return errors.New("conversations.encryption_key must contain at least 32 characters when conversations are enabled")
		}
		if c.Conversations.Retention.Value() < time.Minute {
			return errors.New("conversations.retention must be at least 1m")
		}
		if c.Conversations.MaxEntries < 1 || c.Conversations.MaxEntries > 1000000 {
			return errors.New("conversations.max_entries must be between 1 and 1000000")
		}
		if c.Conversations.MaxContentBytes < 1024 || c.Conversations.MaxContentBytes > 64<<20 {
			return errors.New("conversations.max_content_bytes must be between 1KiB and 64MiB")
		}
	}
	if c.OpenTelemetry.Enabled && strings.TrimSpace(c.OpenTelemetry.Endpoint) == "" {
		return errors.New("opentelemetry.endpoint is required when enabled")
	}
	if c.WarmModels.ReconcileInterval.Value() < time.Second {
		return errors.New("warm_models.reconcile_interval must be at least 1s")
	}
	if c.WarmModels.OperationTimeout.Value() < time.Second {
		return errors.New("warm_models.operation_timeout must be at least 1s")
	}
	for pattern, wp := range c.WarmModels.Policies {
		if err := validateSimpleModelPattern(pattern); err != nil {
			return fmt.Errorf("warm_models.policies[%q]: %w", pattern, err)
		}
		switch wp.Class {
		case "hot", "warm", "cold":
		default:
			return fmt.Errorf("warm_models.policies[%q].class must be hot, warm, or cold", pattern)
		}
		if wp.Replicas < 1 {
			return fmt.Errorf("warm_models.policies[%q].replicas must be >= 1", pattern)
		}
		if wp.IdleTimeout.Value() < 0 {
			return fmt.Errorf("warm_models.policies[%q].idle_timeout must be >= 0", pattern)
		}
		for _, wn := range wp.Workers {
			if !names[wn] {
				return fmt.Errorf("warm_models.policies[%q] references unknown worker %q", pattern, wn)
			}
		}
	}
	if c.Alerts.EvaluationInterval.Value() < time.Second {
		return errors.New("alerts.evaluation_interval must be at least 1s")
	}
	if c.Alerts.Cooldown.Value() < 0 {
		return errors.New("alerts.cooldown must be >= 0")
	}
	if c.Alerts.HistoryLimit < 1 || c.Alerts.HistoryLimit > 10000 {
		return errors.New("alerts.history_limit must be between 1 and 10000")
	}
	if c.Alerts.WebhookTimeout.Value() < time.Second || c.Alerts.WebhookTimeout.Value() > 2*time.Minute {
		return errors.New("alerts.webhook_timeout must be between 1s and 2m")
	}
	if c.Alerts.WebhookMaxConcurrent < 1 || c.Alerts.WebhookMaxConcurrent > 64 {
		return errors.New("alerts.webhook_max_concurrent must be between 1 and 64")
	}
	if c.Alerts.WebhookQueue < 1 || c.Alerts.WebhookQueue > 100000 {
		return errors.New("alerts.webhook_queue must be between 1 and 100000")
	}
	if c.Alerts.WebhookRetryAttempts < 1 || c.Alerts.WebhookRetryAttempts > 10 {
		return errors.New("alerts.webhook_retry_attempts must be between 1 and 10")
	}
	if c.Alerts.WebhookRetryBackoff.Value() < 0 || c.Alerts.WebhookRetryBackoff.Value() > time.Minute {
		return errors.New("alerts.webhook_retry_backoff must be between 0 and 1m")
	}
	if c.Alerts.Thresholds.QueueWait.Value() < 0 {
		return errors.New("alerts.thresholds.queue_wait must be >= 0")
	}
	if c.Alerts.Thresholds.VRAMPercent < 0 || c.Alerts.Thresholds.VRAMPercent > 100 {
		return errors.New("alerts.thresholds.vram_percent must be between 0 and 100")
	}
	if c.Alerts.Thresholds.QuotaRemainingPct < 0 || c.Alerts.Thresholds.QuotaRemainingPct > 100 {
		return errors.New("alerts.thresholds.quota_remaining_percent must be between 0 and 100")
	}
	for i, wh := range c.Alerts.Webhooks {
		if !wh.Enabled {
			continue
		}
		if strings.TrimSpace(wh.URL) == "" {
			return fmt.Errorf("alerts.webhooks[%d].url is required when enabled", i)
		}
		if !(strings.HasPrefix(wh.URL, "http://") || strings.HasPrefix(wh.URL, "https://")) {
			return fmt.Errorf("alerts.webhooks[%d].url must be http(s)", i)
		}
	}
	for alias, a := range c.ModelAliases {
		if strings.TrimSpace(alias) == "" {
			return errors.New("model_aliases contains empty alias")
		}
		if len(a.Models) == 0 {
			return fmt.Errorf("model_aliases[%q] requires at least one model", alias)
		}
		for _, m := range a.Models {
			if strings.TrimSpace(m) == "" {
				return fmt.Errorf("model_aliases[%q] contains empty model", alias)
			}
		}
	}
	for _, k := range c.Auth.APIKeys {
		if err := ValidateModelAccessRule(ModelAccessRule{Mode: "allow_all", AllowedModels: k.AllowedModels, DeniedModels: k.DeniedModels}); err != nil {
			return fmt.Errorf("api key %q model ACL: %w", k.Name, err)
		}
	}
	for _, w := range c.Workers {
		for pattern, limit := range w.ModelConcurrency {
			if strings.TrimSpace(pattern) == "" || limit <= 0 {
				return fmt.Errorf("worker %q model_concurrency entries require a non-empty pattern and limit > 0", w.Name)
			}
		}
		if err := ValidateModelPlacementRule(w.ModelPlacement); err != nil {
			return fmt.Errorf("worker %q model_placement: %w", w.Name, err)
		}
	}
	if c.UI.Enabled {
		if c.UI.Path == "" || c.UI.Path == "/" {
			return errors.New("ui.path must be a non-root path")
		}
		if c.UI.OIDC.Enabled {
			if !c.Auth.OIDC.Enabled {
				return errors.New("ui.oidc.enabled requires auth.oidc.enabled")
			}
			if c.UI.OIDC.ClientID == "" {
				return errors.New("ui.oidc.client_id is required when browser OIDC login is enabled")
			}
			if len(c.UI.SessionSecret) < 32 {
				return errors.New("ui.session_secret must contain at least 32 characters when browser OIDC login is enabled")
			}
		}
	}
	return nil
}

func ValidateWarmModelPolicies(policies map[string]WarmModelPolicy, workers map[string]bool) error {
	for pattern, wp := range policies {
		if err := validateSimpleModelPattern(pattern); err != nil {
			return fmt.Errorf("policy %q: %w", pattern, err)
		}
		switch wp.Class {
		case "hot", "warm", "cold":
		default:
			return fmt.Errorf("policy %q class must be hot, warm, or cold", pattern)
		}
		if wp.Replicas < 1 {
			return fmt.Errorf("policy %q replicas must be >= 1", pattern)
		}
		if wp.IdleTimeout.Value() < 0 {
			return fmt.Errorf("policy %q idle_timeout must be >= 0", pattern)
		}
		for _, wn := range wp.Workers {
			if len(workers) > 0 && !workers[wn] {
				return fmt.Errorf("policy %q references unknown worker %q", pattern, wn)
			}
		}
	}
	return nil
}

func validateSimpleModelPattern(p string) error {
	p = strings.TrimSpace(p)
	if p == "" {
		return errors.New("model pattern must not be empty")
	}
	if strings.Count(p, "*") > 1 || (strings.Contains(p, "*") && !strings.HasSuffix(p, "*")) {
		return errors.New("model pattern supports only one trailing * wildcard")
	}
	return nil
}

// ValidateModelPlacementRule validates the compact pattern syntax used by
// model placement. Patterns may be exact model names, "*", or a single
// trailing wildcard such as "gemma4:*". This deliberately matches the model
// pattern semantics already used by cost and per-model concurrency rules.
func ValidateModelPlacementRule(r ModelPlacementRule) error {
	mode := strings.TrimSpace(r.Mode)
	if mode == "" {
		mode = "allow_all"
	}
	if mode != "allow_all" && mode != "whitelist" {
		return errors.New("mode must be allow_all or whitelist")
	}
	for _, group := range []struct {
		name string
		vals []string
	}{{"allowed_models", r.AllowedModels}, {"denied_models", r.DeniedModels}} {
		seen := map[string]bool{}
		for _, raw := range group.vals {
			p := strings.TrimSpace(raw)
			if p == "" {
				return fmt.Errorf("%s contains an empty pattern", group.name)
			}
			if strings.Count(p, "*") > 1 || (strings.Contains(p, "*") && !strings.HasSuffix(p, "*")) {
				return fmt.Errorf("%s pattern %q must be exact, '*' or use one trailing '*'", group.name, p)
			}
			if seen[p] {
				return fmt.Errorf("%s contains duplicate pattern %q", group.name, p)
			}
			seen[p] = true
		}
	}
	return nil
}

// ValidateModelAccessRule uses the same compact pattern syntax as placement.
func ValidateModelAccessRule(r ModelAccessRule) error {
	return ValidateModelPlacementRule(ModelPlacementRule{Mode: r.Mode, AllowedModels: r.AllowedModels, DeniedModels: r.DeniedModels})
}

// ModelAccessAllowed evaluates exact and trailing-wildcard rules. More specific
// rules win; at equal specificity deny wins.
func ModelAccessAllowed(r ModelAccessRule, model string) bool {
	mode := strings.TrimSpace(r.Mode)
	if mode == "" {
		mode = "allow_all"
	}
	model = strings.TrimSpace(model)
	plain := strings.TrimSuffix(model, ":latest")
	match := func(pattern string) (int, bool) {
		pattern = strings.TrimSpace(pattern)
		if pattern == model || pattern == plain {
			return 100000 + len(pattern), true
		}
		if pattern == "*" {
			return 0, true
		}
		if strings.HasSuffix(pattern, "*") {
			prefix := strings.TrimSuffix(pattern, "*")
			if strings.HasPrefix(model, prefix) || strings.HasPrefix(plain, prefix) {
				return len(prefix), true
			}
		}
		return -1, false
	}
	bestAllow, bestDeny := -1, -1
	for _, p := range r.AllowedModels {
		if n, ok := match(p); ok && n > bestAllow {
			bestAllow = n
		}
	}
	for _, p := range r.DeniedModels {
		if n, ok := match(p); ok && n > bestDeny {
			bestDeny = n
		}
	}
	if bestAllow >= 0 || bestDeny >= 0 {
		return bestAllow > bestDeny
	}
	return mode != "whitelist"
}

func (c *Config) ModelAccessRuleForTenant(tenant string) ModelAccessRule {
	if r, ok := c.ModelAccess.Tenants[tenant]; ok {
		if r.Mode == "" {
			r.Mode = "allow_all"
		}
		return r
	}
	r := c.ModelAccess.Default
	if r.Mode == "" {
		r.Mode = "allow_all"
	}
	return r
}

func (c *Config) Policy(tenant string) TenantPolicy {
	p, ok := c.Scheduler.Policies[tenant]
	if !ok {
		p = c.Scheduler.Policies["*"]
	}
	if p.TenantWeight <= 0 {
		p.TenantWeight = c.Scheduler.DefaultTenantWeight
	}
	if p.ActorWeight <= 0 {
		p.ActorWeight = c.Scheduler.DefaultActorWeight
	}
	return p
}
