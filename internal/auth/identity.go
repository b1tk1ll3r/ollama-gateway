package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/example/ollama-fair-gateway/internal/config"
)

type Identity struct {
	Tenant       string                 `json:"tenant"`
	Subject      string                 `json:"subject"`
	Application  string                 `json:"application,omitempty"`
	AuthType     string                 `json:"auth_type"`
	Scopes       map[string]bool        `json:"-"`
	ClientIP     string                 `json:"client_ip"`
	ModelACLSet  bool                   `json:"-"`
	ModelAccess  config.ModelAccessRule `json:"-"`
	ServiceClass string                 `json:"-"`
}

func (i Identity) Actor() string {
	// Interactive OIDC identities are fair-scheduled by subject, so one user
	// cannot gain extra shares by using multiple OAuth clients. Static API-key
	// and trusted-IP identities are normally applications and use that identity.
	if i.AuthType != "oidc" && i.Application != "" {
		return "app:" + i.Application
	}
	if i.Subject != "" {
		return i.Subject
	}
	if i.Application != "" {
		return "app:" + i.Application
	}
	return "anonymous"
}
func (i Identity) HasScope(s string) bool { return i.Scopes[s] || i.Scopes["*"] }
func (i Identity) IsAdmin() bool          { return i.HasScope("gateway:admin") }

type ctxKey struct{}

func WithIdentity(ctx context.Context, i Identity) context.Context {
	return context.WithValue(ctx, ctxKey{}, i)
}
func FromContext(ctx context.Context) (Identity, bool) {
	i, ok := ctx.Value(ctxKey{}).(Identity)
	return i, ok
}

// APIKeyInfo is safe to return through the admin API. It never contains the
// key secret. UI-created keys may be backed by a durable RuntimeKeyStore.
type APIKeyInfo struct {
	ID            string     `json:"id,omitempty"`
	Name          string     `json:"name"`
	Tenant        string     `json:"tenant"`
	Subject       string     `json:"subject"`
	Application   string     `json:"application,omitempty"`
	Scopes        []string   `json:"scopes"`
	AllowedModels []string   `json:"allowed_models,omitempty"`
	DeniedModels  []string   `json:"denied_models,omitempty"`
	ServiceClass  string     `json:"service_class,omitempty"`
	Source        string     `json:"source"` // config | runtime | persistent
	KeyHint       string     `json:"key_hint,omitempty"`
	CreatedAt     *time.Time `json:"created_at,omitempty"`
	Deletable     bool       `json:"deletable"`
}

type APIKeyCreate struct {
	Name          string
	Tenant        string
	Subject       string
	Application   string
	Scopes        []string
	AllowedModels []string
	DeniedModels  []string
	ServiceClass  string
}

// StoredAPIKey is the durable representation of a UI-created API key. Only
// the SHA-256 hash is persisted; the plaintext secret never leaves CreateAPIKey.
type StoredAPIKey struct {
	ID            string    `json:"id"`
	Name          string    `json:"name"`
	Tenant        string    `json:"tenant"`
	Subject       string    `json:"subject"`
	Application   string    `json:"application,omitempty"`
	Scopes        []string  `json:"scopes"`
	AllowedModels []string  `json:"allowed_models,omitempty"`
	DeniedModels  []string  `json:"denied_models,omitempty"`
	ServiceClass  string    `json:"service_class,omitempty"`
	HashHex       string    `json:"hash_sha256"`
	KeyHint       string    `json:"key_hint,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}

type RuntimeKeyStore interface {
	Load() ([]StoredAPIKey, error)
	Put(StoredAPIKey) error
	Delete(string) error
	Health(context.Context) error
}

type managedKey struct {
	identity Identity
	info     APIKeyInfo
}

type bypassRule struct {
	nets     []*net.IPNet
	identity Identity
}

type Authenticator struct {
	oidc            *OIDCVerifier
	mu              sync.RWMutex
	keys            map[[32]byte]managedKey
	runtime         map[string][32]byte
	bypass          []bypassRule
	trusted         []*net.IPNet
	bypassForwarded bool
	store           RuntimeKeyStore
}

func New(ctx context.Context, cfg config.AuthConfig) (*Authenticator, error) {
	return NewWithRuntimeStore(ctx, cfg, nil)
}

func NewWithRuntimeStore(ctx context.Context, cfg config.AuthConfig, store RuntimeKeyStore) (*Authenticator, error) {
	a := &Authenticator{keys: make(map[[32]byte]managedKey), runtime: make(map[string][32]byte), bypassForwarded: cfg.IPBypassUseForwardedIP, store: store}
	for _, c := range cfg.TrustedProxies {
		_, n, _ := net.ParseCIDR(c)
		a.trusted = append(a.trusted, n)
	}
	for _, b := range cfg.IPBypass {
		r := bypassRule{identity: Identity{Tenant: b.Tenant, Subject: b.Subject, Application: b.Application, AuthType: "ip-bypass", Scopes: scopeMap(b.Scopes)}}
		if r.identity.Subject == "" {
			r.identity.Subject = "ip-bypass"
		}
		for _, c := range b.CIDRs {
			_, n, _ := net.ParseCIDR(c)
			r.nets = append(r.nets, n)
		}
		a.bypass = append(a.bypass, r)
	}
	for _, k := range cfg.APIKeys {
		if k.Key == "" {
			return nil, fmt.Errorf("api key %q is empty (is its environment variable set?)", k.Name)
		}
		id := Identity{Tenant: k.Tenant, Subject: k.Subject, Application: k.Application, AuthType: "api-key", Scopes: scopeMap(k.Scopes), ModelACLSet: len(k.AllowedModels) > 0 || len(k.DeniedModels) > 0, ModelAccess: config.ModelAccessRule{Mode: "allow_all", AllowedModels: append([]string(nil), k.AllowedModels...), DeniedModels: append([]string(nil), k.DeniedModels...)}, ServiceClass: strings.TrimSpace(k.ServiceClass)}
		if id.Tenant == "" {
			return nil, fmt.Errorf("api key %q has no tenant", k.Name)
		}
		if id.Subject == "" {
			id.Subject = "apikey:" + k.Name
		}
		h := sha256.Sum256([]byte(k.Key))
		a.keys[h] = managedKey{identity: id, info: APIKeyInfo{Name: k.Name, Tenant: id.Tenant, Subject: id.Subject, Application: id.Application, Scopes: sortedScopes(k.Scopes), AllowedModels: append([]string(nil), k.AllowedModels...), DeniedModels: append([]string(nil), k.DeniedModels...), ServiceClass: strings.TrimSpace(k.ServiceClass), Source: "config", Deletable: false}}
	}
	if store != nil {
		records, err := store.Load()
		if err != nil {
			return nil, fmt.Errorf("load persistent API keys: %w", err)
		}
		for _, rec := range records {
			hb, err := hex.DecodeString(rec.HashHex)
			if err != nil || len(hb) != sha256.Size {
				return nil, fmt.Errorf("persistent API key %q has invalid hash", rec.ID)
			}
			var h [32]byte
			copy(h[:], hb)
			if rec.ID == "" || rec.Tenant == "" {
				return nil, fmt.Errorf("persistent API key has missing id or tenant")
			}
			created := rec.CreatedAt
			info := APIKeyInfo{ID: rec.ID, Name: rec.Name, Tenant: rec.Tenant, Subject: rec.Subject, Application: rec.Application, Scopes: sortedScopes(rec.Scopes), AllowedModels: append([]string(nil), rec.AllowedModels...), DeniedModels: append([]string(nil), rec.DeniedModels...), ServiceClass: rec.ServiceClass, Source: "persistent", KeyHint: rec.KeyHint, CreatedAt: &created, Deletable: true}
			id := Identity{Tenant: rec.Tenant, Subject: rec.Subject, Application: rec.Application, AuthType: "api-key", Scopes: scopeMap(rec.Scopes), ModelACLSet: len(rec.AllowedModels) > 0 || len(rec.DeniedModels) > 0, ModelAccess: config.ModelAccessRule{Mode: "allow_all", AllowedModels: append([]string(nil), rec.AllowedModels...), DeniedModels: append([]string(nil), rec.DeniedModels...)}, ServiceClass: rec.ServiceClass}
			if id.Subject == "" {
				id.Subject = "apikey:" + rec.Name
				info.Subject = id.Subject
			}
			if _, exists := a.keys[h]; exists {
				return nil, fmt.Errorf("persistent API key hash collision for %q", rec.ID)
			}
			if _, exists := a.runtime[rec.ID]; exists {
				return nil, fmt.Errorf("duplicate persistent API key id %q", rec.ID)
			}
			a.keys[h] = managedKey{identity: id, info: info}
			a.runtime[rec.ID] = h
		}
	}

	if cfg.OIDC.Enabled {
		v, err := NewOIDCVerifier(ctx, cfg.OIDC)
		if err != nil {
			return nil, err
		}
		a.oidc = v
	}
	return a, nil
}

func scopeMap(in []string) map[string]bool {
	m := map[string]bool{}
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s != "" {
			m[s] = true
		}
	}
	return m
}

func sortedScopes(in []string) []string {
	m := map[string]struct{}{}
	for _, s := range in {
		if s = strings.TrimSpace(s); s != "" {
			m[s] = struct{}{}
		}
	}
	out := make([]string, 0, len(m))
	for s := range m {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

func (a *Authenticator) Authenticate(r *http.Request) (Identity, error) {
	ip := a.ClientIP(r)
	bypassIP := a.PeerIP(r)
	if a.bypassForwarded {
		bypassIP = ip
	}
	for _, rule := range a.bypass {
		if containsAny(rule.nets, net.ParseIP(bypassIP)) {
			id := rule.identity
			// Keep the resolved client IP for observability even though the
			// authentication decision defaults to the TCP peer address.
			id.ClientIP = ip
			return id, nil
		}
	}
	token := ""
	if x := strings.TrimSpace(r.Header.Get("X-API-Key")); x != "" {
		token = x
	}
	if token == "" {
		h := r.Header.Get("Authorization")
		if len(h) > 7 && strings.EqualFold(h[:7], "Bearer ") {
			token = strings.TrimSpace(h[7:])
		}
	}
	if token != "" {
		sum := sha256.Sum256([]byte(token))
		a.mu.RLock()
		k, ok := a.keys[sum]
		a.mu.RUnlock()
		if ok {
			id := k.identity
			id.ClientIP = ip
			return id, nil
		}
		if a.oidc != nil {
			id, err := a.oidc.Verify(r.Context(), token)
			if err == nil {
				id.ClientIP = ip
				return id, nil
			}
			return Identity{}, fmt.Errorf("invalid bearer token: %w", err)
		}
	}
	return Identity{}, ErrUnauthorized
}

var ErrUnauthorized = fmt.Errorf("authentication required")

// CreateAPIKey creates an API key. With a RuntimeKeyStore configured, only the
// key hash and metadata are persisted. The returned secret is the
// only copy of the plaintext key and must be shown to the administrator once.
func (a *Authenticator) CreateAPIKey(in APIKeyCreate) (APIKeyInfo, string, error) {
	if a == nil {
		return APIKeyInfo{}, "", errors.New("authenticator unavailable")
	}
	in.Name = strings.TrimSpace(in.Name)
	in.Tenant = strings.TrimSpace(in.Tenant)
	in.Subject = strings.TrimSpace(in.Subject)
	in.Application = strings.TrimSpace(in.Application)
	if in.Name == "" || len(in.Name) > 128 {
		return APIKeyInfo{}, "", errors.New("name is required and must be at most 128 characters")
	}
	if in.Tenant == "" || len(in.Tenant) > 256 {
		return APIKeyInfo{}, "", errors.New("tenant is required and must be at most 256 characters")
	}
	if len(in.Subject) > 256 || len(in.Application) > 256 {
		return APIKeyInfo{}, "", errors.New("subject and application must be at most 256 characters")
	}
	if in.Subject == "" {
		in.Subject = "apikey:" + in.Name
	}
	scopes := sortedScopes(in.Scopes)
	for _, s := range scopes {
		if len(s) > 128 {
			return APIKeyInfo{}, "", errors.New("scope must be at most 128 characters")
		}
	}
	if err := config.ValidateModelAccessRule(config.ModelAccessRule{Mode: "allow_all", AllowedModels: in.AllowedModels, DeniedModels: in.DeniedModels}); err != nil {
		return APIKeyInfo{}, "", fmt.Errorf("model ACL: %w", err)
	}

	secretBytes := make([]byte, 32)
	if _, err := rand.Read(secretBytes); err != nil {
		return APIKeyInfo{}, "", fmt.Errorf("generate key: %w", err)
	}
	secret := "ofg_" + base64.RawURLEncoding.EncodeToString(secretBytes)
	idBytes := make([]byte, 12)
	if _, err := rand.Read(idBytes); err != nil {
		return APIKeyInfo{}, "", fmt.Errorf("generate key id: %w", err)
	}
	id := base64.RawURLEncoding.EncodeToString(idBytes)
	h := sha256.Sum256([]byte(secret))
	createdAt := time.Now().UTC()
	info := APIKeyInfo{ID: id, Name: in.Name, Tenant: in.Tenant, Subject: in.Subject, Application: in.Application, Scopes: scopes, AllowedModels: append([]string(nil), in.AllowedModels...), DeniedModels: append([]string(nil), in.DeniedModels...), ServiceClass: strings.TrimSpace(in.ServiceClass), Source: map[bool]string{true: "persistent", false: "runtime"}[a.store != nil], KeyHint: keyHint(secret), CreatedAt: &createdAt, Deletable: true}
	identity := Identity{Tenant: in.Tenant, Subject: in.Subject, Application: in.Application, AuthType: "api-key", Scopes: scopeMap(scopes), ModelACLSet: len(in.AllowedModels) > 0 || len(in.DeniedModels) > 0, ModelAccess: config.ModelAccessRule{Mode: "allow_all", AllowedModels: append([]string(nil), in.AllowedModels...), DeniedModels: append([]string(nil), in.DeniedModels...)}, ServiceClass: strings.TrimSpace(in.ServiceClass)}

	a.mu.Lock()
	defer a.mu.Unlock()
	for _, existing := range a.keys {
		if existing.info.Name == in.Name && existing.info.Tenant == in.Tenant {
			return APIKeyInfo{}, "", fmt.Errorf("an API key named %q already exists for tenant %q", in.Name, in.Tenant)
		}
	}
	if _, exists := a.keys[h]; exists {
		return APIKeyInfo{}, "", errors.New("generated API key collision")
	}
	if _, exists := a.runtime[id]; exists {
		return APIKeyInfo{}, "", errors.New("generated API key id collision")
	}
	if a.store != nil {
		rec := StoredAPIKey{ID: id, Name: in.Name, Tenant: in.Tenant, Subject: in.Subject, Application: in.Application, Scopes: scopes, AllowedModels: append([]string(nil), in.AllowedModels...), DeniedModels: append([]string(nil), in.DeniedModels...), ServiceClass: strings.TrimSpace(in.ServiceClass), HashHex: hex.EncodeToString(h[:]), KeyHint: info.KeyHint, CreatedAt: createdAt}
		if err := a.store.Put(rec); err != nil {
			return APIKeyInfo{}, "", fmt.Errorf("persist API key: %w", err)
		}
	}
	a.keys[h] = managedKey{identity: identity, info: info}
	a.runtime[id] = h
	return info, secret, nil
}

func keyHint(secret string) string {
	if len(secret) <= 12 {
		return secret
	}
	return secret[:8] + "…" + secret[len(secret)-4:]
}

func (a *Authenticator) APIKeys() []APIKeyInfo {
	if a == nil {
		return nil
	}
	a.mu.RLock()
	out := make([]APIKeyInfo, 0, len(a.keys))
	for _, k := range a.keys {
		i := k.info
		i.Scopes = append([]string(nil), i.Scopes...)
		i.AllowedModels = append([]string(nil), i.AllowedModels...)
		i.DeniedModels = append([]string(nil), i.DeniedModels...)
		out = append(out, i)
	}
	a.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].Source != out[j].Source {
			return out[i].Source < out[j].Source
		}
		if out[i].Tenant != out[j].Tenant {
			return out[i].Tenant < out[j].Tenant
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func (a *Authenticator) DeleteAPIKey(id string) (APIKeyInfo, bool, error) {
	if a == nil || id == "" {
		return APIKeyInfo{}, false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	h, ok := a.runtime[id]
	if !ok {
		return APIKeyInfo{}, false, nil
	}
	k, ok := a.keys[h]
	if !ok {
		delete(a.runtime, id)
		return APIKeyInfo{}, false, nil
	}
	if a.store != nil {
		if err := a.store.Delete(id); err != nil {
			return APIKeyInfo{}, true, fmt.Errorf("delete persistent API key: %w", err)
		}
	}
	delete(a.keys, h)
	delete(a.runtime, id)
	return k.info, true, nil
}

func (a *Authenticator) HasPersistentRuntimeStore() bool { return a != nil && a.store != nil }

func (a *Authenticator) RuntimeStoreHealth(ctx context.Context) error {
	if a == nil || a.store == nil {
		return nil
	}
	return a.store.Health(ctx)
}

func (a *Authenticator) PeerIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	return strings.TrimSpace(host)
}

func (a *Authenticator) ClientIP(r *http.Request) string {
	host := a.PeerIP(r)
	peer := net.ParseIP(host)
	if peer == nil || !containsAny(a.trusted, peer) {
		return host
	}
	parts := strings.Split(r.Header.Get("X-Forwarded-For"), ",")
	chain := make([]net.IP, 0, len(parts)+1)
	for _, p := range parts {
		if ip := net.ParseIP(strings.TrimSpace(p)); ip != nil {
			chain = append(chain, ip)
		}
	}
	chain = append(chain, peer)
	for i := len(chain) - 1; i >= 0; i-- {
		if !containsAny(a.trusted, chain[i]) {
			return chain[i].String()
		}
	}
	if len(chain) > 0 {
		return chain[0].String()
	}
	return strings.TrimSpace(host)
}
func containsAny(nets []*net.IPNet, ip net.IP) bool {
	if ip == nil {
		return false
	}
	for _, n := range nets {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

func (a *Authenticator) OIDCEnabled() bool { return a != nil && a.oidc != nil }

func (a *Authenticator) OIDCBrowserEndpoints() (BrowserEndpoints, bool) {
	if a == nil || a.oidc == nil {
		return BrowserEndpoints{}, false
	}
	return a.oidc.BrowserEndpoints(), true
}

func (a *Authenticator) ExchangeOIDCCode(ctx context.Context, code, redirectURI, clientID, clientSecret, verifier string) (TokenExchange, error) {
	if a == nil || a.oidc == nil {
		return TokenExchange{}, errors.New("OIDC is disabled")
	}
	return a.oidc.ExchangeCode(ctx, code, redirectURI, clientID, clientSecret, verifier)
}

func (a *Authenticator) VerifyOIDCToken(ctx context.Context, token string) (Identity, error) {
	if a == nil || a.oidc == nil {
		return Identity{}, errors.New("OIDC is disabled")
	}
	return a.oidc.Verify(ctx, token)
}
