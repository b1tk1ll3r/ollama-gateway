package auth

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/example/ollama-fair-gateway/internal/config"
)

type discoveryDoc struct {
	Issuer                string `json:"issuer"`
	JWKSURI               string `json:"jwks_uri"`
	AuthorizationEndpoint string `json:"authorization_endpoint"`
	TokenEndpoint         string `json:"token_endpoint"`
}
type jwksDoc struct {
	Keys []json.RawMessage `json:"keys"`
}
type jwkHeader struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Alg string `json:"alg"`
	Use string `json:"use"`
	N   string `json:"n"`
	E   string `json:"e"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
}
type jwtHeader struct {
	Alg string `json:"alg"`
	Kid string `json:"kid"`
	Typ string `json:"typ"`
}

type OIDCVerifier struct {
	cfg                   config.OIDCConfig
	issuer                string
	jwksURI               string
	authorizationEndpoint string
	tokenEndpoint         string
	client                *http.Client
	mu                    sync.RWMutex
	keys                  map[string]crypto.PublicKey
	lastRefresh           time.Time
	allowed               map[string]bool
	adminGroups           map[string]bool
}

func NewOIDCVerifier(ctx context.Context, cfg config.OIDCConfig) (*OIDCVerifier, error) {
	issuer := strings.TrimSuffix(cfg.Issuer, "/")
	v := &OIDCVerifier{cfg: cfg, issuer: issuer, client: &http.Client{Timeout: 10 * time.Second}, keys: map[string]crypto.PublicKey{}, allowed: map[string]bool{}, adminGroups: map[string]bool{}}
	for _, a := range cfg.AllowedAlgorithms {
		v.allowed[a] = true
	}
	for _, g := range cfg.AdminGroups {
		v.adminGroups[g] = true
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, issuer+"/.well-known/openid-configuration", nil)
	if err != nil {
		return nil, err
	}
	resp, err := v.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("oidc discovery: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("oidc discovery: HTTP %d", resp.StatusCode)
	}
	var d discoveryDoc
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&d); err != nil {
		return nil, err
	}
	if strings.TrimSuffix(d.Issuer, "/") != issuer {
		return nil, fmt.Errorf("oidc discovery issuer mismatch: got %q", d.Issuer)
	}
	if d.JWKSURI == "" {
		return nil, errors.New("oidc discovery returned no jwks_uri")
	}
	v.jwksURI = d.JWKSURI
	v.authorizationEndpoint = d.AuthorizationEndpoint
	v.tokenEndpoint = d.TokenEndpoint
	if err := v.refresh(ctx, true); err != nil {
		return nil, err
	}
	return v, nil
}

func (v *OIDCVerifier) Verify(ctx context.Context, token string) (Identity, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return Identity{}, errors.New("token is not a JWT")
	}
	hb, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return Identity{}, err
	}
	var h jwtHeader
	if err := json.Unmarshal(hb, &h); err != nil {
		return Identity{}, err
	}
	if h.Alg == "none" || !v.allowed[h.Alg] {
		return Identity{}, fmt.Errorf("JWT algorithm %q is not allowed", h.Alg)
	}
	key := v.key(h.Kid)
	if key == nil {
		if err := v.refresh(ctx, false); err != nil {
			return Identity{}, err
		}
		key = v.key(h.Kid)
	}
	if key == nil {
		return Identity{}, fmt.Errorf("no signing key for kid %q", h.Kid)
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return Identity{}, err
	}
	signingInput := []byte(parts[0] + "." + parts[1])
	if err := verifySignature(h.Alg, key, signingInput, sig); err != nil {
		// Some providers rotate key material while reusing a kid. Refresh once
		// (subject to the anti-thundering-herd interval) before rejecting.
		_ = v.refresh(ctx, false)
		key = v.key(h.Kid)
		if key == nil {
			return Identity{}, err
		}
		if err2 := verifySignature(h.Alg, key, signingInput, sig); err2 != nil {
			return Identity{}, err2
		}
	}
	pb, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return Identity{}, err
	}
	dec := json.NewDecoder(strings.NewReader(string(pb)))
	dec.UseNumber()
	claims := map[string]any{}
	if err := dec.Decode(&claims); err != nil {
		return Identity{}, err
	}
	now := time.Now()
	skew := v.cfg.ClockSkew.Value()
	if iss, _ := claims["iss"].(string); strings.TrimSuffix(iss, "/") != v.issuer {
		return Identity{}, errors.New("issuer mismatch")
	}
	if !audContains(claims["aud"], v.cfg.Audience) {
		return Identity{}, errors.New("audience mismatch")
	}
	if exp, ok := numericTime(claims["exp"]); !ok || now.After(exp.Add(skew)) {
		return Identity{}, errors.New("token expired or missing exp")
	}
	if nbf, ok := numericTime(claims["nbf"]); ok && now.Add(skew).Before(nbf) {
		return Identity{}, errors.New("token not valid yet")
	}
	sub, _ := claims["sub"].(string)
	if sub == "" {
		return Identity{}, errors.New("token has no sub")
	}
	tenant := claimString(claims, v.cfg.TenantClaim)
	if tenant == "" {
		tenant = sub
	}
	app := claimString(claims, v.cfg.ApplicationClaim)
	scopes := map[string]bool{}
	for _, s := range claimStrings(claims["scope"]) {
		for _, p := range strings.Fields(s) {
			scopes[p] = true
		}
	}
	for _, s := range claimStrings(claims["scp"]) {
		for _, p := range strings.Fields(s) {
			scopes[p] = true
		}
	}
	groups := claimStrings(claimValue(claims, v.cfg.GroupsClaim))
	for _, g := range groups {
		if v.adminGroups[g] {
			scopes["gateway:admin"] = true
		}
	}
	return Identity{Tenant: tenant, Subject: sub, Application: app, AuthType: "oidc", Scopes: scopes}, nil
}

func (v *OIDCVerifier) key(kid string) crypto.PublicKey {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if kid != "" {
		return v.keys[kid]
	}
	if len(v.keys) == 1 {
		for _, k := range v.keys {
			return k
		}
	}
	return nil
}
func (v *OIDCVerifier) refresh(ctx context.Context, force bool) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if !force && time.Since(v.lastRefresh) < v.cfg.JWKSRefreshMinInterval.Value() {
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.jwksURI, nil)
	if err != nil {
		return err
	}
	resp, err := v.client.Do(req)
	if err != nil {
		return fmt.Errorf("fetch jwks: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("fetch jwks: HTTP %d", resp.StatusCode)
	}
	var d jwksDoc
	if err := json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(&d); err != nil {
		return err
	}
	keys := map[string]crypto.PublicKey{}
	for _, raw := range d.Keys {
		var j jwkHeader
		if err := json.Unmarshal(raw, &j); err != nil {
			continue
		}
		k, err := parseJWK(j)
		if err != nil {
			continue
		}
		keys[j.Kid] = k
	}
	if len(keys) == 0 {
		return errors.New("jwks contained no supported signing keys")
	}
	v.keys = keys
	v.lastRefresh = time.Now()
	return nil
}
func parseJWK(j jwkHeader) (crypto.PublicKey, error) {
	dec := base64.RawURLEncoding.DecodeString
	switch j.Kty {
	case "RSA":
		nB, err := dec(j.N)
		if err != nil {
			return nil, err
		}
		eB, err := dec(j.E)
		if err != nil {
			return nil, err
		}
		e := 0
		for _, b := range eB {
			e = e<<8 + int(b)
		}
		if e == 0 {
			return nil, errors.New("bad RSA exponent")
		}
		return &rsa.PublicKey{N: new(big.Int).SetBytes(nB), E: e}, nil
	case "EC":
		xb, err := dec(j.X)
		if err != nil {
			return nil, err
		}
		yb, err := dec(j.Y)
		if err != nil {
			return nil, err
		}
		var c elliptic.Curve
		switch j.Crv {
		case "P-256":
			c = elliptic.P256()
		case "P-384":
			c = elliptic.P384()
		case "P-521":
			c = elliptic.P521()
		default:
			return nil, errors.New("unsupported EC curve")
		}
		x, y := new(big.Int).SetBytes(xb), new(big.Int).SetBytes(yb)
		if !c.IsOnCurve(x, y) {
			return nil, errors.New("EC key is not on curve")
		}
		return &ecdsa.PublicKey{Curve: c, X: x, Y: y}, nil
	case "OKP":
		if j.Crv != "Ed25519" {
			return nil, errors.New("unsupported OKP curve")
		}
		b, err := dec(j.X)
		if err != nil {
			return nil, err
		}
		if len(b) != ed25519.PublicKeySize {
			return nil, errors.New("bad Ed25519 key")
		}
		return ed25519.PublicKey(b), nil
	default:
		return nil, errors.New("unsupported key type")
	}
}
func verifySignature(alg string, key crypto.PublicKey, msg, sig []byte) error {
	var hash crypto.Hash
	var digest []byte
	switch alg {
	case "RS256", "PS256", "ES256":
		h := sha256.Sum256(msg)
		hash = crypto.SHA256
		digest = h[:]
	case "RS384", "PS384", "ES384":
		h := sha512.Sum384(msg)
		hash = crypto.SHA384
		digest = h[:]
	case "RS512", "PS512", "ES512":
		h := sha512.Sum512(msg)
		hash = crypto.SHA512
		digest = h[:]
	case "EdDSA":
		pk, ok := key.(ed25519.PublicKey)
		if !ok || !ed25519.Verify(pk, msg, sig) {
			return errors.New("invalid JWT signature")
		}
		return nil
	default:
		return errors.New("unsupported JWT algorithm")
	}
	if strings.HasPrefix(alg, "RS") {
		pk, ok := key.(*rsa.PublicKey)
		if !ok {
			return errors.New("JWT key type mismatch")
		}
		if err := rsa.VerifyPKCS1v15(pk, hash, digest, sig); err != nil {
			return errors.New("invalid JWT signature")
		}
		return nil
	}
	if strings.HasPrefix(alg, "PS") {
		pk, ok := key.(*rsa.PublicKey)
		if !ok {
			return errors.New("JWT key type mismatch")
		}
		if err := rsa.VerifyPSS(pk, hash, digest, sig, &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash, Hash: hash}); err != nil {
			return errors.New("invalid JWT signature")
		}
		return nil
	}
	pk, ok := key.(*ecdsa.PublicKey)
	if !ok {
		return errors.New("JWT key type mismatch")
	}
	n := (pk.Curve.Params().BitSize + 7) / 8
	if len(sig) != 2*n {
		return errors.New("bad ECDSA JWT signature length")
	}
	r, s := new(big.Int).SetBytes(sig[:n]), new(big.Int).SetBytes(sig[n:])
	if !ecdsa.Verify(pk, digest, r, s) {
		return errors.New("invalid JWT signature")
	}
	return nil
}
func audContains(v any, want string) bool {
	for _, s := range claimStrings(v) {
		if s == want {
			return true
		}
	}
	return false
}
func claimStrings(v any) []string {
	switch x := v.(type) {
	case string:
		return []string{x}
	case []any:
		out := []string{}
		for _, z := range x {
			if s, ok := z.(string); ok {
				out = append(out, s)
			}
		}
		return out
	case []string:
		return x
	}
	return nil
}
func numericTime(v any) (time.Time, bool) {
	var n int64
	switch x := v.(type) {
	case json.Number:
		i, e := x.Int64()
		if e != nil {
			return time.Time{}, false
		}
		n = i
	case float64:
		n = int64(x)
	case int64:
		n = x
	default:
		return time.Time{}, false
	}
	return time.Unix(n, 0), true
}
func claimValue(m map[string]any, path string) any {
	if path == "" {
		return nil
	}
	var cur any = m
	for _, p := range strings.Split(path, ".") {
		mm, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = mm[p]
	}
	return cur
}
func claimString(m map[string]any, path string) string {
	if s, ok := claimValue(m, path).(string); ok {
		return s
	}
	return ""
}

type BrowserEndpoints struct {
	Authorization string `json:"authorization_endpoint"`
	Token         string `json:"token_endpoint"`
}

type TokenExchange struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int64  `json:"expires_in"`
	IDToken     string `json:"id_token,omitempty"`
	Scope       string `json:"scope,omitempty"`
}

func (v *OIDCVerifier) BrowserEndpoints() BrowserEndpoints {
	return BrowserEndpoints{Authorization: v.authorizationEndpoint, Token: v.tokenEndpoint}
}

func (v *OIDCVerifier) ExchangeCode(ctx context.Context, code, redirectURI, clientID, clientSecret, verifier string) (TokenExchange, error) {
	if v.tokenEndpoint == "" {
		return TokenExchange{}, errors.New("OIDC discovery returned no token_endpoint")
	}
	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", code)
	form.Set("redirect_uri", redirectURI)
	form.Set("client_id", clientID)
	if verifier != "" {
		form.Set("code_verifier", verifier)
	}
	if clientSecret != "" {
		form.Set("client_secret", clientSecret)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, v.tokenEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return TokenExchange{}, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := v.client.Do(req)
	if err != nil {
		return TokenExchange{}, fmt.Errorf("OIDC token exchange: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return TokenExchange{}, err
	}
	if resp.StatusCode/100 != 2 {
		return TokenExchange{}, fmt.Errorf("OIDC token exchange: HTTP %d", resp.StatusCode)
	}
	var out TokenExchange
	if err := json.Unmarshal(body, &out); err != nil {
		return TokenExchange{}, err
	}
	if out.AccessToken == "" {
		return TokenExchange{}, errors.New("OIDC token response contains no access_token")
	}
	return out, nil
}
