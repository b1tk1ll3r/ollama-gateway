package auth

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/example/ollama-fair-gateway/internal/config"
)

func TestOIDCVerifierRS256(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	var issuer string
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()
	issuer = srv.URL
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"issuer": issuer, "jwks_uri": issuer + "/jwks"})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, r *http.Request) {
		e := big.NewInt(int64(key.PublicKey.E)).Bytes()
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]any{"kty": "RSA", "kid": "k1", "alg": "RS256", "n": base64.RawURLEncoding.EncodeToString(key.PublicKey.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString(e)}}})
	})
	v, err := NewOIDCVerifier(context.Background(), config.OIDCConfig{Enabled: true, Issuer: issuer, Audience: "gateway", TenantClaim: "tenant", ApplicationClaim: "azp", GroupsClaim: "groups", AdminGroups: []string{"admins"}, ClockSkew: config.Duration(time.Second), JWKSRefreshMinInterval: config.Duration(time.Second), AllowedAlgorithms: []string{"RS256"}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	tok := signRS256(t, key, "k1", map[string]any{"iss": issuer, "aud": "gateway", "sub": "alice", "tenant": "team-a", "azp": "web", "groups": []string{"admins"}, "exp": now.Add(time.Minute).Unix(), "nbf": now.Add(-time.Second).Unix()})
	id, err := v.Verify(context.Background(), tok)
	if err != nil {
		t.Fatal(err)
	}
	if id.Tenant != "team-a" || id.Subject != "alice" || id.Application != "web" || !id.IsAdmin() {
		t.Fatalf("unexpected identity: %#v", id)
	}
}
func signRS256(t *testing.T, key *rsa.PrivateKey, kid string, claims map[string]any) string {
	t.Helper()
	h, _ := json.Marshal(map[string]any{"alg": "RS256", "typ": "JWT", "kid": kid})
	p, _ := json.Marshal(claims)
	a := base64.RawURLEncoding.EncodeToString(h) + "." + base64.RawURLEncoding.EncodeToString(p)
	sum := sha256.Sum256([]byte(a))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sum[:])
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%s.%s", a, base64.RawURLEncoding.EncodeToString(sig))
}
