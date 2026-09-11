package auth

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/example/ollama-fair-gateway/internal/config"
)

func bypassTestConfig(useForwarded bool) config.AuthConfig {
	return config.AuthConfig{
		IPBypassUseForwardedIP: useForwarded,
		TrustedProxies:         []string{"10.0.0.0/8"},
		IPBypass: []config.IPBypassConfig{{
			CIDRs:   []string{"127.0.0.1/32"},
			Tenant:  "local",
			Subject: "localhost",
			Scopes:  []string{"gateway:admin"},
		}},
	}
}

func TestIPBypassDefaultsToDirectPeerNotForwardedHeader(t *testing.T) {
	a, err := New(context.Background(), bypassTestConfig(false))
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("GET", "http://gateway/admin", nil)
	r.RemoteAddr = "10.2.3.4:12345"
	r.Header.Set("X-Forwarded-For", "127.0.0.1")
	if got := a.ClientIP(r); got != "127.0.0.1" {
		t.Fatalf("resolved client ip changed: got %q", got)
	}
	if _, err := a.Authenticate(r); err == nil {
		t.Fatal("spoofed forwarded loopback unexpectedly satisfied ip_bypass")
	}
}

func TestIPBypassForwardedCompatibilityMustBeExplicit(t *testing.T) {
	a, err := New(context.Background(), bypassTestConfig(true))
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("GET", "http://gateway/admin", nil)
	r.RemoteAddr = "10.2.3.4:12345"
	r.Header.Set("X-Forwarded-For", "127.0.0.1")
	id, err := a.Authenticate(r)
	if err != nil {
		t.Fatal(err)
	}
	if !id.IsAdmin() || id.AuthType != "ip-bypass" || id.ClientIP != "127.0.0.1" {
		t.Fatalf("unexpected identity: %#v", id)
	}
}

func TestIPBypassStillAcceptsDirectPeer(t *testing.T) {
	a, err := New(context.Background(), bypassTestConfig(false))
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("GET", "http://gateway/admin", nil)
	r.RemoteAddr = "127.0.0.1:12345"
	id, err := a.Authenticate(r)
	if err != nil {
		t.Fatal(err)
	}
	if !id.IsAdmin() || id.ClientIP != "127.0.0.1" {
		t.Fatalf("unexpected identity: %#v", id)
	}
}
