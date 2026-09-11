package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestModelStopUsesGenerateKeepAliveZero(t *testing.T) {
	var gotMethod, gotPath string
	var got map[string]any
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"done":true}`))
	}))
	defer backend.Close()
	u, _ := url.Parse(backend.URL)

	m := newOperationManager()
	if err := m.modelAction(context.Background(), u, "stop", "gemma4:latest"); err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodPost || gotPath != "/api/generate" {
		t.Fatalf("method/path=%s %s", gotMethod, gotPath)
	}
	if got["model"] != "gemma4:latest" {
		t.Fatalf("model=%v", got["model"])
	}
	if v, ok := got["keep_alive"].(float64); !ok || v != 0 {
		t.Fatalf("keep_alive=%#v", got["keep_alive"])
	}
	if v, ok := got["stream"].(bool); !ok || v {
		t.Fatalf("stream=%#v", got["stream"])
	}
}
