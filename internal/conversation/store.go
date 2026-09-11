package conversation

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/example/ollama-fair-gateway/internal/config"
	"github.com/example/ollama-fair-gateway/internal/state"
)

var ErrContextTooLarge = errors.New("conversation context exceeds configured max_content_bytes")

// Entry is the minimum content-bearing state required to implement
// previous_response_id semantics for the OpenAI Responses API. Context is a
// JSON array containing the flattened input/output items through this response.
type Entry struct {
	ID        string          `json:"id"`
	Tenant    string          `json:"tenant"`
	Actor     string          `json:"actor"`
	Model     string          `json:"model,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
	ExpiresAt time.Time       `json:"expires_at"`
	Context   json.RawMessage `json:"context"`
}

type Status struct {
	Enabled   bool      `json:"enabled"`
	Entries   int       `json:"entries"`
	Oldest    time.Time `json:"oldest_at,omitempty"`
	Newest    time.Time `json:"newest_at,omitempty"`
	ExpiresAt time.Time `json:"next_expiry_at,omitempty"`
}

type Store struct {
	mu   sync.Mutex
	cfg  config.ConversationsConfig
	file state.AtomicJSON
	key  [32]byte
	now  func() time.Time
	data map[string]Entry
}

type diskEnvelope struct {
	Version    int       `json:"version"`
	Algorithm  string    `json:"algorithm"`
	Nonce      string    `json:"nonce"`
	Ciphertext string    `json:"ciphertext"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type diskPlaintext struct {
	Version int              `json:"version"`
	Entries map[string]Entry `json:"entries"`
}

func New(cfg config.ConversationsConfig, path string) (*Store, error) {
	s := &Store{cfg: cfg, file: state.AtomicJSON{Path: path, Mode: 0600}, now: time.Now, data: map[string]Entry{}}
	s.key = sha256.Sum256([]byte(cfg.EncryptionKey))
	if !cfg.Enabled {
		return s, nil
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) Enabled() bool { return s != nil && s.cfg.Enabled }

// StartCleanup enforces retention even when no requests touch the store.
func (s *Store) StartCleanup(ctx context.Context, onError func(error)) {
	if !s.Enabled() {
		return
	}
	interval := s.cfg.Retention.Value() / 4
	if interval < time.Minute {
		interval = time.Minute
	}
	if interval > 15*time.Minute {
		interval = 15 * time.Minute
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := s.pruneAndSave(); err != nil && onError != nil {
					onError(err)
				}
			}
		}
	}()
}

func (s *Store) pruneAndSave() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.pruneLocked(s.now().UTC()) {
		return nil
	}
	return s.saveLocked()
}

func (s *Store) Get(id, tenant, actor string) (Entry, bool, error) {
	if !s.Enabled() || id == "" {
		return Entry{}, false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	changed := s.pruneLocked(s.now().UTC())
	e, ok := s.data[id]
	if !ok || e.Tenant != tenant || e.Actor != actor {
		if changed {
			_ = s.saveLocked()
		}
		return Entry{}, false, nil
	}
	if changed {
		if err := s.saveLocked(); err != nil {
			return Entry{}, false, err
		}
	}
	e.Context = append(json.RawMessage(nil), e.Context...)
	return e, true, nil
}

func (s *Store) Put(e Entry) error {
	if !s.Enabled() {
		return nil
	}
	if e.ID == "" || e.Tenant == "" || e.Actor == "" {
		return errors.New("conversation entry requires id, tenant, and actor")
	}
	if len(e.Context) == 0 || !json.Valid(e.Context) {
		return errors.New("conversation entry context must contain valid JSON")
	}
	if s.cfg.MaxContentBytes > 0 && int64(len(e.Context)) > s.cfg.MaxContentBytes {
		return ErrContextTooLarge
	}
	now := s.now().UTC()
	if e.CreatedAt.IsZero() {
		e.CreatedAt = now
	} else {
		e.CreatedAt = e.CreatedAt.UTC()
	}
	if e.ExpiresAt.IsZero() {
		e.ExpiresAt = e.CreatedAt.Add(s.cfg.Retention.Value())
	} else {
		e.ExpiresAt = e.ExpiresAt.UTC()
	}
	e.Context = append(json.RawMessage(nil), e.Context...)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(now)
	s.data[e.ID] = e
	s.enforceLimitLocked()
	return s.saveLocked()
}

func (s *Store) Delete(id, tenant, actor string) error {
	if !s.Enabled() || id == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[id]
	if !ok || e.Tenant != tenant || e.Actor != actor {
		return nil
	}
	delete(s.data, id)
	return s.saveLocked()
}

func (s *Store) Compact() error {
	if !s.Enabled() {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(s.now().UTC())
	s.enforceLimitLocked()
	return s.saveLocked()
}

func (s *Store) Status() Status {
	st := Status{Enabled: s.Enabled()}
	if !s.Enabled() {
		return st
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now().UTC()
	_ = s.pruneLocked(now)
	st.Entries = len(s.data)
	for _, e := range s.data {
		if st.Oldest.IsZero() || e.CreatedAt.Before(st.Oldest) {
			st.Oldest = e.CreatedAt
		}
		if st.Newest.IsZero() || e.CreatedAt.After(st.Newest) {
			st.Newest = e.CreatedAt
		}
		if st.ExpiresAt.IsZero() || e.ExpiresAt.Before(st.ExpiresAt) {
			st.ExpiresAt = e.ExpiresAt
		}
	}
	return st
}

func (s *Store) load() error {
	var env diskEnvelope
	if err := s.file.Load(&env); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if env.Version != 1 || env.Algorithm != "AES-256-GCM" {
		return fmt.Errorf("unsupported conversation store envelope version/algorithm")
	}
	nonce, err := base64.StdEncoding.DecodeString(env.Nonce)
	if err != nil {
		return fmt.Errorf("decode conversation nonce: %w", err)
	}
	ct, err := base64.StdEncoding.DecodeString(env.Ciphertext)
	if err != nil {
		return fmt.Errorf("decode conversation ciphertext: %w", err)
	}
	block, err := aes.NewCipher(s.key[:])
	if err != nil {
		return err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return err
	}
	plain, err := gcm.Open(nil, nonce, ct, []byte("ollama-fair-gateway/conversations/v1"))
	if err != nil {
		return errors.New("decrypt conversation store: encryption key mismatch or file corruption")
	}
	var snap diskPlaintext
	if err := json.Unmarshal(plain, &snap); err != nil {
		return fmt.Errorf("decode conversation store: %w", err)
	}
	if snap.Version != 1 {
		return fmt.Errorf("unsupported conversation plaintext version %d", snap.Version)
	}
	if snap.Entries != nil {
		s.data = snap.Entries
	}
	s.pruneLocked(s.now().UTC())
	s.enforceLimitLocked()
	return nil
}

func (s *Store) saveLocked() error {
	plain, err := json.Marshal(diskPlaintext{Version: 1, Entries: s.data})
	if err != nil {
		return err
	}
	block, err := aes.NewCipher(s.key[:])
	if err != nil {
		return err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return err
	}
	ct := gcm.Seal(nil, nonce, plain, []byte("ollama-fair-gateway/conversations/v1"))
	env := diskEnvelope{Version: 1, Algorithm: "AES-256-GCM", Nonce: base64.StdEncoding.EncodeToString(nonce), Ciphertext: base64.StdEncoding.EncodeToString(ct), UpdatedAt: s.now().UTC()}
	return s.file.Save(env)
}

func (s *Store) pruneLocked(now time.Time) bool {
	changed := false
	for id, e := range s.data {
		if !e.ExpiresAt.IsZero() && !e.ExpiresAt.After(now) {
			delete(s.data, id)
			changed = true
		}
	}
	return changed
}

func (s *Store) enforceLimitLocked() {
	max := s.cfg.MaxEntries
	if max <= 0 || len(s.data) <= max {
		return
	}
	type pair struct {
		id string
		t  time.Time
	}
	xs := make([]pair, 0, len(s.data))
	for id, e := range s.data {
		xs = append(xs, pair{id: id, t: e.CreatedAt})
	}
	sort.Slice(xs, func(i, j int) bool { return xs[i].t.Before(xs[j].t) })
	for len(s.data) > max && len(xs) > 0 {
		delete(s.data, xs[0].id)
		xs = xs[1:]
	}
}
