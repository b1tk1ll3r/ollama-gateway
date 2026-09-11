package state

import (
	"encoding/json"
	"errors"
	"os"

	"github.com/example/ollama-fair-gateway/internal/config"
)

const bootstrapSecret = "<bootstrap-secret>"

type ConfigStore struct{ file AtomicJSON }

func NewConfigStore(path string) *ConfigStore {
	return &ConfigStore{file: AtomicJSON{Path: path, Mode: 0600}}
}
func (s *ConfigStore) Path() string { return s.file.Path }

// LoadWithBootstrap restores secrets that deliberately remain owned by the
// startup configuration and are never copied into the persistent UI override.
func (s *ConfigStore) LoadWithBootstrap(base *config.Config) (*config.Config, error) {
	b, err := os.ReadFile(s.file.Path)
	if err != nil {
		return nil, err
	}
	var c config.Config
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	c.Auth.APIKeys = append([]config.APIKeyConfig(nil), base.Auth.APIKeys...)
	if c.UI.SessionSecret == "" || c.UI.SessionSecret == bootstrapSecret {
		c.UI.SessionSecret = base.UI.SessionSecret
	}
	if c.UI.OIDC.ClientSecret == "" || c.UI.OIDC.ClientSecret == bootstrapSecret {
		c.UI.OIDC.ClientSecret = base.UI.OIDC.ClientSecret
	}
	merged, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	return config.ParseBytes(merged)
}

// Save writes a complete config override but strips bootstrap-owned secrets.
func (s *ConfigStore) Save(c *config.Config) error {
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	var clean config.Config
	if err := json.Unmarshal(b, &clean); err != nil {
		return err
	}
	for i := range clean.Auth.APIKeys {
		clean.Auth.APIKeys[i].Key = bootstrapSecret
	}
	if clean.UI.SessionSecret != "" {
		clean.UI.SessionSecret = bootstrapSecret
	}
	if clean.UI.OIDC.ClientSecret != "" {
		clean.UI.OIDC.ClientSecret = bootstrapSecret
	}
	return s.file.Save(&clean)
}
func (s *ConfigStore) Delete() error { return s.file.Delete() }
func (s *ConfigStore) Exists() bool  { _, err := os.Stat(s.file.Path); return err == nil }
func (s *ConfigStore) LoadIfExists(base *config.Config) (*config.Config, bool, error) {
	c, err := s.LoadWithBootstrap(base)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return c, true, nil
}
