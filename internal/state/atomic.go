package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// AtomicJSON provides small, durable JSON state files. Writes are staged in
// the target directory, fsynced, chmodded, then atomically renamed.
type AtomicJSON struct {
	Path string
	Mode os.FileMode
}

func (f AtomicJSON) Load(out any) error {
	b, err := os.ReadFile(f.Path)
	if errors.Is(err, os.ErrNotExist) {
		return os.ErrNotExist
	}
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, out); err != nil {
		return fmt.Errorf("decode %s: %w", f.Path, err)
	}
	return nil
}

func (f AtomicJSON) Save(v any) error {
	if f.Path == "" {
		return errors.New("state path is empty")
	}
	dir := filepath.Dir(f.Path)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	tmp, err := os.CreateTemp(dir, ".state-*.tmp")
	if err != nil {
		return err
	}
	name := tmp.Name()
	ok := false
	defer func() {
		_ = tmp.Close()
		if !ok {
			_ = os.Remove(name)
		}
	}()
	mode := f.Mode
	if mode == 0 {
		mode = 0600
	}
	if err := tmp.Chmod(mode); err != nil {
		return err
	}
	if _, err := tmp.Write(b); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	// os.Rename replaces an existing regular file on Unix. On Windows the
	// destination may need to be removed first; keep a .bak so a failed
	// replacement does not destroy the previous durable state.
	if err := os.Rename(name, f.Path); err != nil {
		bak := f.Path + ".bak"
		_ = os.Remove(bak)
		if _, statErr := os.Stat(f.Path); statErr == nil {
			if rerr := os.Rename(f.Path, bak); rerr != nil {
				return err
			}
		}
		if rerr := os.Rename(name, f.Path); rerr != nil {
			_ = os.Rename(bak, f.Path)
			return rerr
		}
		_ = os.Remove(bak)
	}
	syncDir(dir)
	ok = true
	return nil
}

func syncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = d.Sync()
	_ = d.Close()
}

func (f AtomicJSON) Delete() error {
	err := os.Remove(f.Path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
