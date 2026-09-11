package server

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/example/ollama-fair-gateway/internal/state"
)

type persistentSaver interface {
	SavePersistent(string) error
}

type storageFileStatus struct {
	Name     string    `json:"name"`
	Kind     string    `json:"kind"`
	Path     string    `json:"path"`
	Exists   bool      `json:"exists"`
	Size     int64     `json:"size_bytes,omitempty"`
	Modified time.Time `json:"modified_at,omitempty"`
}

func fileStatus(name, kind, path string) storageFileStatus {
	x := storageFileStatus{Name: name, Kind: kind, Path: path}
	st, err := os.Stat(path)
	if err == nil && st.Mode().IsRegular() {
		x.Exists = true
		x.Size = st.Size()
		x.Modified = st.ModTime().UTC()
	}
	return x
}

func (s *Server) storageStatus() map[string]any {
	paths := state.Resolve(s.cfg.Storage)
	files := []storageFileStatus{
		fileStatus("configuration override", "config", paths.Config),
		fileStatus("API keys", "security", paths.APIKeys),
		fileStatus("tenant policies", "policy", paths.Policies),
		fileStatus("metrics snapshot", "metrics", paths.Metrics),
		fileStatus("quota buckets", "quota", paths.Quota),
		fileStatus("worker performance", "routing", paths.WorkerPerformance),
		fileStatus("model placement", "routing", paths.ModelPlacement),
		fileStatus("worker runtime state", "routing", paths.WorkerState),
		fileStatus("warm model policies", "capacity", paths.WarmModels),
		fileStatus("alerts history", "alerts", paths.Alerts),
		fileStatus("encrypted conversations", "content", paths.Conversations),
		fileStatus("durable batch jobs", "batch", paths.BatchJobs),
	}
	usageDir := s.cfg.Usage.JournalDir
	usageFiles := 0
	var usageBytes int64
	var usageNewest time.Time
	if usageDir != "" {
		matches, _ := filepath.Glob(filepath.Join(usageDir, "usage-*.jsonl"))
		for _, p := range matches {
			if st, err := os.Stat(p); err == nil && st.Mode().IsRegular() {
				usageFiles++
				usageBytes += st.Size()
				if st.ModTime().After(usageNewest) {
					usageNewest = st.ModTime().UTC()
				}
			}
		}
	}
	batchFiles := 0
	var batchBytes int64
	var batchNewest time.Time
	_ = filepath.Walk(paths.BatchDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() {
			batchFiles++
			batchBytes += info.Size()
			if info.ModTime().After(batchNewest) {
				batchNewest = info.ModTime().UTC()
			}
		}
		return nil
	})
	retention := s.usage.RetentionStatus()
	var total int64
	for _, f := range files {
		total += f.Size
	}
	total += usageBytes + retention.DailyBytes + retention.MonthlyBytes + batchBytes
	return map[string]any{
		"mode":                   "local-persistent",
		"data_dir":               paths.DataDir,
		"flush_interval":         s.cfg.Storage.FlushInterval.Value().String(),
		"config_override_active": s.configStore != nil && fileStatus("", "", paths.Config).Exists,
		"files":                  files,
		"usage": map[string]any{
			"directory":   usageDir,
			"files":       usageFiles,
			"size_bytes":  usageBytes,
			"modified_at": usageNewest,
			"retention":   retention,
		},
		"batch": map[string]any{
			"enabled":     s.batchJobs != nil && s.batchJobs.Enabled(),
			"directory":   paths.BatchDir,
			"files":       batchFiles,
			"size_bytes":  batchBytes,
			"modified_at": batchNewest,
		},
		"conversations": func() any {
			if s.conversations == nil {
				return map[string]any{"enabled": false}
			}
			return s.conversations.Status()
		}(),
		"total_bytes": total,
		"volatile": []string{
			"active transient inference jobs and cancellation handles",
			"active durable-batch attempt contexts (batch metadata remains persistent)",
			"fair-queue heap and virtual clocks",
			"active worker/model slots",
			"browser OIDC sessions",
			"live-flow animation state",
		},
	}
}

func (s *Server) flushPersistentState(ctx context.Context) error {
	paths := state.Resolve(s.cfg.Storage)
	var errs []string
	if err := s.metrics.SavePersistent(paths.Metrics); err != nil {
		errs = append(errs, "metrics: "+err.Error())
	}
	if saver, ok := s.quota.(persistentSaver); ok {
		if err := saver.SavePersistent(paths.Quota); err != nil {
			errs = append(errs, "quota: "+err.Error())
		}
	}
	if err := s.workers.SavePerformance(paths.WorkerPerformance); err != nil {
		errs = append(errs, "worker performance: "+err.Error())
	}
	if err := s.usage.Flush(ctx); err != nil {
		errs = append(errs, "usage: "+err.Error())
	}
	if s.conversations != nil && s.conversations.Enabled() {
		if err := s.conversations.Compact(); err != nil {
			errs = append(errs, "conversations: "+err.Error())
		}
	}
	if s.batchJobs != nil && s.batchJobs.Enabled() {
		if err := s.batchJobs.Compact(); err != nil {
			errs = append(errs, "batch jobs: "+err.Error())
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("persistent flush failed: %s", strings.Join(errs, "; "))
	}
	return nil
}

func (s *Server) uiStorageFlush(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := s.flushPersistentState(ctx); err != nil {
		writeProtocolError(w, r, http.StatusServiceUnavailable, "storage_flush", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"flushed": true, "at": time.Now().UTC(), "storage": s.storageStatus()})
}

func (s *Server) uiStorageCompact(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Minute)
	defer cancel()
	if err := s.flushPersistentState(ctx); err != nil {
		writeProtocolError(w, r, http.StatusServiceUnavailable, "storage_flush", err.Error())
		return
	}
	status, err := s.usage.Compact(ctx)
	if err != nil {
		writeProtocolError(w, r, http.StatusInternalServerError, "usage_compaction", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"compacted": true, "at": time.Now().UTC(), "retention": status, "storage": s.storageStatus()})
}

func (s *Server) uiStorageBackup(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := s.flushPersistentState(ctx); err != nil {
		writeProtocolError(w, r, http.StatusServiceUnavailable, "storage_flush", err.Error())
		return
	}
	name := "ollama-gateway-backup-" + time.Now().UTC().Format("20060102-150405") + ".zip"
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", `attachment; filename="`+name+`"`)
	w.Header().Set("Cache-Control", "no-store")
	zw := zip.NewWriter(w)
	defer zw.Close()

	paths := state.Resolve(s.cfg.Storage)
	known := []struct{ archive, path string }{
		{"state/" + filepath.Base(paths.Config), paths.Config},
		{"state/" + filepath.Base(paths.APIKeys), paths.APIKeys},
		{"state/" + filepath.Base(paths.Policies), paths.Policies},
		{"state/" + filepath.Base(paths.Metrics), paths.Metrics},
		{"state/" + filepath.Base(paths.Quota), paths.Quota},
		{"state/" + filepath.Base(paths.WorkerPerformance), paths.WorkerPerformance},
		{"state/" + filepath.Base(paths.ModelPlacement), paths.ModelPlacement},
		{"state/" + filepath.Base(paths.WorkerState), paths.WorkerState},
		{"state/" + filepath.Base(paths.WarmModels), paths.WarmModels},
		{"state/" + filepath.Base(paths.Alerts), paths.Alerts},
		{"state/" + filepath.Base(paths.Conversations), paths.Conversations},
		{"state/" + filepath.Base(paths.BatchJobs), paths.BatchJobs},
	}
	for _, f := range known {
		if err := zipFile(zw, f.archive, f.path); err != nil && !os.IsNotExist(err) {
			return
		}
	}
	if paths.BatchDir != "" {
		var matches []string
		_ = filepath.Walk(paths.BatchDir, func(path string, info os.FileInfo, err error) error {
			if err == nil && info.Mode().IsRegular() {
				matches = append(matches, path)
			}
			return nil
		})
		sort.Strings(matches)
		for _, p := range matches {
			rel, err := filepath.Rel(paths.BatchDir, p)
			if err != nil {
				continue
			}
			if err := zipFile(zw, "batch/"+filepath.ToSlash(rel), p); err != nil && !os.IsNotExist(err) {
				return
			}
		}
	}
	if s.cfg.Usage.JournalDir != "" {
		var matches []string
		_ = filepath.Walk(s.cfg.Usage.JournalDir, func(path string, info os.FileInfo, err error) error {
			if err == nil && info.Mode().IsRegular() && (strings.HasSuffix(info.Name(), ".jsonl") || strings.HasSuffix(info.Name(), ".json")) {
				matches = append(matches, path)
			}
			return nil
		})
		sort.Strings(matches)
		for _, p := range matches {
			rel, err := filepath.Rel(s.cfg.Usage.JournalDir, p)
			if err != nil {
				continue
			}
			if err := zipFile(zw, "usage/"+filepath.ToSlash(rel), p); err != nil && !os.IsNotExist(err) {
				return
			}
		}
	}
}

func zipFile(zw *zip.Writer, archiveName, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if !st.Mode().IsRegular() {
		return nil
	}
	h, err := zip.FileInfoHeader(st)
	if err != nil {
		return err
	}
	h.Name = filepath.ToSlash(archiveName)
	h.Method = zip.Deflate
	dst, err := zw.CreateHeader(h)
	if err != nil {
		return err
	}
	_, err = io.Copy(dst, f)
	return err
}
