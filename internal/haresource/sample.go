package haresource

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const SamplingSchemaVersion = 1

// SamplingReport is a bounded sustained-resource trace around one benchmark level.
type SamplingReport struct {
	Version                     int        `json:"version"`
	PID                         int        `json:"pid"`
	GOOS                        string     `json:"goos"`
	StartedAt                   time.Time  `json:"started_at"`
	FinishedAt                  time.Time  `json:"finished_at"`
	IntervalMS                  int64      `json:"interval_ms"`
	CPUMethod                   string     `json:"cpu_method"`
	Samples                     []Snapshot `json:"samples"`
	PeakProcessRSSBytes         int64      `json:"peak_process_rss_bytes,omitempty"`
	PeakProcessCPUPercent       float64    `json:"peak_process_cpu_percent,omitempty"`
	PeakProcessThreads          int64      `json:"peak_process_threads,omitempty"`
	PeakProcessOpenFDs          int64      `json:"peak_process_open_fds,omitempty"`
	MinHostMemoryAvailableBytes int64      `json:"min_host_memory_available_bytes,omitempty"`
	PeakHostLoad1               float64    `json:"peak_host_load1,omitempty"`
	ProcessExited               bool       `json:"process_exited,omitempty"`
	StopReason                  string     `json:"stop_reason"`
	Warnings                    []string   `json:"warnings,omitempty"`
}

// Complete reports whether the trace is suitable as sustained resource evidence.
func (r SamplingReport) Complete() bool {
	return r.Version == SamplingSchemaVersion && r.PID > 0 && len(r.Samples) >= 2 && !r.ProcessExited && r.PeakProcessRSSBytes > 0 && r.StopReason == "stop-file"
}

type cpuCounters struct {
	process uint64
	total   uint64
}

// Sample collects until stopFile appears, maxDuration expires, or ctx is canceled.
// stopFile is polled so shell wrappers do not need to forward signals reliably.
func Sample(ctx context.Context, pid int, interval, maxDuration time.Duration, stopFile string) (SamplingReport, error) {
	if pid <= 0 {
		return SamplingReport{}, errors.New("pid must be positive")
	}
	if interval < 50*time.Millisecond {
		return SamplingReport{}, errors.New("interval must be at least 50ms")
	}
	if maxDuration <= 0 {
		return SamplingReport{}, errors.New("max duration must be positive")
	}
	if strings.TrimSpace(stopFile) == "" {
		return SamplingReport{}, errors.New("stop file is required")
	}
	if _, err := os.Stat(stopFile); err == nil {
		return SamplingReport{}, fmt.Errorf("stop file already exists: %s", stopFile)
	} else if !errors.Is(err, os.ErrNotExist) {
		return SamplingReport{}, fmt.Errorf("check stop file: %w", err)
	}

	r := SamplingReport{
		Version:    SamplingSchemaVersion,
		PID:        pid,
		GOOS:       runtime.GOOS,
		StartedAt:  time.Now().UTC(),
		IntervalMS: interval.Milliseconds(),
		CPUMethod:  "ps_process_percent_sample",
	}
	var prev *cpuCounters
	if runtime.GOOS == "linux" {
		if c, err := readLinuxCPUCounters(pid); err == nil {
			prev = &c
			r.CPUMethod = "linux_procfs_interval_all_cpus_percent"
		} else {
			r.Warnings = append(r.Warnings, "initial procfs cpu counters: "+err.Error())
		}
	}

	collectOne := func() bool {
		s := Collect(pid)
		if runtime.GOOS == "linux" && prev != nil {
			if cur, err := readLinuxCPUCounters(pid); err == nil {
				if cur.total > prev.total && cur.process >= prev.process {
					dProc := float64(cur.process - prev.process)
					dTotal := float64(cur.total - prev.total)
					s.ProcessCPUPercent = (dProc / dTotal) * float64(max(1, s.HostLogicalCPUs)) * 100
				}
				*prev = cur
			} else {
				r.Warnings = append(r.Warnings, "procfs cpu counters: "+err.Error())
				if errors.Is(err, os.ErrNotExist) {
					r.ProcessExited = true
				}
			}
		}
		if processMissing(s) {
			r.ProcessExited = true
		}
		r.Samples = append(r.Samples, s)
		updatePeaks(&r, s)
		return !r.ProcessExited
	}

	if !collectOne() {
		r.StopReason = "process-exited"
		r.FinishedAt = time.Now().UTC()
		return r, nil
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	deadline := time.NewTimer(maxDuration)
	defer deadline.Stop()
	for {
		select {
		case <-ctx.Done():
			r.StopReason = "context-canceled"
			r.FinishedAt = time.Now().UTC()
			return r, nil
		case <-deadline.C:
			_ = collectOne()
			r.StopReason = "max-duration"
			r.FinishedAt = time.Now().UTC()
			return r, nil
		case <-ticker.C:
			if _, err := os.Stat(stopFile); err == nil {
				_ = collectOne()
				r.StopReason = "stop-file"
				r.FinishedAt = time.Now().UTC()
				return r, nil
			} else if !errors.Is(err, os.ErrNotExist) {
				r.Warnings = append(r.Warnings, "check stop file: "+err.Error())
			}
			if !collectOne() {
				r.StopReason = "process-exited"
				r.FinishedAt = time.Now().UTC()
				return r, nil
			}
		}
	}
}

func updatePeaks(r *SamplingReport, s Snapshot) {
	if s.ProcessRSSBytes > r.PeakProcessRSSBytes {
		r.PeakProcessRSSBytes = s.ProcessRSSBytes
	}
	if s.ProcessCPUPercent > r.PeakProcessCPUPercent {
		r.PeakProcessCPUPercent = s.ProcessCPUPercent
	}
	if s.ProcessThreads > r.PeakProcessThreads {
		r.PeakProcessThreads = s.ProcessThreads
	}
	if s.ProcessOpenFDs > r.PeakProcessOpenFDs {
		r.PeakProcessOpenFDs = s.ProcessOpenFDs
	}
	if s.HostMemoryAvailBytes > 0 && (r.MinHostMemoryAvailableBytes == 0 || s.HostMemoryAvailBytes < r.MinHostMemoryAvailableBytes) {
		r.MinHostMemoryAvailableBytes = s.HostMemoryAvailBytes
	}
	if s.HostLoad1 > r.PeakHostLoad1 {
		r.PeakHostLoad1 = s.HostLoad1
	}
}

func processMissing(s Snapshot) bool {
	if runtime.GOOS == "linux" {
		for _, w := range s.Warnings {
			if strings.HasPrefix(w, "proc status:") && (strings.Contains(w, "no such file") || strings.Contains(w, "not found")) {
				return true
			}
		}
	}
	return false
}

func readLinuxCPUCounters(pid int) (cpuCounters, error) {
	proc, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return cpuCounters{}, err
	}
	procTicks, err := ParseProcStatTicks(string(proc))
	if err != nil {
		return cpuCounters{}, err
	}
	host, err := os.ReadFile("/proc/stat")
	if err != nil {
		return cpuCounters{}, err
	}
	totalTicks, err := ParseHostCPUTicks(string(host))
	if err != nil {
		return cpuCounters{}, err
	}
	return cpuCounters{process: procTicks, total: totalTicks}, nil
}

// ParseProcStatTicks returns utime+stime from Linux /proc/<pid>/stat.
func ParseProcStatTicks(s string) (uint64, error) {
	end := strings.LastIndexByte(strings.TrimSpace(s), ')')
	if end < 0 || end+2 >= len(s) {
		return 0, errors.New("malformed proc stat")
	}
	fields := strings.Fields(s[end+1:])
	// fields[0] is field 3 (state); utime/stime are fields 14/15.
	if len(fields) <= 12 {
		return 0, errors.New("proc stat has too few fields")
	}
	utime, err := strconv.ParseUint(fields[11], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse utime: %w", err)
	}
	stime, err := strconv.ParseUint(fields[12], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse stime: %w", err)
	}
	return utime + stime, nil
}

// ParseHostCPUTicks sums the aggregate Linux /proc/stat cpu line.
func ParseHostCPUTicks(s string) (uint64, error) {
	line, _, _ := strings.Cut(s, "\n")
	fields := strings.Fields(line)
	if len(fields) < 2 || fields[0] != "cpu" {
		return 0, errors.New("missing aggregate cpu line")
	}
	var total uint64
	for _, field := range fields[1:] {
		n, err := strconv.ParseUint(field, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse host cpu tick: %w", err)
		}
		total += n
	}
	return total, nil
}
