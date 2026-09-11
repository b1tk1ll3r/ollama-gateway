package haresource

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// Snapshot is a point-in-time view of gateway-process and host resources.
// Fields that cannot be collected on the current platform are omitted from JSON.
type Snapshot struct {
	CapturedAt           time.Time `json:"captured_at"`
	GOOS                 string    `json:"goos"`
	PID                  int       `json:"pid"`
	ProcessRSSBytes      int64     `json:"process_rss_bytes,omitempty"`
	ProcessCPUPercent    float64   `json:"process_cpu_percent,omitempty"`
	ProcessThreads       int64     `json:"process_threads,omitempty"`
	ProcessOpenFDs       int64     `json:"process_open_fds,omitempty"`
	HostLogicalCPUs      int       `json:"host_logical_cpus"`
	HostMemoryTotalBytes int64     `json:"host_memory_total_bytes,omitempty"`
	HostMemoryAvailBytes int64     `json:"host_memory_available_bytes,omitempty"`
	HostLoad1            float64   `json:"host_load1,omitempty"`
	HostLoad5            float64   `json:"host_load5,omitempty"`
	HostLoad15           float64   `json:"host_load15,omitempty"`
	Warnings             []string  `json:"warnings,omitempty"`
}

// Collect captures one resource snapshot for pid without modifying the process.
func Collect(pid int) Snapshot {
	s := Snapshot{CapturedAt: time.Now().UTC(), GOOS: runtime.GOOS, PID: pid, HostLogicalCPUs: runtime.NumCPU()}
	if rss, cpu, err := psProcess(pid); err == nil {
		s.ProcessRSSBytes = rss
		s.ProcessCPUPercent = cpu
	} else {
		s.Warnings = append(s.Warnings, "ps: "+err.Error())
	}
	if runtime.GOOS == "linux" {
		if b, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid)); err == nil {
			vmRSS, threads := ParseProcStatus(string(b))
			if vmRSS > 0 {
				s.ProcessRSSBytes = vmRSS
			}
			s.ProcessThreads = threads
		} else {
			s.Warnings = append(s.Warnings, "proc status: "+err.Error())
		}
		if entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid)); err == nil {
			s.ProcessOpenFDs = int64(len(entries))
		} else {
			s.Warnings = append(s.Warnings, "proc fd: "+err.Error())
		}
		if b, err := os.ReadFile("/proc/meminfo"); err == nil {
			s.HostMemoryTotalBytes, s.HostMemoryAvailBytes = ParseMeminfo(string(b))
		}
		if b, err := os.ReadFile("/proc/loadavg"); err == nil {
			s.HostLoad1, s.HostLoad5, s.HostLoad15 = ParseLoadavg(string(b))
		}
	}
	return s
}

func psProcess(pid int) (rssBytes int64, cpuPercent float64, err error) {
	cmd := exec.Command("ps", "-o", "rss=", "-o", "%cpu=", "-p", strconv.Itoa(pid))
	b, err := cmd.Output()
	if err != nil {
		return 0, 0, err
	}
	fields := strings.Fields(string(b))
	if len(fields) < 2 {
		return 0, 0, fmt.Errorf("unexpected ps output %q", strings.TrimSpace(string(b)))
	}
	rssKB, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	cpu, err := strconv.ParseFloat(strings.ReplaceAll(fields[1], ",", "."), 64)
	if err != nil {
		return 0, 0, err
	}
	return rssKB * 1024, cpu, nil
}

// ParseProcStatus extracts VmRSS and Threads from Linux /proc/<pid>/status text.
func ParseProcStatus(s string) (rssBytes, threads int64) {
	scan := bufio.NewScanner(strings.NewReader(s))
	for scan.Scan() {
		line := strings.TrimSpace(scan.Text())
		if strings.HasPrefix(line, "VmRSS:") {
			f := strings.Fields(line)
			if len(f) >= 2 {
				n, _ := strconv.ParseInt(f[1], 10, 64)
				rssBytes = n * 1024
			}
		}
		if strings.HasPrefix(line, "Threads:") {
			f := strings.Fields(line)
			if len(f) >= 2 {
				threads, _ = strconv.ParseInt(f[1], 10, 64)
			}
		}
	}
	return rssBytes, threads
}

// ParseMeminfo extracts MemTotal and MemAvailable from Linux /proc/meminfo text.
func ParseMeminfo(s string) (total, available int64) {
	for _, line := range strings.Split(s, "\n") {
		f := strings.Fields(line)
		if len(f) < 2 {
			continue
		}
		n, _ := strconv.ParseInt(f[1], 10, 64)
		switch strings.TrimSuffix(f[0], ":") {
		case "MemTotal":
			total = n * 1024
		case "MemAvailable":
			available = n * 1024
		}
	}
	return total, available
}

// ParseLoadavg extracts the 1, 5 and 15 minute load averages.
func ParseLoadavg(s string) (one, five, fifteen float64) {
	f := strings.Fields(s)
	if len(f) > 0 {
		one, _ = strconv.ParseFloat(f[0], 64)
	}
	if len(f) > 1 {
		five, _ = strconv.ParseFloat(f[1], 64)
	}
	if len(f) > 2 {
		fifteen, _ = strconv.ParseFloat(f[2], 64)
	}
	return
}
