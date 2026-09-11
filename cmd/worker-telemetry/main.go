package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/example/ollama-fair-gateway/internal/hoststats"
)

type telemetry struct {
	MemoryUsedBytes   int64     `json:"memory_used_bytes,omitempty"`
	MemoryTotalBytes  int64     `json:"memory_total_bytes,omitempty"`
	VRAMUsedBytes     int64     `json:"vram_used_bytes,omitempty"`
	VRAMTotalBytes    int64     `json:"vram_total_bytes,omitempty"`
	GPUUtilizationPct float64   `json:"gpu_utilization_percent,omitempty"`
	GPUTemperatureC   float64   `json:"gpu_temperature_c,omitempty"`
	GPUPowerWatts     float64   `json:"gpu_power_watts,omitempty"`
	Source            string    `json:"source,omitempty"`
	UpdatedAt         time.Time `json:"updated_at,omitempty"`
	Error             string    `json:"error,omitempty"`
}

type collector struct {
	nvidia    bool
	nvidiaGPU string
	amd       bool
	amdDevice string
}

func (c collector) collect(ctx context.Context) telemetry {
	out := telemetry{UpdatedAt: time.Now().UTC()}
	var errs []string
	mctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	m, err := hoststats.ReadMemory(mctx)
	cancel()
	if err != nil {
		errs = append(errs, "memory: "+err.Error())
	} else {
		out.MemoryTotalBytes, out.MemoryUsedBytes = m.TotalBytes, m.UsedBytes
		out.Source = appendSource(out.Source, "host-memory")
	}
	if c.nvidia {
		gctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
		g, err := hoststats.ReadNVIDIA(gctx, c.nvidiaGPU)
		cancel()
		if err != nil {
			errs = append(errs, "nvidia: "+err.Error())
		} else {
			out.VRAMUsedBytes, out.VRAMTotalBytes = g.MemoryUsedBytes, g.MemoryTotalBytes
			out.GPUUtilizationPct, out.GPUTemperatureC, out.GPUPowerWatts = g.UtilizationPercent, g.TemperatureC, g.PowerWatts
			out.Source = appendSource(out.Source, "nvidia-smi")
		}
	}
	if c.amd {
		gctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
		g, err := hoststats.ReadAMD(gctx, c.amdDevice)
		cancel()
		if err != nil {
			errs = append(errs, "amd: "+err.Error())
		} else {
			out.VRAMUsedBytes, out.VRAMTotalBytes = g.MemoryUsedBytes, g.MemoryTotalBytes
			out.GPUUtilizationPct, out.GPUTemperatureC, out.GPUPowerWatts = g.UtilizationPercent, g.TemperatureC, g.PowerWatts
			out.Source = appendSource(out.Source, "amdgpu-sysfs")
		}
	}
	out.Error = strings.Join(errs, "; ")
	return out
}

func appendSource(cur, next string) string {
	if cur == "" {
		return next
	}
	return cur + "+" + next
}

type cidrAllowlist struct{ nets []*net.IPNet }

func parseCIDRs(raw string) (cidrAllowlist, error) {
	var out cidrAllowlist
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		_, n, err := net.ParseCIDR(part)
		if err != nil {
			return out, fmt.Errorf("invalid allow CIDR %q: %w", part, err)
		}
		out.nets = append(out.nets, n)
	}
	if len(out.nets) == 0 {
		return out, fmt.Errorf("at least one allow CIDR is required")
	}
	return out, nil
}

func (a cidrAllowlist) allowed(remote string) bool {
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		host = remote
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	if ip == nil {
		return false
	}
	for _, n := range a.nets {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

func main() {
	listen := flag.String("listen", "127.0.0.1:11500", "listen address")
	path := flag.String("path", "/telemetry", "telemetry path")
	allow := flag.String("allow-cidrs", "127.0.0.1/32,::1/128", "comma-separated client CIDRs allowed to read telemetry")
	nvidia := flag.Bool("nvidia-smi", false, "collect NVIDIA telemetry with nvidia-smi")
	nvidiaGPU := flag.String("nvidia-gpu", "", "optional nvidia-smi GPU selector")
	amd := flag.Bool("amd-sysfs", false, "collect Linux AMDGPU telemetry from sysfs")
	amdDevice := flag.String("amd-device", "", "optional AMDGPU device path such as /sys/class/drm/card0/device; empty auto-detects")
	once := flag.Bool("once", false, "print one telemetry sample as JSON and exit")
	flag.Parse()
	if !strings.HasPrefix(*path, "/") {
		log.Fatal("-path must begin with /")
	}
	acl, err := parseCIDRs(*allow)
	if err != nil {
		log.Fatal(err)
	}
	c := collector{nvidia: *nvidia, nvidiaGPU: *nvidiaGPU, amd: *amd, amdDevice: *amdDevice}
	if *once {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(c.collect(context.Background())); err != nil {
			log.Fatal(err)
		}
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc(*path, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if !acl.allowed(r.RemoteAddr) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		_ = json.NewEncoder(w).Encode(c.collect(r.Context()))
	})
	srv := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: time.Minute}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	log.Printf("worker telemetry listening on http://%s%s allowed=%s nvidia=%t amd=%t", *listen, *path, *allow, *nvidia, *amd)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
