package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/example/ollama-fair-gateway/internal/haresource"
)

type durationStats struct {
	P50 float64 `json:"p50_ms"`
	P95 float64 `json:"p95_ms"`
	P99 float64 `json:"p99_ms"`
	Max float64 `json:"max_ms"`
}

type benchSummary struct {
	StartedAt       time.Time      `json:"started_at"`
	FinishedAt      time.Time      `json:"finished_at"`
	BaseURL         string         `json:"base_url"`
	Model           string         `json:"model"`
	Concurrency     int            `json:"concurrency"`
	Requests        int            `json:"requests"`
	WarmupRequests  int            `json:"warmup_requests"`
	Successful      int            `json:"successful"`
	Errors          int            `json:"errors"`
	StatusCounts    map[string]int `json:"status_counts"`
	WallMS          float64        `json:"wall_ms"`
	ThroughputRPS   float64        `json:"throughput_rps"`
	Latency         durationStats  `json:"latency"`
	TTFB            durationStats  `json:"ttfb"`
	ServiceClass    string         `json:"service_class,omitempty"`
	ErrorsByMessage map[string]int `json:"errors_by_message,omitempty"`
}

type metricSample struct {
	Name   string
	Labels string
	Value  float64
}

type resourceSnapshot = haresource.Snapshot
type resourceSamplingReport = haresource.SamplingReport

type levelReport struct {
	Concurrency              int                     `json:"concurrency"`
	Requests                 int                     `json:"requests"`
	WarmupRequests           int                     `json:"warmup_requests"`
	Successful               int                     `json:"successful"`
	Errors                   int                     `json:"errors"`
	ThroughputRPS            float64                 `json:"throughput_rps"`
	Latency                  durationStats           `json:"latency"`
	TTFB                     durationStats           `json:"ttfb"`
	GatewayRequestsDelta     float64                 `json:"gateway_requests_delta"`
	Gateway2xxDelta          float64                 `json:"gateway_2xx_delta"`
	GatewayErrorDelta        float64                 `json:"gateway_error_delta"`
	RetriesDelta             float64                 `json:"retries_delta"`
	CircuitOpensDelta        float64                 `json:"circuit_opens_delta"`
	PromptTokensDelta        float64                 `json:"prompt_tokens_delta"`
	CompletionTokensDelta    float64                 `json:"completion_tokens_delta"`
	QueueObservationsDelta   float64                 `json:"queue_observations_delta"`
	ServiceObservationsDelta float64                 `json:"service_observations_delta"`
	AccountingMatches        bool                    `json:"accounting_matches"`
	ResourcesBefore          *resourceSnapshot       `json:"resources_before,omitempty"`
	ResourcesAfter           *resourceSnapshot       `json:"resources_after,omitempty"`
	ResourceSamples          *resourceSamplingReport `json:"resource_samples,omitempty"`
}

type evidenceReport struct {
	GeneratedAt                       time.Time     `json:"generated_at"`
	InputDir                          string        `json:"input_dir"`
	Levels                            []levelReport `json:"levels"`
	EvidenceComplete                  bool          `json:"evidence_complete"`
	ResourceEvidenceComplete          bool          `json:"resource_evidence_complete"`
	SustainedResourceEvidenceComplete bool          `json:"sustained_resource_evidence_complete"`
	GateStatus                        string        `json:"gate_status"`
	GateReason                        string        `json:"gate_reason"`
}

func main() {
	input := flag.String("input", "", "HA-readiness result directory")
	jsonOut := flag.String("json-out", "", "JSON report path; defaults to <input>/report.json")
	markdownOut := flag.String("markdown-out", "", "Markdown report path; defaults to <input>/report.md")
	flag.Parse()
	if *input == "" {
		fmt.Fprintln(os.Stderr, "-input is required")
		os.Exit(2)
	}
	if *jsonOut == "" {
		*jsonOut = filepath.Join(*input, "report.json")
	}
	if *markdownOut == "" {
		*markdownOut = filepath.Join(*input, "report.md")
	}
	r, err := buildReport(*input)
	if err != nil {
		fmt.Fprintln(os.Stderr, "build report:", err)
		os.Exit(1)
	}
	if err := writeJSON(*jsonOut, r); err != nil {
		fmt.Fprintln(os.Stderr, "write JSON report:", err)
		os.Exit(1)
	}
	if err := os.WriteFile(*markdownOut, []byte(renderMarkdown(r)), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "write Markdown report:", err)
		os.Exit(1)
	}
	fmt.Printf("report: %s\nreport: %s\ngate: %s — %s\n", *jsonOut, *markdownOut, r.GateStatus, r.GateReason)
}

func buildReport(dir string) (evidenceReport, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "concurrency-*.json"))
	if err != nil {
		return evidenceReport{}, err
	}
	if len(matches) == 0 {
		return evidenceReport{}, errors.New("no concurrency-*.json files found")
	}
	levels := make([]levelReport, 0, len(matches))
	complete := true
	resourceComplete := true
	resourceSeen := false
	sustainedComplete := true
	sustainedSeen := false
	for _, p := range matches {
		b, err := os.ReadFile(p)
		if err != nil {
			return evidenceReport{}, err
		}
		var s benchSummary
		if err := json.Unmarshal(b, &s); err != nil {
			return evidenceReport{}, fmt.Errorf("%s: %w", p, err)
		}
		beforePath := filepath.Join(dir, fmt.Sprintf("metrics-before-c%d.prom", s.Concurrency))
		afterPath := filepath.Join(dir, fmt.Sprintf("metrics-after-c%d.prom", s.Concurrency))
		before, errBefore := parsePrometheusFile(beforePath)
		after, errAfter := parsePrometheusFile(afterPath)
		if errBefore != nil || errAfter != nil {
			complete = false
		}
		l := levelReport{
			Concurrency: s.Concurrency, Requests: s.Requests, WarmupRequests: s.WarmupRequests,
			Successful: s.Successful, Errors: s.Errors, ThroughputRPS: s.ThroughputRPS,
			Latency: s.Latency, TTFB: s.TTFB,
		}
		resourceBefore, errResourceBefore := readResourceSnapshot(filepath.Join(dir, fmt.Sprintf("resources-before-c%d.json", s.Concurrency)))
		resourceAfter, errResourceAfter := readResourceSnapshot(filepath.Join(dir, fmt.Sprintf("resources-after-c%d.json", s.Concurrency)))
		if errResourceBefore == nil && errResourceAfter == nil {
			l.ResourcesBefore = &resourceBefore
			l.ResourcesAfter = &resourceAfter
			resourceSeen = true
		} else {
			resourceComplete = false
		}
		resourceSamples, errResourceSamples := readResourceSamples(filepath.Join(dir, fmt.Sprintf("resources-samples-c%d.json", s.Concurrency)))
		if errResourceSamples == nil && resourceSamples.Complete() {
			l.ResourceSamples = &resourceSamples
			sustainedSeen = true
		} else {
			sustainedComplete = false
		}
		if errBefore == nil && errAfter == nil {
			l.GatewayRequestsDelta = deltaByName(before, after, "ollama_gateway_requests_total")
			l.Gateway2xxDelta = deltaByNameLabelContains(before, after, "ollama_gateway_requests_total", `status_class="2xx"`)
			l.GatewayErrorDelta = deltaByName(before, after, "ollama_gateway_errors_total")
			// Older/current builds expose errors via requests_total status class rather than a dedicated counter.
			if l.GatewayErrorDelta == 0 {
				l.GatewayErrorDelta = l.GatewayRequestsDelta - l.Gateway2xxDelta
			}
			l.RetriesDelta = deltaByName(before, after, "ollama_gateway_retries_total")
			l.CircuitOpensDelta = deltaByName(before, after, "ollama_gateway_circuit_opens_total")
			l.PromptTokensDelta = deltaByName(before, after, "ollama_gateway_prompt_tokens_total")
			l.CompletionTokensDelta = deltaByName(before, after, "ollama_gateway_completion_tokens_total")
			l.QueueObservationsDelta = deltaByName(before, after, "ollama_gateway_queue_seconds_count")
			l.ServiceObservationsDelta = deltaByName(before, after, "ollama_gateway_service_seconds_count")
			expected := float64(s.Requests + s.WarmupRequests)
			l.AccountingMatches = almostEqual(l.GatewayRequestsDelta, expected) && almostEqual(l.QueueObservationsDelta, expected) && almostEqual(l.ServiceObservationsDelta, expected)
			if !l.AccountingMatches {
				complete = false
			}
		}
		levels = append(levels, l)
	}
	sort.Slice(levels, func(i, j int) bool { return levels[i].Concurrency < levels[j].Concurrency })
	if !resourceSeen {
		resourceComplete = false
	}
	if !sustainedSeen {
		sustainedComplete = false
	}
	r := evidenceReport{
		GeneratedAt: time.Now().UTC(), InputDir: filepath.Clean(dir), Levels: levels,
		EvidenceComplete: complete, ResourceEvidenceComplete: resourceComplete,
		SustainedResourceEvidenceComplete: sustainedComplete,
	}
	if !complete {
		r.GateStatus = "incomplete"
		r.GateReason = "benchmark and gateway metrics evidence are missing or do not reconcile"
	} else if sustainedComplete {
		r.GateStatus = "not-proven"
		r.GateReason = "client/gateway accounting reconciles and sustained host/process resource sampling is complete; HA still requires an operator-demonstrated capacity, availability, or topology need"
	} else if resourceComplete {
		r.GateStatus = "not-proven"
		r.GateReason = "client/gateway accounting reconciles and before/after host/process resource snapshots are present; sustained peak resource evidence is incomplete, and HA still requires an operator-demonstrated capacity, availability, or topology need"
	} else {
		r.GateStatus = "not-proven"
		r.GateReason = "client and gateway counters reconcile; host/process resource evidence is incomplete, and HA still requires demonstrated capacity, availability, or topology need"
	}
	return r, nil
}

func readResourceSnapshot(path string) (resourceSnapshot, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return resourceSnapshot{}, err
	}
	var s resourceSnapshot
	if err := json.Unmarshal(b, &s); err != nil {
		return resourceSnapshot{}, err
	}
	if s.PID <= 0 {
		return resourceSnapshot{}, fmt.Errorf("%s: invalid pid", path)
	}
	return s, nil
}

func readResourceSamples(path string) (resourceSamplingReport, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return resourceSamplingReport{}, err
	}
	var r resourceSamplingReport
	if err := json.Unmarshal(b, &r); err != nil {
		return resourceSamplingReport{}, err
	}
	if !r.Complete() {
		return r, fmt.Errorf("%s: incomplete sustained resource report", path)
	}
	return r, nil
}

func parsePrometheusFile(path string) ([]metricSample, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []metricSample
	s := bufio.NewScanner(f)
	buf := make([]byte, 64*1024)
	s.Buffer(buf, 4*1024*1024)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		v, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			continue
		}
		key := parts[0]
		name, labels := key, ""
		if i := strings.IndexByte(key, '{'); i >= 0 {
			name = key[:i]
			labels = strings.TrimSuffix(key[i+1:], "}")
		}
		out = append(out, metricSample{Name: name, Labels: labels, Value: v})
	}
	return out, s.Err()
}

func deltaByName(before, after []metricSample, name string) float64 {
	return sumBy(before, name, "", false, after)
}

func deltaByNameLabelContains(before, after []metricSample, name, label string) float64 {
	return sumBy(before, name, label, true, after)
}

func sumBy(before []metricSample, name, label string, filter bool, after []metricSample) float64 {
	sum := func(xs []metricSample) float64 {
		var n float64
		for _, x := range xs {
			if x.Name != name {
				continue
			}
			if filter && !strings.Contains(x.Labels, label) {
				continue
			}
			n += x.Value
		}
		return n
	}
	return sum(after) - sum(before)
}

func almostEqual(a, b float64) bool {
	d := a - b
	if d < 0 {
		d = -d
	}
	return d < 0.000001
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(path, b, 0o644)
}

func renderMarkdown(r evidenceReport) string {
	var b strings.Builder
	fmt.Fprintln(&b, "# HA readiness evidence report")
	fmt.Fprintln(&b)
	fmt.Fprintf(&b, "Generated: `%s`  \n", r.GeneratedAt.Format(time.RFC3339))
	fmt.Fprintf(&b, "Gateway accounting complete: **%t**  \n", r.EvidenceComplete)
	fmt.Fprintf(&b, "Resource snapshots complete: **%t**  \n", r.ResourceEvidenceComplete)
	fmt.Fprintf(&b, "Sustained resource sampling complete: **%t**  \n", r.SustainedResourceEvidenceComplete)
	fmt.Fprintf(&b, "P3.2 gate: **%s** — %s\n\n", r.GateStatus, r.GateReason)
	fmt.Fprintln(&b, "| Concurrency | Success/Error | req/s | p95 latency | p95 TTFB | Gateway requests Δ | 2xx Δ | retries Δ | circuit opens Δ | counters reconcile | after RSS | sampled peak RSS | sampled peak CPU | sampled peak FDs | samples |")
	fmt.Fprintln(&b, "|---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|---:|---:|---:|---:|---:|")
	for _, l := range r.Levels {
		afterRSS, _, _, _, _ := resourceCells(l.ResourcesAfter)
		peakRSS, peakCPU, peakFDs, sampleCount := sustainedCells(l.ResourceSamples)
		fmt.Fprintf(&b, "| %d | %d/%d | %.2f | %.2f ms | %.2f ms | %.0f | %.0f | %.0f | %.0f | %t | %s | %s | %s | %s | %s |\n", l.Concurrency, l.Successful, l.Errors, l.ThroughputRPS, l.Latency.P95, l.TTFB.P95, l.GatewayRequestsDelta, l.Gateway2xxDelta, l.RetriesDelta, l.CircuitOpensDelta, l.AccountingMatches, afterRSS, peakRSS, peakCPU, peakFDs, sampleCount)
	}
	fmt.Fprintln(&b, "\n## Interpretation")
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "This report verifies that client-side benchmark counts reconcile with gateway-side Prometheus counters. Before/after resource files remain endpoint evidence. When `resources-samples-cN.json` is present and complete, the sampled peak columns come from measurements taken throughout that benchmark level; on Linux process CPU is calculated from `/proc` process/host tick deltas and may exceed 100% when multiple logical CPUs are used.")
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "The report intentionally does not prove that HA is required. Correlate sustained process/host evidence with worker/GPU saturation and an explicit capacity, availability, or topology requirement before changing the P3.2 gate.")
	return b.String()
}

func resourceCells(s *resourceSnapshot) (rss, cpu, fds, threads, load1 string) {
	if s == nil {
		return "-", "-", "-", "-", "-"
	}
	if s.ProcessRSSBytes > 0 {
		rss = fmt.Sprintf("%.1f MiB", float64(s.ProcessRSSBytes)/(1024*1024))
	} else {
		rss = "-"
	}
	if s.ProcessCPUPercent > 0 {
		cpu = fmt.Sprintf("%.1f%%", s.ProcessCPUPercent)
	} else {
		cpu = "0.0%"
	}
	if s.ProcessOpenFDs > 0 {
		fds = strconv.FormatInt(s.ProcessOpenFDs, 10)
	} else {
		fds = "-"
	}
	if s.ProcessThreads > 0 {
		threads = strconv.FormatInt(s.ProcessThreads, 10)
	} else {
		threads = "-"
	}
	load1 = fmt.Sprintf("%.2f", s.HostLoad1)
	return
}

func sustainedCells(r *resourceSamplingReport) (rss, cpu, fds, samples string) {
	if r == nil {
		return "-", "-", "-", "-"
	}
	if r.PeakProcessRSSBytes > 0 {
		rss = fmt.Sprintf("%.1f MiB", float64(r.PeakProcessRSSBytes)/(1024*1024))
	} else {
		rss = "-"
	}
	cpu = fmt.Sprintf("%.1f%%", r.PeakProcessCPUPercent)
	if r.PeakProcessOpenFDs > 0 {
		fds = strconv.FormatInt(r.PeakProcessOpenFDs, 10)
	} else {
		fds = "-"
	}
	samples = strconv.Itoa(len(r.Samples))
	return
}
