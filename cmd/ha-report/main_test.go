package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildReportReconcilesWarmupAndMeasuredRequests(t *testing.T) {
	d := t.TempDir()
	s := benchSummary{Concurrency: 4, Requests: 20, WarmupRequests: 3, Successful: 20, ThroughputRPS: 123.4, Latency: durationStats{P95: 8}, TTFB: durationStats{P95: 3}}
	b, _ := json.Marshal(s)
	if err := os.WriteFile(filepath.Join(d, "concurrency-4.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
	before := `ollama_gateway_requests_total{api="openai",status_class="2xx"} 10
ollama_gateway_queue_seconds_count 10
ollama_gateway_service_seconds_count 10
ollama_gateway_prompt_tokens_total 100
ollama_gateway_completion_tokens_total 50
`
	after := `ollama_gateway_requests_total{api="openai",status_class="2xx"} 33
ollama_gateway_queue_seconds_count 33
ollama_gateway_service_seconds_count 33
ollama_gateway_prompt_tokens_total 330
ollama_gateway_completion_tokens_total 165
ollama_gateway_retries_total{worker="w"} 0
`
	if err := os.WriteFile(filepath.Join(d, "metrics-before-c4.prom"), []byte(before), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, "metrics-after-c4.prom"), []byte(after), 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := buildReport(d)
	if err != nil {
		t.Fatal(err)
	}
	if !r.EvidenceComplete || len(r.Levels) != 1 {
		t.Fatalf("unexpected report: %+v", r)
	}
	l := r.Levels[0]
	if l.GatewayRequestsDelta != 23 || !l.AccountingMatches {
		t.Fatalf("unexpected level: %+v", l)
	}
	if r.GateStatus != "not-proven" {
		t.Fatalf("gate=%q", r.GateStatus)
	}
	if got := renderMarkdown(r); !strings.Contains(got, "| 4 | 20/0 |") {
		t.Fatalf("markdown missing row: %s", got)
	}
}

func TestBuildReportMarksMissingMetricsIncomplete(t *testing.T) {
	d := t.TempDir()
	b, _ := json.Marshal(benchSummary{Concurrency: 1, Requests: 1, Successful: 1})
	if err := os.WriteFile(filepath.Join(d, "concurrency-1.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := buildReport(d)
	if err != nil {
		t.Fatal(err)
	}
	if r.EvidenceComplete || r.GateStatus != "incomplete" {
		t.Fatalf("unexpected report: %+v", r)
	}
}

func TestPrometheusParserUsesFullOllamaGatewayNames(t *testing.T) {
	p := filepath.Join(t.TempDir(), "m.prom")
	content := "# HELP x x\nollama_gateway_requests_total{api=\"openai\",status_class=\"2xx\"} 7\n"
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	xs, err := parsePrometheusFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if got := deltaByName(nil, xs, "ollama_gateway_requests_total"); got != 7 {
		t.Fatalf("got %v", got)
	}
	if got := deltaByName(nil, xs, "gateway_requests_total"); got != 0 {
		t.Fatalf("short metric name unexpectedly matched: %v", got)
	}
}

func TestBuildReportIncludesCompleteResourceSnapshots(t *testing.T) {
	d := t.TempDir()
	s := benchSummary{Concurrency: 8, Requests: 5, WarmupRequests: 2, Successful: 5, ThroughputRPS: 50, Latency: durationStats{P95: 10}, TTFB: durationStats{P95: 4}}
	b, _ := json.Marshal(s)
	if err := os.WriteFile(filepath.Join(d, "concurrency-8.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
	beforeMetrics := "ollama_gateway_requests_total{status_class=\"2xx\"} 10\nollama_gateway_queue_seconds_count 10\nollama_gateway_service_seconds_count 10\n"
	afterMetrics := "ollama_gateway_requests_total{status_class=\"2xx\"} 17\nollama_gateway_queue_seconds_count 17\nollama_gateway_service_seconds_count 17\n"
	if err := os.WriteFile(filepath.Join(d, "metrics-before-c8.prom"), []byte(beforeMetrics), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, "metrics-after-c8.prom"), []byte(afterMetrics), 0o600); err != nil {
		t.Fatal(err)
	}
	before := resourceSnapshot{PID: 4242, ProcessRSSBytes: 64 * 1024 * 1024, ProcessCPUPercent: 2.5, ProcessThreads: 8, ProcessOpenFDs: 21, HostLogicalCPUs: 8, HostLoad1: 0.5}
	after := resourceSnapshot{PID: 4242, ProcessRSSBytes: 96 * 1024 * 1024, ProcessCPUPercent: 33.3, ProcessThreads: 12, ProcessOpenFDs: 40, HostLogicalCPUs: 8, HostLoad1: 1.25}
	for name, snap := range map[string]resourceSnapshot{"resources-before-c8.json": before, "resources-after-c8.json": after} {
		data, _ := json.Marshal(snap)
		if err := os.WriteFile(filepath.Join(d, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	r, err := buildReport(d)
	if err != nil {
		t.Fatal(err)
	}
	if !r.EvidenceComplete || !r.ResourceEvidenceComplete {
		t.Fatalf("unexpected completeness: %+v", r)
	}
	if r.Levels[0].ResourcesAfter == nil || r.Levels[0].ResourcesAfter.ProcessOpenFDs != 40 {
		t.Fatalf("resource snapshot missing: %+v", r.Levels[0])
	}
	md := renderMarkdown(r)
	if !strings.Contains(md, "96.0 MiB") || !strings.Contains(md, "Sustained resource sampling complete: **false**") || !strings.Contains(md, "Before/after resource files remain endpoint evidence") {
		t.Fatalf("resource evidence missing from markdown: %s", md)
	}
}

func TestBuildReportMarksPartialResourceSnapshotsIncompleteWithoutBreakingAccounting(t *testing.T) {
	d := t.TempDir()
	b, _ := json.Marshal(benchSummary{Concurrency: 2, Requests: 1, Successful: 1})
	if err := os.WriteFile(filepath.Join(d, "concurrency-2.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
	metrics := "ollama_gateway_requests_total{status_class=\"2xx\"} 1\nollama_gateway_queue_seconds_count 1\nollama_gateway_service_seconds_count 1\n"
	if err := os.WriteFile(filepath.Join(d, "metrics-before-c2.prom"), []byte("ollama_gateway_requests_total{status_class=\"2xx\"} 0\nollama_gateway_queue_seconds_count 0\nollama_gateway_service_seconds_count 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, "metrics-after-c2.prom"), []byte(metrics), 0o600); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(resourceSnapshot{PID: 99, HostLogicalCPUs: 4})
	if err := os.WriteFile(filepath.Join(d, "resources-before-c2.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := buildReport(d)
	if err != nil {
		t.Fatal(err)
	}
	if !r.EvidenceComplete || r.ResourceEvidenceComplete || r.GateStatus != "not-proven" {
		t.Fatalf("unexpected report: %+v", r)
	}
	if !strings.Contains(r.GateReason, "resource evidence is incomplete") {
		t.Fatalf("unexpected gate reason: %s", r.GateReason)
	}
}

func TestBuildReportIncludesSustainedResourceSampling(t *testing.T) {
	d := t.TempDir()
	s := benchSummary{Concurrency: 16, Requests: 10, WarmupRequests: 2, Successful: 10, ThroughputRPS: 75, Latency: durationStats{P95: 12}, TTFB: durationStats{P95: 5}}
	b, _ := json.Marshal(s)
	if err := os.WriteFile(filepath.Join(d, "concurrency-16.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
	beforeMetrics := "ollama_gateway_requests_total{status_class=\"2xx\"} 4\nollama_gateway_queue_seconds_count 4\nollama_gateway_service_seconds_count 4\n"
	afterMetrics := "ollama_gateway_requests_total{status_class=\"2xx\"} 16\nollama_gateway_queue_seconds_count 16\nollama_gateway_service_seconds_count 16\n"
	if err := os.WriteFile(filepath.Join(d, "metrics-before-c16.prom"), []byte(beforeMetrics), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, "metrics-after-c16.prom"), []byte(afterMetrics), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, snap := range map[string]resourceSnapshot{
		"resources-before-c16.json": {PID: 77, ProcessRSSBytes: 50 * 1024 * 1024, HostLogicalCPUs: 8},
		"resources-after-c16.json":  {PID: 77, ProcessRSSBytes: 60 * 1024 * 1024, HostLogicalCPUs: 8},
	} {
		data, _ := json.Marshal(snap)
		if err := os.WriteFile(filepath.Join(d, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	samples := resourceSamplingReport{
		Version: 1, PID: 77, GOOS: "linux", PeakProcessRSSBytes: 80 * 1024 * 1024,
		PeakProcessCPUPercent: 145.5, PeakProcessOpenFDs: 55,
		Samples: []resourceSnapshot{{PID: 77}, {PID: 77}}, StopReason: "stop-file",
	}
	data, _ := json.Marshal(samples)
	if err := os.WriteFile(filepath.Join(d, "resources-samples-c16.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := buildReport(d)
	if err != nil {
		t.Fatal(err)
	}
	if !r.EvidenceComplete || !r.ResourceEvidenceComplete || !r.SustainedResourceEvidenceComplete {
		t.Fatalf("unexpected completeness: %+v", r)
	}
	if r.Levels[0].ResourceSamples == nil || r.Levels[0].ResourceSamples.PeakProcessOpenFDs != 55 {
		t.Fatalf("missing sustained samples: %+v", r.Levels[0])
	}
	if !strings.Contains(r.GateReason, "sustained host/process resource sampling is complete") {
		t.Fatalf("unexpected gate reason: %s", r.GateReason)
	}
	md := renderMarkdown(r)
	if !strings.Contains(md, "80.0 MiB") || !strings.Contains(md, "145.5%") || !strings.Contains(md, "| 16 | 10/0 |") {
		t.Fatalf("sustained evidence missing from markdown: %s", md)
	}
}
