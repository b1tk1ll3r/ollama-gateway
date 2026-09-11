package worker

import (
	"testing"
	"time"
)

func int64p(v int64) *int64       { return &v }
func float64p(v float64) *float64 { return &v }

func TestMergeExternalTelemetryPreservesOmittedFieldsAndAcceptsZero(t *testing.T) {
	base := ResourceTelemetry{MemoryTotalBytes: 100, MemoryUsedBytes: 50, VRAMTotalBytes: 200, VRAMUsedBytes: 80, GPUUtilizationPct: 75, GPUTemperatureC: 60, GPUPowerWatts: 120, Source: "local-system", Error: "local warning"}
	mergeExternalTelemetry(&base, externalTelemetry{
		VRAMUsedBytes:     int64p(0),
		GPUUtilizationPct: float64p(0),
		Source:            "host-memory+amdgpu-sysfs",
		Error:             "agent warning",
	})
	if base.MemoryTotalBytes != 100 || base.MemoryUsedBytes != 50 || base.VRAMTotalBytes != 200 {
		t.Fatalf("omitted fields were overwritten: %+v", base)
	}
	if base.VRAMUsedBytes != 0 || base.GPUUtilizationPct != 0 {
		t.Fatalf("explicit zero fields were not applied: %+v", base)
	}
	if base.GPUTemperatureC != 60 || base.GPUPowerWatts != 120 {
		t.Fatalf("omitted GPU fields were overwritten: %+v", base)
	}
	if base.Source != "local-system+telemetry-url:host-memory+amdgpu-sysfs" {
		t.Fatalf("unexpected source %q", base.Source)
	}
	if base.Error != "local warning; agent warning" {
		t.Fatalf("unexpected error %q", base.Error)
	}
}

func timep(v time.Time) *time.Time { return &v }

func TestValidateExternalTelemetryTimestamp(t *testing.T) {
	now := time.Date(2026, 9, 8, 18, 0, 0, 0, time.UTC)
	if err := validateExternalTelemetryTimestamp(now, nil, 30*time.Second); err != nil {
		t.Fatalf("missing legacy timestamp should remain compatible: %v", err)
	}
	fresh := now.Add(-10 * time.Second)
	if err := validateExternalTelemetryTimestamp(now, timep(fresh), 30*time.Second); err != nil {
		t.Fatalf("fresh timestamp rejected: %v", err)
	}
	stale := now.Add(-31 * time.Second)
	if err := validateExternalTelemetryTimestamp(now, timep(stale), 30*time.Second); err == nil {
		t.Fatal("expected stale timestamp rejection")
	}
	future := now.Add(31 * time.Second)
	if err := validateExternalTelemetryTimestamp(now, timep(future), 30*time.Second); err == nil {
		t.Fatal("expected future timestamp rejection")
	}
}

func TestTelemetryMaxAge(t *testing.T) {
	if got := telemetryMaxAge(5 * time.Second); got != 30*time.Second {
		t.Fatalf("got %s", got)
	}
	if got := telemetryMaxAge(20 * time.Second); got != 2*time.Minute {
		t.Fatalf("got %s", got)
	}
}
