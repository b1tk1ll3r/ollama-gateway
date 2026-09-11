package haresource

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestParseProcStatTicksHandlesSpacesInComm(t *testing.T) {
	// fields 3..15 after the closing parenthesis; utime=11 and stime=13.
	line := "123 (gateway worker (test)) S 1 2 3 4 5 6 7 8 9 10 11 13 0 0 0"
	got, err := ParseProcStatTicks(line)
	if err != nil {
		t.Fatal(err)
	}
	if got != 24 {
		t.Fatalf("ticks=%d want=24", got)
	}
}

func TestParseHostCPUTicks(t *testing.T) {
	got, err := ParseHostCPUTicks("cpu  1 2 3 4 5 6 7 8 9 10\ncpu0 1 2 3\n")
	if err != nil {
		t.Fatal(err)
	}
	if got != 55 {
		t.Fatalf("ticks=%d want=55", got)
	}
}

func TestSamplingReportComplete(t *testing.T) {
	r := SamplingReport{Version: SamplingSchemaVersion, PID: 1, Samples: []Snapshot{{}, {}}, PeakProcessRSSBytes: 1, StopReason: "stop-file"}
	if !r.Complete() {
		t.Fatal("expected complete report")
	}
	r.ProcessExited = true
	if r.Complete() {
		t.Fatal("process-exited report must be incomplete")
	}
	r.ProcessExited = false
	r.StopReason = "max-duration"
	if r.Complete() {
		t.Fatal("max-duration report must be incomplete")
	}
}

func TestSampleCurrentProcessStopsOnFile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("ps-based collector is not portable to Windows test hosts")
	}
	stop := filepath.Join(t.TempDir(), "stop")
	go func() {
		time.Sleep(140 * time.Millisecond)
		_ = os.WriteFile(stop, []byte("stop\n"), 0o600)
	}()
	r, err := Sample(context.Background(), os.Getpid(), 50*time.Millisecond, 2*time.Second, stop)
	if err != nil {
		t.Fatal(err)
	}
	if r.StopReason != "stop-file" || len(r.Samples) < 2 || r.PeakProcessRSSBytes <= 0 {
		t.Fatalf("unexpected report: %+v", r)
	}
	if runtime.GOOS == "linux" && r.CPUMethod != "linux_procfs_interval_all_cpus_percent" {
		t.Fatalf("cpu method=%q", r.CPUMethod)
	}
}
