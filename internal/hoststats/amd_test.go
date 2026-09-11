//go:build linux

package hoststats

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func writeStat(t *testing.T, path, value string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(value), 0644); err != nil {
		t.Fatal(err)
	}
}

func TestReadAMDDevice(t *testing.T) {
	device := filepath.Join(t.TempDir(), "device")
	writeStat(t, filepath.Join(device, "mem_info_vram_total"), "17179869184\n")
	writeStat(t, filepath.Join(device, "mem_info_vram_used"), "4294967296\n")
	writeStat(t, filepath.Join(device, "gpu_busy_percent"), "73\n")
	writeStat(t, filepath.Join(device, "hwmon", "hwmon0", "temp1_input"), "62500\n")
	writeStat(t, filepath.Join(device, "hwmon", "hwmon0", "power1_average"), "88000000\n")

	got, err := readAMDDevice(context.Background(), device)
	if err != nil {
		t.Fatal(err)
	}
	if got.MemoryTotalBytes != 17179869184 || got.MemoryUsedBytes != 4294967296 || got.UtilizationPercent != 73 || got.TemperatureC != 62.5 || got.PowerWatts != 88 {
		t.Fatalf("unexpected AMD telemetry: %+v", got)
	}
}

func TestFindAMDDevice(t *testing.T) {
	root := t.TempDir()
	writeStat(t, filepath.Join(root, "card0", "device", "vendor"), "0x10de\n")
	writeStat(t, filepath.Join(root, "card1", "device", "vendor"), "0x1002\n")
	got, err := findAMDDevice(root)
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(root, "card1", "device")
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}
