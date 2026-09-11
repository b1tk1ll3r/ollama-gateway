//go:build linux

package hoststats

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func readAMD(ctx context.Context, device string) (AMD, error) {
	if err := ctx.Err(); err != nil {
		return AMD{}, err
	}
	if strings.TrimSpace(device) == "" {
		var err error
		device, err = findAMDDevice("/sys/class/drm")
		if err != nil {
			return AMD{}, err
		}
	}
	return readAMDDevice(ctx, device)
}

func findAMDDevice(root string) (string, error) {
	cards, err := filepath.Glob(filepath.Join(root, "card*", "device"))
	if err != nil {
		return "", err
	}
	for _, device := range cards {
		b, err := os.ReadFile(filepath.Join(device, "vendor"))
		if err != nil {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(string(b)), "0x1002") {
			return device, nil
		}
	}
	return "", fmt.Errorf("no AMDGPU sysfs device found")
}

func readAMDDevice(ctx context.Context, device string) (AMD, error) {
	if err := ctx.Err(); err != nil {
		return AMD{}, err
	}
	var out AMD
	var found bool
	if v, ok := readInt64(filepath.Join(device, "mem_info_vram_total")); ok {
		out.MemoryTotalBytes = v
		found = true
	}
	if v, ok := readInt64(filepath.Join(device, "mem_info_vram_used")); ok {
		out.MemoryUsedBytes = v
		found = true
	}
	if v, ok := readInt64(filepath.Join(device, "gpu_busy_percent")); ok {
		out.UtilizationPercent = float64(v)
		found = true
	}
	hwmons, _ := filepath.Glob(filepath.Join(device, "hwmon", "hwmon*"))
	for _, hw := range hwmons {
		if v, ok := readInt64(filepath.Join(hw, "temp1_input")); ok && out.TemperatureC == 0 {
			out.TemperatureC = float64(v) / 1000
			found = true
		}
		if v, ok := readInt64(filepath.Join(hw, "power1_average")); ok && out.PowerWatts == 0 {
			out.PowerWatts = float64(v) / 1_000_000
			found = true
		}
	}
	if !found {
		return AMD{}, fmt.Errorf("no readable AMDGPU telemetry in %s", device)
	}
	return out, nil
}

func readInt64(path string) (int64, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	v, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	return v, err == nil
}
