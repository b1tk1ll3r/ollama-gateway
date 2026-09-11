package hoststats

import (
	"context"
	"encoding/csv"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

// NVIDIA contains lightweight local GPU telemetry obtained from nvidia-smi.
// Values are intentionally limited to stable fields supported by the selective
// --query-gpu interface; the gateway does not require NVML or CGO.
type NVIDIA struct {
	UtilizationPercent float64
	MemoryUsedBytes    int64
	MemoryTotalBytes   int64
	TemperatureC       float64
	PowerWatts         float64
}

func ReadNVIDIA(ctx context.Context, gpu string) (NVIDIA, error) {
	args := []string{"--query-gpu=utilization.gpu,memory.used,memory.total,temperature.gpu,power.draw", "--format=csv,noheader,nounits"}
	if strings.TrimSpace(gpu) != "" {
		args = append(args, "-i", strings.TrimSpace(gpu))
	}
	out, err := exec.CommandContext(ctx, "nvidia-smi", args...).Output()
	if err != nil {
		if ctx.Err() != nil {
			return NVIDIA{}, ctx.Err()
		}
		return NVIDIA{}, fmt.Errorf("nvidia-smi: %w", err)
	}
	r := csv.NewReader(strings.NewReader(strings.TrimSpace(string(out))))
	rec, err := r.Read()
	if err != nil || len(rec) < 5 {
		return NVIDIA{}, fmt.Errorf("nvidia-smi returned an unexpected row")
	}
	parse := func(s string) (float64, error) {
		s = strings.TrimSpace(s)
		if s == "" || strings.EqualFold(s, "N/A") || strings.EqualFold(s, "[Not Supported]") {
			return 0, nil
		}
		return strconv.ParseFloat(s, 64)
	}
	util, err := parse(rec[0])
	if err != nil {
		return NVIDIA{}, fmt.Errorf("parse gpu utilization: %w", err)
	}
	usedMiB, err := parse(rec[1])
	if err != nil {
		return NVIDIA{}, fmt.Errorf("parse gpu memory used: %w", err)
	}
	totalMiB, err := parse(rec[2])
	if err != nil {
		return NVIDIA{}, fmt.Errorf("parse gpu memory total: %w", err)
	}
	temp, err := parse(rec[3])
	if err != nil {
		return NVIDIA{}, fmt.Errorf("parse gpu temperature: %w", err)
	}
	power, err := parse(rec[4])
	if err != nil {
		return NVIDIA{}, fmt.Errorf("parse gpu power: %w", err)
	}
	const mib = int64(1024 * 1024)
	return NVIDIA{UtilizationPercent: util, MemoryUsedBytes: int64(usedMiB * float64(mib)), MemoryTotalBytes: int64(totalMiB * float64(mib)), TemperatureC: temp, PowerWatts: power}, nil
}
