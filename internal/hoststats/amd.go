package hoststats

import "context"

// AMD contains lightweight AMDGPU telemetry. Linux reads the standard amdgpu
// sysfs counters and therefore needs no ROCm library, daemon or CGO.
type AMD struct {
	UtilizationPercent float64
	MemoryUsedBytes    int64
	MemoryTotalBytes   int64
	TemperatureC       float64
	PowerWatts         float64
}

func ReadAMD(ctx context.Context, device string) (AMD, error) { return readAMD(ctx, device) }
