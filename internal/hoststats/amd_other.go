//go:build !linux

package hoststats

import (
	"context"
	"fmt"
)

func readAMD(context.Context, string) (AMD, error) {
	return AMD{}, fmt.Errorf("AMDGPU sysfs telemetry is supported only on Linux")
}
