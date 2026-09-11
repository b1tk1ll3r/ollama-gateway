//go:build !linux && !darwin && !windows

package hoststats

import (
	"context"
	"fmt"
)

func readMemory(context.Context) (Memory, error) {
	return Memory{}, fmt.Errorf("host memory telemetry unsupported on this OS")
}
