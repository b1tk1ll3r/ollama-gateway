//go:build linux

package hoststats

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func readMemory(context.Context) (Memory, error) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return Memory{}, err
	}
	defer f.Close()
	var total, available int64
	s := bufio.NewScanner(f)
	for s.Scan() {
		fields := strings.Fields(s.Text())
		if len(fields) < 2 {
			continue
		}
		v, _ := strconv.ParseInt(fields[1], 10, 64)
		switch strings.TrimSuffix(fields[0], ":") {
		case "MemTotal":
			total = v * 1024
		case "MemAvailable":
			available = v * 1024
		}
	}
	if err := s.Err(); err != nil {
		return Memory{}, err
	}
	if total <= 0 {
		return Memory{}, fmt.Errorf("MemTotal unavailable")
	}
	used := total - available
	if used < 0 {
		used = 0
	}
	return Memory{TotalBytes: total, UsedBytes: used}, nil
}
