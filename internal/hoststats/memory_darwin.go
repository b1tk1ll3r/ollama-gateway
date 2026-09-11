//go:build darwin

package hoststats

import (
	"bufio"
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
)

var pageSizeRE = regexp.MustCompile(`page size of ([0-9]+) bytes`)

func readMemory(ctx context.Context) (Memory, error) {
	totalOut, err := exec.CommandContext(ctx, "/usr/sbin/sysctl", "-n", "hw.memsize").Output()
	if err != nil {
		return Memory{}, err
	}
	total, err := strconv.ParseInt(strings.TrimSpace(string(totalOut)), 10, 64)
	if err != nil || total <= 0 {
		return Memory{}, fmt.Errorf("invalid hw.memsize")
	}
	out, err := exec.CommandContext(ctx, "/usr/bin/vm_stat").Output()
	if err != nil {
		return Memory{}, err
	}
	pageSize := int64(4096)
	var freePages int64
	s := bufio.NewScanner(strings.NewReader(string(out)))
	first := true
	for s.Scan() {
		line := s.Text()
		if first {
			first = false
			if m := pageSizeRE.FindStringSubmatch(line); len(m) == 2 {
				if v, e := strconv.ParseInt(m[1], 10, 64); e == nil {
					pageSize = v
				}
			}
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		name := strings.TrimSpace(parts[0])
		v := strings.TrimSpace(strings.TrimSuffix(parts[1], "."))
		n, _ := strconv.ParseInt(v, 10, 64)
		switch name {
		case "Pages free", "Pages inactive", "Pages speculative":
			freePages += n
		}
	}
	available := freePages * pageSize
	used := total - available
	if used < 0 {
		used = 0
	}
	if used > total {
		used = total
	}
	return Memory{TotalBytes: total, UsedBytes: used}, nil
}
