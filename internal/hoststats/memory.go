package hoststats

import "context"

type Memory struct {
	TotalBytes int64
	UsedBytes  int64
}

func ReadMemory(ctx context.Context) (Memory, error) { return readMemory(ctx) }
