//go:build windows

package hoststats

import (
	"context"
	"fmt"
	"syscall"
	"unsafe"
)

type memoryStatusEx struct {
	Length               uint32
	MemoryLoad           uint32
	TotalPhys            uint64
	AvailPhys            uint64
	TotalPageFile        uint64
	AvailPageFile        uint64
	TotalVirtual         uint64
	AvailVirtual         uint64
	AvailExtendedVirtual uint64
}

func readMemory(ctx context.Context) (Memory, error) {
	if err := ctx.Err(); err != nil {
		return Memory{}, err
	}
	var st memoryStatusEx
	st.Length = uint32(unsafe.Sizeof(st))
	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	proc := kernel32.NewProc("GlobalMemoryStatusEx")
	r1, _, callErr := proc.Call(uintptr(unsafe.Pointer(&st)))
	if r1 == 0 {
		if callErr != syscall.Errno(0) {
			return Memory{}, fmt.Errorf("GlobalMemoryStatusEx: %w", callErr)
		}
		return Memory{}, fmt.Errorf("GlobalMemoryStatusEx failed")
	}
	used := int64(st.TotalPhys - st.AvailPhys)
	return Memory{TotalBytes: int64(st.TotalPhys), UsedBytes: used}, nil
}
