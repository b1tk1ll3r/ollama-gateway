package hoststats

import "testing"

// Parsing is exercised indirectly through the command-backed function in
// integration environments with nvidia-smi. Keep this package dependency-free.
func TestNVIDIAStruct(t *testing.T) {
	n := NVIDIA{UtilizationPercent: 50, MemoryUsedBytes: 1, MemoryTotalBytes: 2}
	if n.MemoryTotalBytes != 2 {
		t.Fatal("unexpected struct value")
	}
}
