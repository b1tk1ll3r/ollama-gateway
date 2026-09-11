package haresource

import "testing"

func TestParseProcStatus(t *testing.T) {
	rss, threads := ParseProcStatus("Name:\tx\nVmRSS:\t 1234 kB\nThreads:\t7\n")
	if rss != 1234*1024 || threads != 7 {
		t.Fatalf("rss=%d threads=%d", rss, threads)
	}
}

func TestParseMeminfoAndLoadavg(t *testing.T) {
	total, avail := ParseMeminfo("MemTotal: 1000 kB\nMemAvailable: 400 kB\n")
	if total != 1000*1024 || avail != 400*1024 {
		t.Fatalf("total=%d avail=%d", total, avail)
	}
	a, b, c := ParseLoadavg("1.25 0.50 0.10 1/100 1\n")
	if a != 1.25 || b != .5 || c != .1 {
		t.Fatalf("load=%v %v %v", a, b, c)
	}
}
