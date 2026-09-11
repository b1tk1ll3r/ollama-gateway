package main

import "testing"

func TestCIDRAllowlist(t *testing.T) {
	a, err := parseCIDRs("127.0.0.1/32,10.2.19.0/24")
	if err != nil {
		t.Fatal(err)
	}
	if !a.allowed("10.2.19.42:1234") || !a.allowed("127.0.0.1:1") || a.allowed("10.2.18.1:5") {
		t.Fatal("unexpected allowlist result")
	}
}

func TestAppendSource(t *testing.T) {
	if got := appendSource("host-memory", "amdgpu-sysfs"); got != "host-memory+amdgpu-sysfs" {
		t.Fatal(got)
	}
}
