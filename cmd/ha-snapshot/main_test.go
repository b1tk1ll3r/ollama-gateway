package main

import "testing"

func TestOutputName(t *testing.T) {
	got := outputName("out", 8, "before")
	if got != "out/resources-before-c8.json" {
		t.Fatalf("outputName=%q", got)
	}
}
