package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/example/ollama-fair-gateway/internal/haresource"
)

func main() {
	pid := flag.Int("pid", 0, "gateway process PID")
	out := flag.String("json-out", "-", "output JSON path; '-' writes stdout")
	flag.Parse()
	if *pid <= 0 {
		fmt.Fprintln(os.Stderr, "-pid must be positive")
		os.Exit(2)
	}
	s := haresource.Collect(*pid)
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	b = append(b, '\n')
	if *out == "-" {
		_, _ = os.Stdout.Write(b)
		return
	}
	if err := os.WriteFile(*out, b, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func outputName(dir string, concurrency int, phase string) string {
	return filepath.Join(dir, fmt.Sprintf("resources-%s-c%d.json", phase, concurrency))
}
