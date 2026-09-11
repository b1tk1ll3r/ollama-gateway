package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/example/ollama-fair-gateway/internal/haresource"
)

func main() {
	pid := flag.Int("pid", 0, "gateway process PID")
	interval := flag.Duration("interval", 250*time.Millisecond, "resource sample interval (minimum 50ms)")
	maxDuration := flag.Duration("max-duration", 15*time.Minute, "safety limit for one sampling run")
	stopFile := flag.String("stop-file", "", "stop sampling after this file appears")
	out := flag.String("json-out", "-", "output JSON path; '-' writes stdout")
	flag.Parse()
	if *pid <= 0 || *stopFile == "" {
		fmt.Fprintln(os.Stderr, "-pid must be positive and -stop-file is required")
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	r, err := haresource.Sample(ctx, *pid, *interval, *maxDuration, *stopFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	b = append(b, '\n')
	if *out == "-" {
		_, _ = os.Stdout.Write(b)
	} else if err := os.WriteFile(*out, b, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if !r.Complete() {
		fmt.Fprintln(os.Stderr, "sampling completed but sustained resource evidence is incomplete")
		os.Exit(3)
	}
}
