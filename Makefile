BINARY=ollama-gateway

.PHONY: test build build-mac build-worker-telemetry worker-telemetry bench mock-ollama ha-report ha-snapshot ha-sampler ha-readiness fmt vet clean

test:
	go test ./...

fmt:
	gofmt -w $$(find . -name '*.go' -type f)

vet:
	go vet ./...

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o dist/$(BINARY) ./cmd/ollama-gateway

build-mac:
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o dist/$(BINARY)-darwin-arm64 ./cmd/ollama-gateway
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o dist/ollama-gateway-bench-darwin-arm64 ./cmd/bench
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o dist/ollama-gateway-ha-report-darwin-arm64 ./cmd/ha-report
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o dist/ollama-gateway-ha-snapshot-darwin-arm64 ./cmd/ha-snapshot
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o dist/ollama-gateway-ha-sampler-darwin-arm64 ./cmd/ha-sampler

build-worker-telemetry:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o dist/ollama-gateway-worker-telemetry-linux-amd64 ./cmd/worker-telemetry
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o dist/ollama-gateway-worker-telemetry-linux-arm64 ./cmd/worker-telemetry

worker-telemetry:
	go run ./cmd/worker-telemetry -once

bench:
	go run ./cmd/bench -base-url http://127.0.0.1:8080 -concurrency 4 -requests 20

mock-ollama:
	go run ./cmd/mock-ollama -listen 127.0.0.1:11435

ha-report:
	go run ./cmd/ha-report -input "$${OUT_DIR:?set OUT_DIR to a completed readiness directory}"

ha-snapshot:
	go run ./cmd/ha-snapshot -pid "$${GATEWAY_PID:?set GATEWAY_PID to the gateway process PID}"

ha-sampler:
	go run ./cmd/ha-sampler -pid "$${GATEWAY_PID:?set GATEWAY_PID to the gateway process PID}" -stop-file "$${STOP_FILE:?set STOP_FILE to a path that will be created to stop sampling}"

ha-readiness:
	./scripts/ha-readiness.sh

clean:
	rm -f dist/*
