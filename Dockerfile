FROM golang:1.23-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ollama-gateway ./cmd/ollama-gateway \
    && mkdir -p /out/data \
    && touch /out/data/.keep

FROM scratch
COPY --from=build /out/ollama-gateway /ollama-gateway
# Pre-create a writable state directory for the unprivileged runtime user.
# Empty named volumes mounted at /data inherit this directory's ownership.
COPY --from=build --chown=65532:65532 /out/data /data
USER 65532:65532
ENTRYPOINT ["/ollama-gateway"]
CMD ["-config", "/etc/ollama-gateway/config.json"]

# The binary contains its own HTTP probe so the scratch runtime needs no shell/curl.
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 CMD ["/ollama-gateway", "-probe", "http://127.0.0.1:8080/healthz", "-probe-timeout", "2s"]
