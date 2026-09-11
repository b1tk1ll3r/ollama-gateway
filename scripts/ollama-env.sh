#!/bin/sh
# Starting point for the 256 GB M5 Ultra. Benchmark 1/2/4 parallel requests
# with your actual model and context length before raising these values.
export OLLAMA_NUM_PARALLEL="${OLLAMA_NUM_PARALLEL:-4}"
export OLLAMA_MAX_LOADED_MODELS="${OLLAMA_MAX_LOADED_MODELS:-2}"
export OLLAMA_MAX_QUEUE="${OLLAMA_MAX_QUEUE:-64}"
export OLLAMA_CONTEXT_LENGTH="${OLLAMA_CONTEXT_LENGTH:-32768}"
export OLLAMA_KEEP_ALIVE="${OLLAMA_KEEP_ALIVE:-30m}"
exec ollama serve
