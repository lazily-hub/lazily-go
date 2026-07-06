# lazily-go — build, test, and verification targets.

.PHONY: all build test vet fmt fmt-check race cover conformance bench check tidy

all: check

build:
	go build ./...

test:
	go test ./...

# CRDT/concurrency correctness under the race detector (cgo required).
race:
	CGO_ENABLED=1 go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

fmt-check:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

cover:
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

# Replay the shared lazily-spec conformance fixtures.
conformance:
	go test -run Conformance ./...

# Micro-benchmarks for the hot paths (see BENCHMARKS.md).
bench:
	go test -run '^$$' -bench=. -benchmem ./...

tidy:
	go mod tidy

# Full local gate — run before committing.
check: fmt-check vet build test
	@echo "lazily-go: check OK"
