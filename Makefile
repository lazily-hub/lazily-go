# lazily-go — build, test, and verification targets.

.PHONY: all build test vet fmt fmt-check race cover conformance bench check tidy conformance-coverage

all: check

build:
	go build ./...

# The manifest path is ABSOLUTE: `go test ./...` runs one binary per package
# from that package's directory, so a relative path would scatter partial
# manifests instead of accumulating one union.
test:
	@mkdir -p build && : > build/conformance-fixtures-loaded.txt
	LAZILY_CONFORMANCE_MANIFEST=$(CURDIR)/build/conformance-fixtures-loaded.txt go test ./...

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

# Large-graph scale benchmark: spreadsheet-shaped graph of ~2M nodes (default
# N=1M rows). Set LAZILY_SCALE_N=5000000 for a full 10M-cell Google Sheets
# workbook. Gated behind the `scalebench` build tag.
bench-scale:
	go test -tags scalebench -run '^$$' -bench=Scale -benchmem ./...

tidy:
	go mod tidy

# Full local gate — run before committing.
check: fmt-check vet build test conformance-coverage
	@echo "lazily-go: check OK"

# Conformance-coverage guard (#portconformancecoverage). Static: fails when the
# canonical corpus grows a fixture no test in this repo even names. Naming is not
# replaying — see the script header for what this does and does not prove.
conformance-coverage:
	./scripts/check-conformance-coverage.sh
