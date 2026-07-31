# lazily-go — build, test, and verification targets.

.PHONY: all build test test-interop-peer vet fmt fmt-check race cover conformance bench check tidy conformance-coverage ci-reach

all: check

build:
	go build ./...

# The manifest and scenario-ledger paths are ABSOLUTE: `go test ./...` runs one
# binary per package from that package's directory, so a relative path would
# scatter partial evidence instead of accumulating one union.
#
# Both files are truncated here and appended to by each test binary. The ledger
# (#lzscenariocoverage) records which SCENARIO of each fixture was replayed; the
# manifest records only which FILE was opened, and one scenario is enough to open
# a file.
#
# -count=1 defeats Go's test-result cache. A cached package does not run, so it
# writes nothing to either file, and `conformance-coverage` then fails with "no
# conformance manifest" on an otherwise unchanged tree. Fail-closed, but still a
# false red: whether the evidence exists must not depend on a warm cache. CI does
# the same thing for the same reason (#lzguardsnotinci).
test:
	@mkdir -p build && : > build/conformance-fixtures-loaded.txt && : > build/conformance-scenarios-replayed.txt
	LAZILY_CONFORMANCE_MANIFEST=$(CURDIR)/build/conformance-fixtures-loaded.txt \
	LAZILY_CONFORMANCE_SCENARIOS=$(CURDIR)/build/conformance-scenarios-replayed.txt \
	go test -count=1 ./...

# CRDT/concurrency correctness under the race detector (cgo required).
#
# Gated by `check`, not optional. A reactive read is a graph WRITE in this
# binding — `Get` marks, caches, and re-links edges — so an unsynchronized read
# path is a data race that the plain `go test ./...` run cannot see. That is
# exactly how the v0.23.2 map data race shipped: every read ran off the context
# lock, `make check` was green, and CI caught it. Local closeout must be able to
# catch that class too.
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
test-interop-peer:
	CGO_ENABLED=1 go run -race ./cmd/lazily-interop-peer --self-check

# `race` runs after `test` on purpose: `test` truncates the conformance manifest
# and the scenario ledger, and `race` writes neither, so the recorded fixture
# union and scenario ledger stay the ones the coverage guard is meant to audit.
check: fmt-check vet build test race test-interop-peer conformance-coverage ci-reach
	@echo "lazily-go: check OK"

# CI-reachability guard (#lzcheckcireachguard). Fails when a target above runs a
# gate no CI workflow step reaches — the drift that hid #lzinteroppeerci in every
# binding for months. It guards itself: `ci-reach` is in `check`, so CI has to run
# it too or this target reports itself missing.
ci-reach:
	./scripts/check-ci-reach.sh

# Conformance-coverage guard (#portconformancecoverage). Static: fails when the
# canonical corpus grows a fixture no test in this repo even names. Naming is not
# replaying — see the script header for what this does and does not prove.
conformance-coverage:
	./scripts/check-conformance-coverage.sh
