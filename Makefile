# lazily-go — build, test, and verification targets.

.PHONY: all build test test-interop-peer vet fmt fmt-check race cover conformance bench check tidy conformance-coverage assertion-ordering-check ci-reach

all: check

# One conformance-run id per `make` invocation (#lzstalemanifest).
#
# The two evidence files under build/ are WRITTEN by the `test` target and READ by
# `conformance-coverage`, a separate process. Nothing in that channel proved the
# bytes came from this invocation: the guard read whatever was on disk and
# believed it. `go test` caches per package, and a cached package does not run the
# binary at all — it writes no evidence and leaves the previous run's file exactly
# where it was. Measured here: a second `go test .` on an unchanged tree prints
# `ok ... (cached)` and appends ZERO lines to the manifest, with the evidence env
# vars set and even when their VALUES change, so the cache does not key on them.
#
# So the evidence is STAMPED, and every guard that reads it requires the stamp to
# be this invocation's. The id is the only thing that makes a stale read
# impossible; `-count=1` below only keeps the ordinary case from going falsely
# RED. Distinguishing those two is the point — a flag that avoids producing stale
# evidence is weaker than a guard that refuses it, because the flag is one edit
# away from being dropped and nothing downstream would notice.
#
# `export` is load-bearing: `conformance-coverage` runs the guard in a child
# shell, and it has to see the SAME id the `test` recipe stamped.
#
# `:=` is load-bearing too. With a recursive `=` the $(shell) re-runs at every
# reference, so the test step and the guard step would disagree — fail-closed, but
# for a reason that takes an hour to find.
LAZILY_CONFORMANCE_RUN_ID := go-$(shell date -u +%s%N)-$(shell echo $$$$)
export LAZILY_CONFORMANCE_RUN_ID

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
#
# The truncation on the first line is what keeps a cached run from being a false
# GREEN here — an empty file is missing evidence and the guard says so. It is not
# a freshness check, though: it only covers the case where `test` and the guard
# run in the same `make`. LAZILY_CONFORMANCE_RUN_ID (#lzstalemanifest) is the
# freshness check, and it is passed through the environment by the `export` above,
# not spelled on this line, so that `test` and `conformance-coverage` cannot drift
# to two different ids.
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
assertion-ordering-check:
	python3 ../lazily-spec/scripts/check-assertion-ordering.py --binding go --root .

check: fmt-check vet build test race test-interop-peer conformance-coverage assertion-ordering-check ci-reach
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
#
# It depends on `test` (#lzstalemanifest). The guard reads evidence stamped with
# this invocation's LAZILY_CONFORMANCE_RUN_ID, so a bare `make conformance-coverage`
# would otherwise refuse the previous run's files — correctly, and uselessly. The
# dependency also removes a real `make -j` hazard: nothing but recipe ORDER used to
# stop the guard from reading a manifest the test step was still appending to.
conformance-coverage: test
	./scripts/check-conformance-coverage.sh
