package lazily

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
)

// Runtime conformance manifest (#lazilyupgradeconformance).
//
// The static coverage guard greps test sources for fixture filenames. That catches
// a fixture nobody mentions, but not one mentioned in a comment and
// hand-transcribed — the drift found in lazily-cpp's queue tests, where the source
// names the fixture and the bytes are never opened. Only observing the read proves
// the corpus was replayed.
//
// Go offers no interception seam (no monkey-patching, and strace is unavailable
// here), and this package has no single shared loader — 24 read sites across 22
// files, each with its own per-file helper. So the seam is introduced rather than
// found: `specReadFile` replaces `os.ReadFile` throughout the test package. Every
// test file is `package lazily`, so one helper serves all of them, and the
// substitution is a single token.
//
// Reads outside the conformance corpus pass straight through and are not recorded,
// so routing every os.ReadFile through this is harmless.
var (
	manifestMu     sync.Mutex
	manifestOpened = map[string]struct{}{}
)

// The corpus root, the candidate order, and the attribution rule all live in
// conformance_corpus_test.go (#lzoverrideallrunners).

// specReadFile is os.ReadFile plus a record of any conformance fixture it opens.
func specReadFile(name string) ([]byte, error) {
	recordConformanceRead(name)
	// Rule 8 of the prose-key convention books here too, on a broader marker:
	// an opened fixture whose block declares `prose` owes a verification whether
	// it came from the canonical corpus or the vendored mirror.
	recordProseOpened(name)
	return os.ReadFile(name)
}

// recordConformanceRead attributes a read RELATIVE TO THE RESOLVED CORPUS ROOT,
// not by scanning for a hardcoded path substring. Under
// LAZILY_SPEC_CONFORMANCE_DIR the corpus lives somewhere that contains no such
// substring, and the old rule silently recorded nothing — turning a
// corpus-perturbation probe into a vacuous green (#lzoverrideallrunners).
func recordConformanceRead(name string) {
	id, ok := specCanonicalRelative(name)
	if !ok {
		return
	}
	manifestMu.Lock()
	manifestOpened[id] = struct{}{}
	manifestMu.Unlock()
}

// flushConformanceManifest appends what this binary opened to the path in
// LAZILY_CONFORMANCE_MANIFEST. Append, not truncate: `go test ./...` may run more
// than one binary and each must contribute. A no-op when the variable is unset, so
// a plain `go test` is unaffected.
func flushConformanceManifest() {
	out := os.Getenv("LAZILY_CONFORMANCE_MANIFEST")
	if out == "" {
		return
	}
	manifestMu.Lock()
	defer manifestMu.Unlock()
	if len(manifestOpened) == 0 {
		return
	}
	ids := make([]string, 0, len(manifestOpened))
	for id := range manifestOpened {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	f, err := os.OpenFile(out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		// A manifest we cannot write surfaces downstream as missing evidence,
		// which is correct. Never fail a suite over bookkeeping.
		return
	}
	defer f.Close()
	_, _ = f.WriteString(strings.Join(ids, "\n") + "\n")
}

// TestMain exists solely to flush the manifest — and the per-scenario replay
// ledger it is joined against (#lzscenariocoverage) — after the package's tests
// finish. A deferred flush in each test would race and truncate; a single exit
// hook is the only place the union is complete.
func TestMain(m *testing.M) {
	// An explicitly-set-but-unusable corpus override is a broken run. Refuse it
	// here, before a single runner gets the chance to skip its way to green or
	// to fall back to the corpus the operator redirected away from
	// (#lzoverrideallrunners).
	if err := specCorpusError(); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: %v\n", err)
		os.Exit(1)
	}
	code := m.Run()
	flushConformanceManifest()
	flushConformanceScenarios()
	// Rule 8 of the prose-key convention (#lzprosekeyconvention). Here rather
	// than in a test function for the same reason the manifest flush is: only
	// after m.Run is the union of verifications complete, and no ordering
	// between test functions has to be assumed.
	if !checkProseVerificationCoverage() && code == 0 {
		code = 1
	}
	os.Exit(code)
}
