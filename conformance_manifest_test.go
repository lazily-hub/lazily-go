package lazily

import (
	"os"
	"path/filepath"
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

const conformanceMarker = "lazily-spec" + string(filepath.Separator) + "conformance" + string(filepath.Separator)

// specReadFile is os.ReadFile plus a record of any conformance fixture it opens.
func specReadFile(name string) ([]byte, error) {
	recordConformanceRead(name)
	return os.ReadFile(name)
}

func recordConformanceRead(name string) {
	abs, err := filepath.Abs(name)
	if err != nil {
		abs = name
	}
	idx := strings.Index(abs, conformanceMarker)
	if idx == -1 {
		return
	}
	id := filepath.ToSlash(abs[idx+len(conformanceMarker):])
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

// TestMain exists solely to flush the manifest after the package's tests finish.
// A deferred flush in each test would race and truncate; a single exit hook is the
// only place the union is complete.
func TestMain(m *testing.M) {
	code := m.Run()
	flushConformanceManifest()
	os.Exit(code)
}
