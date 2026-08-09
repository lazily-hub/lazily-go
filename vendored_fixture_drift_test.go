package lazily

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The vendored corpus under test/conformance/ must not drift from the canonical
// one (#lazilyvendoredfixture).
//
// `loadConformanceFixture` deliberately falls back to test/conformance/ when the
// lazily-spec sibling is absent, so the copies are a feature rather than debris —
// they let the suite run offline. But a copy that nobody compares is exactly how a
// fixture silently shrinks to a third of its source, which is the failure lazily-cpp
// documented in its own loader.
//
// The runtime conformance manifest cannot catch this either. It records that a file
// was opened; it has no opinion on whether the file was the right one. Two direct
// reads in this package were reading the vendored copy FIRST, bypassing the
// fallback order entirely, and both the source grep and the manifest were satisfied.
//
// This gate closes that. When the canonical sibling is present, every vendored file
// must be byte-identical to its counterpart. When the sibling is absent the check
// SKIPS rather than passes: with nothing to compare against, a pass would assert
// something it never checked — and the offline case is precisely when the copies
// are load-bearing and least verifiable.
func TestVendoredFixturesMatchCanonical(t *testing.T) {
	const vendoredRoot = "test/conformance"
	canonicalRoot := specDir()

	if _, err := os.Stat(canonicalRoot); os.IsNotExist(err) {
		t.Skipf("canonical corpus absent at %s — cannot compare; clone the lazily-spec sibling", canonicalRoot)
	}
	if _, err := os.Stat(vendoredRoot); os.IsNotExist(err) {
		t.Skipf("no vendored fixtures under %s", vendoredRoot)
	}

	compared := 0
	err := filepath.Walk(vendoredRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(path, ".json") {
			return nil
		}
		rel, relErr := filepath.Rel(vendoredRoot, path)
		if relErr != nil {
			return relErr
		}
		canonical := filepath.Join(canonicalRoot, rel)

		canonicalBytes, cErr := os.ReadFile(canonical)
		if os.IsNotExist(cErr) {
			t.Errorf("vendored fixture %s has no canonical counterpart at %s — it was "+
				"removed or renamed upstream and this copy is now the only version", rel, canonical)
			return nil
		}
		if cErr != nil {
			return cErr
		}
		vendoredBytes, vErr := os.ReadFile(path)
		if vErr != nil {
			return vErr
		}
		if !bytes.Equal(vendoredBytes, canonicalBytes) {
			t.Errorf("vendored fixture %s DRIFTED from the canonical corpus "+
				"(%d bytes vendored vs %d canonical). The offline fallback would replay "+
				"the stale copy. Re-sync it from %s or delete it.",
				rel, len(vendoredBytes), len(canonicalBytes), canonicalRoot)
		}
		compared++
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", vendoredRoot, err)
	}

	// Positive proof: a walk that matched nothing would otherwise pass silently,
	// which is the same vacuous green this gate exists to prevent.
	if compared == 0 {
		t.Fatalf("compared 0 vendored fixtures under %s — the check is vacuous", vendoredRoot)
	}
	t.Logf("%d vendored fixtures match the canonical corpus", compared)
}
