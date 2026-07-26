package lazily

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The queue-family flavor ledger — enforced against the source, not a comment.
//
// queue_test.go replays the canonical queuecell_*.json corpus against the
// single-threaded QueueCell. That is currently the only flavor: no binding in the
// family ships a thread-safe or async queue primitive, and cell-model.md's
// "Core surface vs. binding extensions (queue family)" now makes those Core, so
// their absence is a conformance gap rather than an unfinished nicety.
//
// A three-flavor replay written today would skip two of three flavors entirely,
// and a suite that skips almost everything while reporting green is exactly the
// failure this file prevents. So the ledger is wired to the source: it greps the
// package for each unshipped flavor's type name, and the moment one appears this
// goes red and names the runner to extend.
//
// Mirrors lazily-rs/tests/queue_family_conformance.rs.

var queueFixtures = []string{
	"queuecell_spsc_push_pop.json",
	"queuecell_popped_head_observation.json",
	"queuecell_mpsc_multi_writer.json",
	"queuecell_bounded_backpressure.json",
	"queuecell_closure_lifecycle.json",
}

type queueFlavor struct {
	name string
	// markerType is grepped, not referenced: referencing a type that does not
	// exist would not compile, and a ledger you cannot write until the work is
	// done is no ledger at all.
	markerType string
	shipped    bool
}

var queueLedger = []queueFlavor{
	{"single-threaded", "type QueueCell[", true},
	{"thread-safe", "ThreadSafeQueueCell", false},
	{"async", "AsyncQueueCell", false},
}

// packageSources concatenates the non-test Go sources in this package.
func packageSources(t *testing.T) string {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	var b strings.Builder
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		data, err := specReadFile(name)
		if err != nil {
			continue
		}
		b.Write(data)
	}
	return b.String()
}

// TestQueueLedgerUnshippedFlavorsAreReallyAbsent enforces the ledger. When a
// ThreadSafeQueueCell or AsyncQueueCell lands, this fails and says what to do, so
// a newly-shipped flavor cannot sit silently unreplayed while the suite is green.
func TestQueueLedgerUnshippedFlavorsAreReallyAbsent(t *testing.T) {
	sources := packageSources(t)
	if sources == "" {
		t.Fatal("read no package sources; the ledger check would be vacuous")
	}
	for _, f := range queueLedger {
		defined := strings.Contains(sources, f.markerType)
		if f.shipped && !defined {
			t.Errorf("flavor %q is recorded as shipped but %q is not defined in the "+
				"package — the ledger claims coverage this package does not have",
				f.name, f.markerType)
		}
		if !f.shipped && defined {
			t.Errorf("flavor %q now EXISTS in the package (%q) but the queue-family "+
				"ledger still records it as unshipped, so the canonical corpus is not "+
				"being replayed against it.\n\nFix: flip shipped for %q in queueLedger "+
				"AND extend the replay to drive it, as collections_family_conformance_test.go "+
				"drives all three map flavors. Do NOT flip the flag alone — that restores "+
				"the false green this test prevents.",
				f.name, f.markerType, f.name)
		}
	}
}

// TestQueueLedgerIsNotAllSkips fails if every flavor is unshipped: in a summary
// line, "skipped" and "passed" are indistinguishable.
func TestQueueLedgerIsNotAllSkips(t *testing.T) {
	shipped := 0
	for _, f := range queueLedger {
		if f.shipped {
			shipped++
		}
	}
	if shipped == 0 {
		t.Fatal("every queue flavor is recorded as unshipped, so this suite would " +
			"assert nothing while still reporting success")
	}
	if len(queueLedger) != 3 {
		t.Fatalf("ledger covers %d flavors, want all 3; a missing entry is an unscored "+
			"gap, not an absent one", len(queueLedger))
	}
}

// TestQueueLedgerShippedFlavorReplaysCorpus is positive proof this file read the
// corpus. An absence guard proves the fixtures exist; only a count proves they
// were opened.
func TestQueueLedgerShippedFlavorReplaysCorpus(t *testing.T) {
	dir := ""
	for _, candidate := range []string{
		filepath.Join("..", "lazily-spec", "conformance", "collections"),
		filepath.Join("conformance", "collections"),
		filepath.Join("testdata", "conformance", "collections"),
	} {
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			dir = candidate
			break
		}
	}
	if dir == "" {
		t.Skip("canonical collections fixtures not found")
	}

	fixturesRead, stepsSeen, matricesSeen := 0, 0, 0
	for _, name := range queueFixtures {
		data, err := specReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("%s: declared queue fixture is missing: %v", name, err)
		}
		var fixture map[string]any
		if err := json.Unmarshal(data, &fixture); err != nil {
			t.Fatalf("%s: parse: %v", name, err)
		}
		fixturesRead++

		steps, _ := fixture["steps"].([]any)
		if len(steps) == 0 {
			t.Fatalf("%s: fixture has no steps - a vacuous replay would report green", name)
		}
		stepsSeen += len(steps)

		for i, raw := range steps {
			step, _ := raw.(map[string]any)
			// The matrix nests under `expected`, NOT on the step. lazily-rs's MAP
			// runner read it off the step, so it was always absent and the assertion
			// never ran once. Pin the nesting so that cannot recur here.
			if _, bad := step["invalidates"]; bad {
				t.Fatalf("%s step %d: `invalidates` appears at STEP level; the runners "+
					"read expected.invalidates, so a step-level copy is silently ignored",
					name, i)
			}
			expected, ok := step["expected"].(map[string]any)
			if !ok {
				t.Fatalf("%s step %d: no expected block", name, i)
			}
			if _, has := expected["invalidates"]; has {
				matricesSeen++
			}
		}
	}

	if fixturesRead != len(queueFixtures) {
		t.Fatalf("read %d of %d declared queue fixtures", fixturesRead, len(queueFixtures))
	}
	if stepsSeen == 0 {
		t.Fatal("read the corpus but saw zero steps")
	}
	if matricesSeen == 0 {
		t.Fatal("no fixture carried an expected.invalidates matrix - the reader-kind " +
			"independence contract would be unasserted")
	}
}
