package lazily

import (
	"fmt"
	"os"
	"os/exec"
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

// The corpus root, the candidate order, and the attribution rule all live in
// conformance_corpus_test.go (#lzoverrideallrunners).

// specReadFile is os.ReadFile plus a record of any conformance fixture it opens.
func specReadFile(name string) ([]byte, error) {
	recordConformanceRead(name)
	// Rule 8 of the prose-key convention books here too, on a broader marker:
	// an opened fixture whose block declares `prose` owes a verification whether
	// it came from the canonical corpus or the vendored mirror.
	recordProseOpened(name)
	// Rung 0 books the PATH as well as the id: the unbound-block guard re-reads
	// the same bytes the runner saw (#lzunboundblockguard).
	recordOpenedFixture(name)
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

// ---------------------------------------------------------------------------
// The run-id stamp (#lzstalemanifest)
// ---------------------------------------------------------------------------
//
// Everything the manifest is good for rests on it describing THIS run. It did
// not have to. The file is written by the test binary and read by
// scripts/check-conformance-coverage.sh in a separate process, and the guard had
// no freshness check of any kind — no run identifier, no mtime ordering against
// the test step. Whatever was on disk was accepted as evidence of what just
// happened.
//
// `go test` makes that reachable rather than theoretical. A package whose inputs
// have not changed is served from the test cache: the binary does not run, so
// neither flush executes, and the previous run's file stays exactly where it was.
// Measured in this repo — a second `go test .` prints `ok ... (cached)` and
// appends nothing, with the evidence env vars set and even when their values
// differ from the cached run's, so the cache does not key on them.
//
// `make test` truncates both files before running, which turns a fully cached
// `make check` into a fail-closed "no conformance manifest" rather than a green
// over last week's numbers. That shield is narrow: it only holds when the writer
// and the reader are the same `make` invocation. It does not cover a hand-run
// guard, a CI workflow that runs the steps individually, or a `make -j` that lets
// the two overlap.
//
// So each evidence file carries `# lazily-run-id <value>` as the first line THIS
// BINARY appends, the id comes from the environment of the invocation that asked
// for evidence, and every reader requires a match. The stamp is written by the
// evidence PRODUCER on purpose: a header written by the Makefile would attest to
// the make invocation, and a cached test step would leave it looking perfectly
// fresh above stale data.
const (
	conformanceRunIDEnv    = "LAZILY_CONFORMANCE_RUN_ID"
	conformanceRunIDPrefix = "# lazily-run-id "
)

// conformanceRunIDError reports why this invocation cannot stamp evidence, or nil.
//
// An id with whitespace in it is refused rather than mangled: the stamp is one
// line with a fixed prefix, so an embedded newline would split into a data line
// the guards would then try to resolve against the corpus, and a trailing space
// would make the written id and the compared id differ invisibly.
func conformanceRunIDError() error {
	id := os.Getenv(conformanceRunIDEnv)
	if id == "" {
		return fmt.Errorf("%s is unset, so the evidence this run writes could not be"+
			" distinguished from a previous run's (#lzstalemanifest). The Makefile"+
			" generates one id per `make` invocation and exports it; set it explicitly"+
			" if you are driving the suite by hand", conformanceRunIDEnv)
	}
	if strings.ContainsAny(id, " \t\r\n") {
		return fmt.Errorf("%s=%q contains whitespace. The stamp is a single line with a"+
			" fixed prefix, so whitespace either splits it into a bogus data line or"+
			" makes the written and compared ids differ invisibly (#lzstalemanifest)",
			conformanceRunIDEnv, id)
	}
	return nil
}

// conformanceEvidenceConfigError refuses a run that was asked for evidence it
// cannot stamp. Checked before a single test executes, because the alternative is
// a full suite whose output is unusable: the guards reject unstamped evidence, so
// the run would be thrown away anyway, after paying for it.
//
// Asking for no evidence at all stays free. A plain `go test` sets neither
// variable and is unaffected, which is what keeps this out of the way of ordinary
// development.
func conformanceEvidenceConfigError() error {
	if os.Getenv("LAZILY_CONFORMANCE_MANIFEST") == "" && os.Getenv("LAZILY_CONFORMANCE_SCENARIOS") == "" {
		return nil
	}
	if err := conformanceRunIDError(); err != nil {
		return fmt.Errorf("this run was asked to record conformance evidence but %w", err)
	}
	return nil
}

// conformanceStampLine is the line every evidence file leads with.
func conformanceStampLine() string {
	return conformanceRunIDPrefix + os.Getenv(conformanceRunIDEnv) + "\n"
}

// evidenceAppendPayload is the exact bytes a contributing test binary appends to
// an evidence file: this invocation's stamp, then the data lines.
//
// One function with two callers rather than the same two-line concatenation
// written out in each flush. The guard joins the manifest against the ledger, so
// a stamp convention that held for one file and not the other would let a fresh
// manifest vouch for a stale ledger — and that is exactly the kind of drift a
// duplicated literal produces.
func evidenceAppendPayload(lines []string) string {
	return conformanceStampLine() + strings.Join(lines, "\n") + "\n"
}

// flushConformanceManifest appends what this binary opened to the path in
// LAZILY_CONFORMANCE_MANIFEST. Append, not truncate: `go test ./...` may run more
// than one binary and each must contribute. A no-op when the variable is unset, so
// a plain `go test` is unaffected.
//
// The append leads with this invocation's run-id stamp (#lzstalemanifest). One
// stamp per contributing binary rather than one per file, because appending is the
// only way several binaries can share a file and no binary knows whether it is
// first. The guard requires every stamp it finds to be the current id, which holds
// for one writer and for ten.
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
	_, _ = f.WriteString(evidenceAppendPayload(ids))
}

// TestMain exists solely to flush the manifest — and the per-scenario replay
// ledger it is joined against (#lzscenariocoverage) — after the package's tests
// finish. A deferred flush in each test would race and truncate; a single exit
// hook is the only place the union is complete.
func TestMain(m *testing.M) {
	// Evidence this run cannot stamp is evidence no guard will accept, so refuse
	// before the suite runs rather than after (#lzstalemanifest).
	if err := conformanceEvidenceConfigError(); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: %v\n", err)
		os.Exit(1)
	}
	// An explicitly-set-but-unusable corpus override is a broken run. Refuse it
	// here, before a single runner gets the chance to skip its way to green or
	// to fall back to the corpus the operator redirected away from
	// (#lzoverrideallrunners).
	if err := specCorpusError(); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: %v\n", err)
		os.Exit(1)
	}
	// Same rule for the schemas root (#lzspecschemasoverride): an explicitly-set
	// LAZILY_SPEC_SCHEMAS_DIR that does not resolve must not degrade to the
	// canonical checkout or to the transcribed fallback vocabulary.
	if err := specSchemasError(); err != nil {
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
	// Rung 0 of the assertion ladder (#lzunboundblockguard). Here for the same
	// reason: only after m.Run is the union of BINDINGS complete, so only here
	// can a block be judged to have reached no runner at all.
	if !checkUnboundAssertionBlocks() && code == 0 {
		code = 1
	}
	os.Exit(code)
}

// ---------------------------------------------------------------------------
// Tests for the run-id stamp (#lzstalemanifest)
// ---------------------------------------------------------------------------

func TestConformanceRunIDErrorRefusesUnusableIDs(t *testing.T) {
	t.Setenv(conformanceRunIDEnv, "")
	if err := conformanceRunIDError(); err == nil {
		t.Fatal("an unset run id must be refused: unstamped evidence is evidence about an unknown run")
	} else if !strings.Contains(err.Error(), conformanceRunIDEnv) {
		t.Fatalf("the refusal must name the variable: %v", err)
	}
	for _, bad := range []string{"has space", "has\ttab", "has\nnewline", "trailing "} {
		t.Setenv(conformanceRunIDEnv, bad)
		if err := conformanceRunIDError(); err == nil {
			t.Fatalf("run id %q contains whitespace and must be refused: a newline splits the "+
				"stamp into a data line, and a trailing space makes the written and compared "+
				"ids differ invisibly", bad)
		}
	}
	t.Setenv(conformanceRunIDEnv, "go-1757000000000000000-4242")
	if err := conformanceRunIDError(); err != nil {
		t.Fatalf("an ordinary Makefile-shaped id must be accepted: %v", err)
	}
}

// TestEvidenceConfigErrorFiresOnlyWhenEvidenceWasRequested pins which runs this
// refusal is allowed to break. A plain `go test` asks for no evidence and must
// stay unaffected — a run-id requirement that applied to every invocation would
// be a tax on ordinary development, and the first workaround anyone reached for
// would be to stop setting the evidence variables at all.
func TestEvidenceConfigErrorFiresOnlyWhenEvidenceWasRequested(t *testing.T) {
	t.Setenv(conformanceRunIDEnv, "")
	t.Setenv("LAZILY_CONFORMANCE_MANIFEST", "")
	t.Setenv("LAZILY_CONFORMANCE_SCENARIOS", "")
	if err := conformanceEvidenceConfigError(); err != nil {
		t.Fatalf("a run that records nothing owes no stamp: %v", err)
	}
	for _, requested := range []string{"LAZILY_CONFORMANCE_MANIFEST", "LAZILY_CONFORMANCE_SCENARIOS"} {
		t.Setenv("LAZILY_CONFORMANCE_MANIFEST", "")
		t.Setenv("LAZILY_CONFORMANCE_SCENARIOS", "")
		t.Setenv(requested, filepath.Join(t.TempDir(), "evidence.txt"))
		if err := conformanceEvidenceConfigError(); err == nil {
			t.Fatalf("%s asks for evidence this run cannot stamp; that must be refused before "+
				"the suite runs, not after", requested)
		}
		t.Setenv(conformanceRunIDEnv, "go-1757000000000000000-4242")
		if err := conformanceEvidenceConfigError(); err != nil {
			t.Fatalf("%s with a usable id must be accepted: %v", requested, err)
		}
		t.Setenv(conformanceRunIDEnv, "")
	}
}

func TestEvidenceAppendPayloadLeadsWithTheStamp(t *testing.T) {
	t.Setenv(conformanceRunIDEnv, "go-1757000000000000000-4242")
	got := evidenceAppendPayload([]string{"signaling/frames.json", "temporal/debounce.json"})
	want := "# lazily-run-id go-1757000000000000000-4242\n" +
		"signaling/frames.json\ntemporal/debounce.json\n"
	if got != want {
		t.Fatalf("evidence payload\n got: %q\nwant: %q", got, want)
	}
	// The stamp must be a COMMENT line, not a data line. The guard strips
	// comments to get its fixture ids, and a stamp it could not distinguish from
	// an id would be resolved against the corpus and reported as a recorder that
	// is mislabelling reads.
	if !strings.HasPrefix(got, "#") {
		t.Fatal("the stamp must be a comment line, or the guard will read it as a fixture id")
	}
}

// TestGuardAndRecorderAgreeOnTheStampPrefix couples the two spellings of the
// stamp. They live in different languages in different files: the recorder writes
// `conformanceRunIDPrefix` and scripts/check-conformance-coverage.sh matches
// `RUN_ID_PREFIX`. Nothing but this test makes them the same string.
//
// Drift here fails in the worst available way. The guard would find no stamp in a
// perfectly fresh file and refuse every run — or, if only the trailing space
// moved, compare a mangled id and report a stale manifest on a green suite. This
// binding has already paid for exactly that class of uncoupled wording: CI's
// reactive-graph guard grepped for "fixtures replayed" while the runner printed
// "fixtures," and failed on a green corpus.
func TestGuardAndRecorderAgreeOnTheStampPrefix(t *testing.T) {
	script := coverageGuardScript(t)
	src, err := os.ReadFile(script)
	if err != nil {
		t.Fatalf("reading %s: %v", script, err)
	}
	want := "RUN_ID_PREFIX='" + conformanceRunIDPrefix + "'"
	if !strings.Contains(string(src), want) {
		t.Fatalf("%s does not spell the stamp prefix the recorder writes.\n"+
			"  recorder: %q\n  expected the script to declare: %s\n"+
			"Two spellings of one line format, in two languages, with nothing coupling them.",
			script, conformanceRunIDPrefix, want)
	}
}

// coverageGuardScript locates the committed guard, or skips.
func coverageGuardScript(t *testing.T) string {
	t.Helper()
	for _, candidate := range coverageScriptCandidates() {
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	t.Fatalf("scripts/check-conformance-coverage.sh not found in %v", coverageScriptCandidates())
	return ""
}

// runCoverageGuard executes the committed guard against synthetic evidence and a
// synthetic (empty) corpus root, and returns its combined output.
//
// The corpus is a real empty directory rather than the canonical checkout, so
// these cases are hermetic: they never read ../lazily-spec, and they cannot be
// affected by what the corpus currently holds. An empty corpus means the guard
// cannot reach a green verdict — which is the point. What is under test is
// whether it refuses the evidence BEFORE it gets that far, and the positive
// control below is that a correctly stamped file gets past this gate and dies of
// the empty corpus instead.
func runCoverageGuard(t *testing.T, runID string, manifest, scenarios []byte) (string, error) {
	t.Helper()
	script, err := filepath.Abs(coverageGuardScript(t))
	if err != nil {
		t.Fatalf("resolving the guard path: %v", err)
	}
	dir := t.TempDir()
	corpus := filepath.Join(dir, "corpus")
	if err := os.MkdirAll(corpus, 0o755); err != nil {
		t.Fatalf("creating the synthetic corpus root: %v", err)
	}
	manifestPath := filepath.Join(dir, "fixtures-loaded.txt")
	scenariosPath := filepath.Join(dir, "scenarios-replayed.txt")
	if err := os.WriteFile(manifestPath, manifest, 0o644); err != nil {
		t.Fatalf("writing the synthetic manifest: %v", err)
	}
	if err := os.WriteFile(scenariosPath, scenarios, 0o644); err != nil {
		t.Fatalf("writing the synthetic ledger: %v", err)
	}

	cmd := exec.Command("bash", script)
	cmd.Dir = filepath.Dir(filepath.Dir(script))
	env := []string{}
	for _, kv := range os.Environ() {
		switch strings.SplitN(kv, "=", 2)[0] {
		case conformanceRunIDEnv, "LAZILY_CONFORMANCE_MANIFEST", "LAZILY_CONFORMANCE_SCENARIOS",
			"LAZILY_SPEC_CONFORMANCE_DIR", "CI":
			continue
		}
		env = append(env, kv)
	}
	env = append(env,
		"LAZILY_SPEC_CONFORMANCE_DIR="+corpus,
		"LAZILY_CONFORMANCE_MANIFEST="+manifestPath,
		"LAZILY_CONFORMANCE_SCENARIOS="+scenariosPath,
	)
	if runID != "" {
		env = append(env, conformanceRunIDEnv+"="+runID)
	}
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// TestCoverageGuardRefusesStaleEvidence is the falsification probe, kept.
//
// The two probes that found this hole were run by hand once: point the guard at
// evidence from another run and watch it refuse. A probe run once proves the
// mechanism worked that afternoon. This runs it on every `make check`, which is
// what makes it a guard rather than an anecdote.
func TestCoverageGuardRefusesStaleEvidence(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	const thisRun = "go-1757000000000000000-4242"
	const otherRun = "go-1699999999999999999-1111"
	fresh := func(id string, lines ...string) []byte {
		return []byte("# lazily-run-id " + id + "\n" + strings.Join(lines, "\n") + "\n")
	}
	manifestLines := []string{"signaling/frames.json"}
	ledgerLines := []string{"signaling/frames.json\tsends_a_frame"}

	cases := []struct {
		name      string
		runID     string
		manifest  []byte
		scenarios []byte
		want      []string
	}{{
		// Protocol point 4: absent the variable the guard REFUSES. Skipping the
		// freshness check whenever the variable happens to be unset would be the
		// original hole with one more step in front of it.
		name:      "run id unset",
		runID:     "",
		manifest:  fresh(thisRun, manifestLines...),
		scenarios: fresh(thisRun, ledgerLines...),
		want:      []string{"LAZILY_CONFORMANCE_RUN_ID is unset"},
	}, {
		// Probe 1: the mechanism. Evidence from another run, read under this
		// run's id.
		name:      "manifest written by another run",
		runID:     thisRun,
		manifest:  fresh(otherRun, manifestLines...),
		scenarios: fresh(thisRun, ledgerLines...),
		want:      []string{"conformance manifest", "DIFFERENT run", otherRun, thisRun},
	}, {
		// The ledger is checked too. The guard JOINS the two files, so trusting
		// one and not the other would let a fresh manifest vouch for a stale
		// ledger.
		name:      "ledger written by another run",
		runID:     thisRun,
		manifest:  fresh(thisRun, manifestLines...),
		scenarios: fresh(otherRun, ledgerLines...),
		want:      []string{"scenario ledger", "DIFFERENT run", otherRun},
	}, {
		// An evidence file predating this change carries no stamp at all. That
		// is the same unknown provenance as a wrong stamp and fails the same way.
		name:      "unstamped manifest",
		runID:     thisRun,
		manifest:  []byte(strings.Join(manifestLines, "\n") + "\n"),
		scenarios: fresh(thisRun, ledgerLines...),
		want:      []string{"carries no", "lazily-run-id"},
	}, {
		// One stamp of several belonging to another run is still stale: several
		// test binaries append to one file, so the rule is EVERY stamp, not the
		// first one. Checking only the first would be satisfied by a truncating
		// writer that ran first and a cached one that did not run at all.
		name:      "one contributing binary's stamp is stale",
		runID:     thisRun,
		manifest:  append(fresh(thisRun, manifestLines...), fresh(otherRun, "temporal/debounce.json")...),
		scenarios: fresh(thisRun, ledgerLines...),
		want:      []string{"DIFFERENT run", otherRun},
	}, {
		// A stamp with no data under it is a recorder that attached and recorded
		// nothing — the vacuous green, one layer in.
		name:      "stamped but empty",
		runID:     thisRun,
		manifest:  []byte("# lazily-run-id " + thisRun + "\n"),
		scenarios: fresh(thisRun, ledgerLines...),
		want:      []string{"NO data lines"},
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := runCoverageGuard(t, tc.runID, tc.manifest, tc.scenarios)
			if err == nil {
				t.Fatalf("the guard accepted evidence it must refuse.\n%s", out)
			}
			for _, want := range tc.want {
				if !strings.Contains(out, want) {
					t.Fatalf("the refusal must name %q so the reader can act on it; got:\n%s", want, out)
				}
			}
		})
	}

	// The positive control, and the half that a refusal test cannot supply: a
	// correctly stamped pair gets PAST the freshness gate. Without this, a guard
	// that refused unconditionally — one typo in the comparison — would pass every
	// case above.
	t.Run("correctly stamped evidence passes the freshness gate", func(t *testing.T) {
		if _, err := exec.LookPath("jq"); err != nil {
			t.Skip("jq not available; the guard exits at its jq check before reaching a corpus verdict")
		}
		out, err := runCoverageGuard(t, thisRun, fresh(thisRun, manifestLines...), fresh(thisRun, ledgerLines...))
		// It still fails: the synthetic corpus is empty, so no fixture the
		// manifest names resolves. That is the point — the failure has to be
		// about the CORPUS, never about freshness.
		if err == nil {
			t.Fatalf("an empty corpus cannot yield a verdict of OK:\n%s", out)
		}
		for _, unwanted := range []string{"lazily-run-id", "DIFFERENT run", "LAZILY_CONFORMANCE_RUN_ID"} {
			if strings.Contains(out, unwanted) {
				t.Fatalf("correctly stamped evidence was refused as stale (%q):\n%s", unwanted, out)
			}
		}
		if !strings.Contains(out, "conformance coverage FAILED") {
			t.Fatalf("expected the guard to reach its corpus rungs and fail there; got:\n%s", out)
		}
	})
}
