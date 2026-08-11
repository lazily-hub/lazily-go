package lazily

// Shared conformance-corpus seam (#lzoverrideallrunners).
//
// Every conformance runner in this package resolves fixture paths through the
// helpers below instead of spelling the corpus location itself. Before this
// existed, `LAZILY_SPEC_CONFORMANCE_DIR` was honoured by exactly one runner (the
// materialization/IPC fixture loader) while ~25 other read sites hardcoded the
// sibling checkout. A corpus-perturbation probe could therefore point "the
// suite" at a scratch copy and have almost none of it move — and the coverage
// guard, which reads the same variable, would audit a DIFFERENT corpus than the
// one the tests replayed. Two corpora, one green.
//
// Contract:
//
//   - Variable UNSET: the roots are the historical ones, in the historical
//     preference order (canonical sibling first, then the vendored offline
//     mirror, then the two legacy in-repo locations).
//   - Variable SET: it REPLACES every root. There is no vendored fallback, so a
//     perturbed scratch corpus cannot be silently rescued by a pristine mirror.
//   - Variable SET but unreadable: a loud failure, never a skip. See
//     TestSpecCorpusOverrideIsReadable and the guard in TestMain.
//
// Attribution (the conformance manifest and the scenario ledger) is computed
// RELATIVE TO THE RESOLVED ROOT rather than by scanning for a hardcoded
// "lazily-spec/conformance/" substring. A scratch root does not contain that
// literal, so the substring rule silently stopped recording under an override —
// which is how a perturbation probe becomes a vacuous green.

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// specDirEnv names the corpus-root override. `scripts/check-conformance-coverage.sh`
// reads the same variable, so the suite and the guard auditing it always see one
// corpus.
const specDirEnv = "LAZILY_SPEC_CONFORMANCE_DIR"

// specRepoSegment is the sibling checkout's directory name, assembled rather
// than written whole so TestNoSourceFileSpellsTheCorpusPathItself has a single
// exempt definition site.
const specRepoSegment = "lazily" + "-spec"

// conformanceMarker is the legacy attribution substring. Root-relative
// attribution supersedes it; it survives only as a fallback for absolute paths
// that were built outside this seam.
const conformanceMarker = specRepoSegment + string(filepath.Separator) + "conformance" + string(filepath.Separator)

var (
	specRootsOnce sync.Once
	specRootsVal  []string
	specOverride  string
	specRootErr   error
)

func resolveSpecRoots() {
	specRootsOnce.Do(func() {
		override := strings.TrimSpace(os.Getenv(specDirEnv))
		if override != "" {
			specOverride = override
			info, err := os.Stat(override)
			switch {
			case err != nil:
				specRootErr = fmt.Errorf(
					"%s=%q is set but cannot be read: %w. An explicit corpus override that "+
						"does not resolve is a broken run, not an absent checkout: falling back "+
						"to the sibling or vendored corpus here would replay the corpus the "+
						"operator explicitly redirected AWAY from, and report green about it",
					specDirEnv, override, err)
			case !info.IsDir():
				specRootErr = fmt.Errorf(
					"%s=%q is set but is not a directory. It must point at a conformance "+
						"corpus ROOT (the directory holding collections/, reliable-sync/, ...)",
					specDirEnv, override)
			default:
				if _, err := os.ReadDir(override); err != nil {
					specRootErr = fmt.Errorf(
						"%s=%q is set but its contents cannot be listed: %w",
						specDirEnv, override, err)
					return
				}
				specRootsVal = []string{override}
			}
			return
		}

		roots := []string{
			filepath.Join("..", specRepoSegment, "conformance"),
			// The vendored offline mirror. TestVendoredFixturesMatchCanonical
			// holds it byte-identical to the canonical corpus whenever the
			// sibling is present.
			filepath.Join("test", "conformance"),
			"conformance",
			filepath.Join("testdata", "conformance"),
		}
		// Source-relative duplicates, for a run whose working directory is not
		// the package directory. Previously open-coded in three loaders.
		if _, file, _, ok := runtime.Caller(0); ok {
			dir := filepath.Dir(file)
			roots = append(roots,
				filepath.Join(dir, "..", specRepoSegment, "conformance"),
				filepath.Join(dir, "test", "conformance"),
			)
		}
		specRootsVal = roots
	})
}

// specCorpusError reports why an explicitly-set override is unusable, or nil.
func specCorpusError() error {
	resolveSpecRoots()
	return specRootErr
}

// specCorpusOverridden reports whether the corpus root was explicitly redirected.
func specCorpusOverridden() bool {
	resolveSpecRoots()
	return specOverride != ""
}

// specCorpusRoots returns the corpus roots in preference order. An explicit
// override collapses the list to exactly one entry.
func specCorpusRoots() []string {
	resolveSpecRoots()
	return specRootsVal
}

// specDir is the primary corpus root: the override when set, otherwise the
// canonical sibling checkout.
func specDir() string {
	roots := specCorpusRoots()
	if len(roots) == 0 {
		// Only reachable when the override is broken; every caller is already
		// failing loudly via TestMain / TestSpecCorpusOverrideIsReadable. Return
		// a path that cannot resolve rather than silently reading the canonical
		// corpus the operator redirected away from.
		return filepath.Join(string(filepath.Separator), "nonexistent-conformance-corpus")
	}
	return roots[0]
}

// specPath joins path segments onto the primary corpus root.
func specPath(parts ...string) string {
	return filepath.Join(append([]string{specDir()}, parts...)...)
}

// specCandidatePaths returns one candidate path per corpus root, in preference
// order. Under an override there is exactly one candidate — the vendored mirror
// is NOT consulted.
func specCandidatePaths(parts ...string) []string {
	roots := specCorpusRoots()
	out := make([]string, 0, len(roots))
	for _, root := range roots {
		out = append(out, filepath.Join(append([]string{root}, parts...)...))
	}
	return out
}

// specSchemasDir is the sibling checkout's JSON-schema directory. It is
// deliberately NOT redirected by the corpus override: the override names a
// conformance corpus root, and a scratch copy of that corpus carries no
// schemas/ sibling.
func specSchemasDir() string {
	return filepath.Join("..", specRepoSegment, "schemas")
}

// specFixtureMissing reports a fixture the runner could not open. It skips when
// no corpus was requested explicitly (a contributor without the sibling
// checkout is not making a false claim) and FAILS when one was: an override that
// is missing a fixture the runner replays must not turn a red into a skip.
func specFixtureMissing(t *testing.T, format string, args ...any) {
	t.Helper()
	if specCorpusOverridden() {
		t.Fatalf(format+" [%s=%s is set; a redirected corpus that cannot supply a "+
			"fixture is a failure, not a skip]", append(append([]any{}, args...), specDirEnv, specOverride)...)
	}
	t.Skipf(format, args...)
}

// specCanonicalRelative returns the corpus-relative, slash-separated id of a
// path under the PRIMARY root — the key the conformance manifest and the
// scenario ledger record. Reads served by the vendored mirror deliberately do
// not attribute here; that is what makes a mirror read invisible to the coverage
// guard, by design.
func specCanonicalRelative(path string) (string, bool) {
	if id, ok := relativeToRoot(specDir(), path); ok {
		return id, true
	}
	return legacyMarkerRelative(path)
}

// specAnyRootRelative returns the corpus-relative id of a path under ANY root,
// mirror included. The prose-verification ledger uses this: a fixture replayed
// from the offline fallback owes its paragraphs a discharge exactly as the
// canonical one does.
func specAnyRootRelative(path string) (string, bool) {
	for _, root := range specCorpusRoots() {
		if id, ok := relativeToRoot(root, path); ok {
			return id, true
		}
	}
	return legacyMarkerRelative(path)
}

func relativeToRoot(root, path string) (string, bool) {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", false
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", false
	}
	rel, err := filepath.Rel(absRoot, absPath)
	if err != nil {
		return "", false
	}
	if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", false
	}
	return filepath.ToSlash(rel), true
}

func legacyMarkerRelative(path string) (string, bool) {
	abs, err := filepath.Abs(path)
	if err != nil {
		abs = path
	}
	for _, marker := range []string{
		conformanceMarker,
		"test" + string(filepath.Separator) + "conformance" + string(filepath.Separator),
		"testdata" + string(filepath.Separator) + "conformance" + string(filepath.Separator),
	} {
		if idx := strings.Index(abs, marker); idx != -1 {
			return filepath.ToSlash(abs[idx+len(marker):]), true
		}
	}
	return "", false
}

// ---------------------------------------------------------------------------
// Self-enforcement
// ---------------------------------------------------------------------------

// TestSpecCorpusOverrideIsReadable turns a broken override into a named test
// failure. TestMain also refuses to run in that state, so a filtered `go test
// -run` cannot slip past this one.
func TestSpecCorpusOverrideIsReadable(t *testing.T) {
	if err := specCorpusError(); err != nil {
		t.Fatal(err)
	}
	if !specCorpusOverridden() {
		return
	}
	roots := specCorpusRoots()
	if len(roots) != 1 || roots[0] != specOverride {
		t.Fatalf("override %q did not replace the corpus roots: %v", specOverride, roots)
	}
}

// TestNoSourceFileSpellsTheCorpusPathItself is what makes the seam hold BY
// CONSTRUCTION. A new runner that hardcodes the sibling checkout would honour no
// override and reintroduce the two-corpora split, and no reviewer would notice
// one more `filepath.Join("..", "lazily-spec", "conformance", ...)` among two
// dozen. So the package's own sources are the fixture here.
//
// The scan is AST-based, not textual: only string LITERALS are examined, so
// prose in comments and in skip/failure messages is unaffected.
//
// HARDENING (#lzcorpusrootguards). The first version of this guard was PROVEN
// EVADABLE — three synthetic violations were injected into a scratch copy and it
// still reported green:
//
//  1. It filtered on `*_test.go`, so a NON-test file in package lazily could
//     spell the corpus freely. Fixed: every `.go` file is scanned.
//  2. It called os.ReadDir and `continue`d on entry.IsDir(), so a test file in
//     any SUBDIRECTORY was invisible. Fixed: the scan walks the tree.
//  3. Its predicate was `value == "lazily-spec" || contains "lazily-spec/conformance"`,
//     so `filepath.Join("../lazily-spec", "conformance", ...)` (the checkout name
//     fused to its parent in one literal) and `"lazily"+"-spec"` (the segment
//     split across a concatenation) both slipped through. Fixed: constant string
//     expressions are FOLDED before matching, filepath.Join/path.Join argument
//     lists are reassembled into a single candidate path, and the predicate
//     matches `lazily-spec` as a whole PATH SEGMENT rather than as a whole
//     literal.
//
// Forms this catches:
//
//   - any literal, or `+`-concatenation of literals, whose slash-separated form
//     has `lazily-spec` as a path segment ("lazily-spec", "../lazily-spec",
//     "../lazily-spec/conformance/x", "lazily"+"-spec");
//   - any literal or fold containing the fragment `lazily-spec/conformance`
//     anywhere, even mid-segment;
//   - filepath.Join / path.Join calls whose literal arguments reassemble into
//     either of the above, including when non-constant arguments sit between the
//     literals.
//
// Forms it deliberately does NOT catch, because a string test cannot see them:
//
//   - values assembled at RUNTIME from non-constant parts — fmt.Sprintf("%s-spec", x),
//     os.Getenv, a string built in a loop. A determined author can always evade a
//     source-level guard; the point is that nobody does it by ACCIDENT, and doing
//     it on purpose is no longer deniable as an oversight.
//   - an identifier alias (`const s = "lazily" + "-spec"` used as `Join("..", s)`).
//     The alias's own declaration folds to "lazily-spec" and is reported at the
//     declaration site, so the offense still surfaces — just one line earlier.
//   - the sibling's non-corpus siblings, e.g. `../lazily-spec/schemas`, are NOT
//     exempt: specSchemasDir is the one deliberate reference and it lives in this
//     seam file, which is the single allowlisted definition site (along with the
//     specRepoSegment const itself).
func TestNoSourceFileSpellsTheCorpusPathItself(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate this source file")
	}
	dir := filepath.Dir(thisFile)
	seam := filepath.Base(thisFile)

	files, literals, err := scanForCorpusSpellings(t, dir, seam)
	if err != nil {
		t.Fatalf("walking package tree: %v", err)
	}
	// POSITIVE EVIDENCE. A guard that examined nothing is indistinguishable from
	// a guard that found nothing, and the second reads as green. Both counters
	// must be nonzero or the run proves nothing.
	if files == 0 {
		t.Fatalf("examined nothing: the walk of %s parsed 0 Go files, so this guard "+
			"asserted nothing about the corpus seam", dir)
	}
	if literals == 0 {
		t.Fatalf("examined nothing: the walk of %s parsed %d Go files but inspected 0 "+
			"string literals, so this guard asserted nothing about the corpus seam", dir, files)
	}
	t.Logf("%d Go files (%d string literals) route the conformance corpus through the shared seam", files, literals)
}

// corpusGuardSkipDirs are trees the walk refuses to descend into: version
// control and tooling state, and any vendored or third-party checkout, whose
// sources are not ours to route through this package's seam. `testdata` is
// skipped for the same reason the go tool ignores it — nothing in it is built.
var corpusGuardSkipDirs = map[string]bool{
	"vendor":       true,
	"node_modules": true,
	"third_party":  true,
	"testdata":     true,
}

// scanForCorpusSpellings walks every Go file under root (seam excluded) and
// reports each hardcoded corpus spelling via t.Errorf. It returns the number of
// files parsed and string literals inspected so the caller can prove the walk
// was not vacuous.
func scanForCorpusSpellings(t *testing.T, root, seam string) (files, literals int, err error) {
	t.Helper()
	fset := token.NewFileSet()
	walkErr := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			name := entry.Name()
			if path != root && (strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") || corpusGuardSkipDirs[name]) {
				return filepath.SkipDir
			}
			return nil
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || (path == filepath.Join(root, seam)) {
			return nil
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			rel = path
		}
		// Mode 0: comments are not attached, and only *ast.BasicLit nodes are
		// examined, so prose is exempt for free.
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return fmt.Errorf("parsing %s: %w", rel, parseErr)
		}
		files++
		ast.Inspect(file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.BasicLit:
				value, ok := foldStringExpr(node)
				if !ok {
					return true
				}
				literals++
				return !reportCorpusSpelling(t, fset, rel, node, value, "string literal")
			case *ast.BinaryExpr:
				// A concatenation of literals: fold it before matching so
				// "lazily"+"-spec" cannot hide the segment in the seam between
				// two innocent halves.
				value, ok := foldStringExpr(node)
				if !ok {
					return true
				}
				return !reportCorpusSpelling(t, fset, rel, node, value, "concatenated string expression")
			case *ast.CallExpr:
				value, ok := foldPathJoin(node)
				if !ok {
					return true
				}
				return !reportCorpusSpelling(t, fset, rel, node, value, "assembled Join path")
			}
			return true
		})
		return nil
	})
	return files, literals, walkErr
}

// reportCorpusSpelling fails the guard when value names the sibling corpus, and
// reports whether it did so (the caller stops descending, so one offense is not
// counted once per nested sub-expression).
func reportCorpusSpelling(t *testing.T, fset *token.FileSet, rel string, n ast.Node, value, kind string) bool {
	if !spellsCorpusRoot(value) {
		return false
	}
	t.Errorf("%s:%d: %s %q spells the corpus location directly. "+
		"Route it through specPath/specCandidatePaths/specDir instead — a hardcoded "+
		"root ignores %s and splits the suite across two corpora "+
		"(#lzoverrideallrunners, #lzcorpusrootguards).",
		rel, fset.Position(n.Pos()).Line, kind, value, specDirEnv)
	return true
}

// spellsCorpusRoot is the widened predicate. It matches the sibling checkout as
// a whole PATH SEGMENT — so a joined prefix ("../lazily-spec") is caught where a
// whole-literal equality test was not — and separately matches the corpus
// fragment anywhere in the value.
func spellsCorpusRoot(value string) bool {
	normalized := filepath.ToSlash(value)
	if strings.Contains(normalized, specRepoSegment+"/conformance") {
		return true
	}
	for _, segment := range strings.Split(normalized, "/") {
		if segment == specRepoSegment {
			return true
		}
	}
	return false
}

// foldStringExpr constant-folds string literals and `+` chains of them. It
// resolves no identifiers: see the "deliberately does NOT catch" list above.
func foldStringExpr(e ast.Expr) (string, bool) {
	switch node := e.(type) {
	case *ast.BasicLit:
		if node.Kind != token.STRING {
			return "", false
		}
		value, err := strconv.Unquote(node.Value)
		if err != nil {
			return "", false
		}
		return value, true
	case *ast.ParenExpr:
		return foldStringExpr(node.X)
	case *ast.BinaryExpr:
		if node.Op != token.ADD {
			return "", false
		}
		left, ok := foldStringExpr(node.X)
		if !ok {
			return "", false
		}
		right, ok := foldStringExpr(node.Y)
		if !ok {
			return "", false
		}
		return left + right, true
	}
	return "", false
}

// foldPathJoin reassembles a filepath.Join / path.Join argument list into one
// candidate path. Non-constant arguments become an opaque placeholder rather
// than aborting the fold, so `filepath.Join("..", specRepoSegment, "conformance")`
// still yields a value whose literal parts can be matched — and so a violation
// interleaved with variables is not laundered by the variables.
func foldPathJoin(call *ast.CallExpr) (string, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Join" {
		return "", false
	}
	pkg, ok := sel.X.(*ast.Ident)
	if !ok || (pkg.Name != "filepath" && pkg.Name != "path") {
		return "", false
	}
	parts := make([]string, 0, len(call.Args))
	sawLiteral := false
	for _, arg := range call.Args {
		if value, ok := foldStringExpr(arg); ok {
			sawLiteral = true
			parts = append(parts, value)
			continue
		}
		// A placeholder that occupies its own segment, so it can never itself
		// be — nor silently complete — the segment under test.
		parts = append(parts, "<expr>")
	}
	if !sawLiteral {
		return "", false
	}
	return strings.Join(parts, "/"), true
}

// TestSpecSeamAttributesRelativeToTheResolvedRoot pins the defect that made the
// override unsafe: attribution keyed on a hardcoded "lazily-spec/conformance/"
// substring stops recording the moment the corpus lives anywhere else, and a
// probe against a scratch copy then reports coverage over nothing.
func TestSpecSeamAttributesRelativeToTheResolvedRoot(t *testing.T) {
	const want = "reliable-sync/liveness_orset_lww.json"
	got, ok := specCanonicalRelative(specPath("reliable-sync", "liveness_orset_lww.json"))
	if !ok {
		t.Fatalf("a path under the resolved root %q did not attribute", specDir())
	}
	if got != want {
		t.Fatalf("attribution = %q, want %q", got, want)
	}
	if _, ok := specCanonicalRelative(filepath.Join(t.TempDir(), "elsewhere.json")); ok {
		t.Fatal("a path outside every corpus root attributed anyway")
	}
}
