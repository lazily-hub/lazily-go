package lazily

// Wires the sibling lazily-formal Lean 4 model into the Go test suite.
//
// The Go state-chart / reactive / collection / CRDT code mirrors universal
// theorems in the sibling lazily-formal submodule (LazilyFormal.StateChart /
// StateMachine / Reactive / Collection / Tree / Reconciliation /
// AsyncComputedState). Those theorems are only trustworthy if the model compiles,
// so this test runs `lake build` when the sibling checkout + Lean toolchain are
// present (full repo checkout / CI) and SKIPs gracefully otherwise (module
// consumer via `go get`, shallow clone, no Lean toolchain) so the Go-only tests
// still run. CI uses a full checkout + elan, so the proofs are verified there.

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func TestLazilyFormalProofsCompile(t *testing.T) {
	formalDir := resolveFormalDir()
	if formalDir == "" {
		t.Skip("lazily-formal sibling not present — clone with --recurse-submodules " +
			"(or set LAZILY_FORMAL_PATH) to enable Lean proof verification")
	}
	if _, err := exec.LookPath("lake"); err != nil {
		t.Skip("`lake` (Lean toolchain) not on PATH — install Lean via elan " +
			"(https://lean-lang.org/lean4/doc/setup.html) to enable proof verification")
	}

	t.Logf("[formal-check] building lazily-formal at %s ...", formalDir)
	cmd := exec.Command("lake", "build")
	cmd.Dir = formalDir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("lazily-formal `lake build` failed — a theorem regressed: %v\n%s", err, out)
	}
	t.Logf("[formal-check] OK — all Lean proofs in lazily-formal compile.")
}

// resolveFormalDir returns the path to a real lazily-formal checkout, or "" when
// none is present. Honors LAZILY_FORMAL_PATH, then the in-repo submodule layout
// (src/lazily-go <-> src/lazily-formal), relative to this source file.
func resolveFormalDir() string {
	var candidates []string
	if p := os.Getenv("LAZILY_FORMAL_PATH"); p != "" {
		candidates = append(candidates, p)
	}
	if _, file, _, ok := runtime.Caller(0); ok {
		here := filepath.Dir(file)
		candidates = append(candidates,
			filepath.Join(here, "..", "lazily-formal"), // src/lazily-go <-> src/lazily-formal
			filepath.Join(here, "lazily-formal"),
		)
	}
	if wd, err := os.Getwd(); err == nil {
		candidates = append(candidates, filepath.Join(wd, "..", "lazily-formal"))
	}
	for _, c := range candidates {
		if isFormalDir(c) {
			if resolved, err := filepath.EvalSymlinks(c); err == nil {
				return resolved
			}
			return c
		}
	}
	return ""
}

// isFormalDir reports whether dir looks like a real lazily-formal checkout — it
// ships these two markers.
func isFormalDir(dir string) bool {
	if fi, err := os.Stat(filepath.Join(dir, "lakefile.lean")); err != nil || fi.IsDir() {
		return false
	}
	if fi, err := os.Stat(filepath.Join(dir, "LazilyFormal")); err != nil || !fi.IsDir() {
		return false
	}
	return true
}
