// Phase 1 law-tests for the merge algebra (#relaycell).
//
// Every policy MUST be associative; commutativity/idempotency are asserted per
// flag. Replays the cross-language mergecell_algebra.json fixture — lazily-go
// converges identically to lazily-rs / lazily-js / lazily-py.

package lazily

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func eqIntSlice(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestMergeAssociativity(t *testing.T) {
	// Associativity over int policies.
	for _, p := range []MergePolicy[int]{KeepLatest[int](), Sum[int](), Max[int]()} {
		a, b, c := 5, -3, 8
		if p.Merge(p.Merge(a, b), c) != p.Merge(a, p.Merge(b, c)) {
			t.Fatalf("%s associativity", p.Name)
		}
	}
	// RawFifo (slice) is associative too.
	rf := RawFifo[int]()
	l := rf.Merge(rf.Merge([]int{1}, []int{2}), []int{3})
	r := rf.Merge([]int{1}, rf.Merge([]int{2}, []int{3}))
	if !eqIntSlice(l, r) {
		t.Fatalf("RawFifo associativity")
	}
}

func TestMergeCommutativityMatchesFlag(t *testing.T) {
	for _, p := range []MergePolicy[int]{Sum[int](), Max[int]()} {
		a, b, c := 5, -3, 8
		if p.Commutative && p.Merge(p.Merge(a, b), c) != p.Merge(p.Merge(a, c), b) {
			t.Fatalf("%s should be commutative", p.Name)
		}
	}
	// KeepLatest is NOT commutative.
	kl := KeepLatest[int]()
	if kl.Merge(kl.Merge(0, 1), 2) == kl.Merge(kl.Merge(0, 2), 1) {
		t.Fatalf("KeepLatest must not be commutative")
	}
	if kl.Commutative {
		t.Fatalf("KeepLatest.Commutative flag should be false")
	}
}

func TestMergeIdempotencyMatchesFlag(t *testing.T) {
	mx := Max[int]()
	if mx.Merge(mx.Merge(3, 9), 9) != mx.Merge(3, 9) {
		t.Fatalf("Max should be idempotent")
	}
	// Sum is NOT idempotent.
	sm := Sum[int]()
	if sm.Merge(sm.Merge(0, 5), 5) == sm.Merge(0, 5) {
		t.Fatalf("Sum must not be idempotent")
	}
	if sm.Idempotent {
		t.Fatalf("Sum.Idempotent flag should be false")
	}
}

func TestSetUnionAndRawFifoFlags(t *testing.T) {
	su := SetUnion[int]()
	if !su.Commutative || !su.Idempotent || !su.Conflates {
		t.Fatalf("SetUnion flags")
	}
	rf := RawFifo[int]()
	if rf.Commutative || rf.Idempotent || rf.Conflates {
		t.Fatalf("RawFifo flags: order+multiplicity are meaning, cannot conflate")
	}
}

func TestCellIsMergeCellKeepLatest(t *testing.T) {
	ctx := NewContext()
	cell := NewCell(ctx, 0)
	mc := NewMergeCell(ctx, 0, KeepLatest[int]())
	for _, v := range []int{3, 3, 7, 7, 1} {
		cell.Set(v)
		mc.Merge(v)
		if cell.Get() != mc.Get() {
			t.Fatalf("keep-latest diverged at %d", v)
		}
	}
	if mc.Get() != 1 {
		t.Fatalf("want 1, got %d", mc.Get())
	}
}

func TestSumConvergesRegardlessOfOrder(t *testing.T) {
	ctx := NewContext()
	ops := []int{5, -3, 8, 2, -1}
	a := NewMergeCell(ctx, 0, Sum[int]())
	for _, d := range ops {
		a.Merge(d)
	}
	b := NewMergeCell(ctx, 0, Sum[int]())
	for i := len(ops) - 1; i >= 0; i-- {
		b.Merge(ops[i])
	}
	if a.Get() != b.Get() || a.Get() != 11 {
		t.Fatalf("want 11==11, got %d, %d", a.Get(), b.Get())
	}
}

func TestIdempotentMergeNoOpsViaGuard(t *testing.T) {
	ctx := NewContext()
	mc := NewMergeCell(ctx, 10, Max[int]())
	runs := 0
	NewEffect(ctx, func(ctx *Context) func() {
		mc.Get()
		runs++
		return nil
	})
	if runs != 1 {
		t.Fatalf("initial run, got %d", runs)
	}
	mc.Merge(5)
	mc.Merge(10)
	mc.Merge(0)
	if runs != 1 {
		t.Fatalf("merges at/below max must not rerun; got %d", runs)
	}
	mc.Merge(42)
	if mc.Get() != 42 || runs != 2 {
		t.Fatalf("want 42/2, got %d/%d", mc.Get(), runs)
	}
}

type mergeScenario struct {
	Policy string `json:"policy"`
	Flags  struct {
		Commutative bool `json:"commutative"`
		Idempotent  bool `json:"idempotent"`
	} `json:"flags"`
	Initial int `json:"initial"`
	Steps   []struct {
		Merge    int `json:"merge"`
		Expected struct {
			Value       int  `json:"value"`
			Invalidates bool `json:"invalidates"`
		} `json:"expected"`
	} `json:"steps"`
}

func TestMergeCellAlgebraFixture(t *testing.T) {
	var data []byte
	for _, path := range []string{
		filepath.Join("..", "lazily-spec", "conformance", "collections", "mergecell_algebra.json"),
	} {
		if b, err := os.ReadFile(path); err == nil {
			data = b
			break
		}
	}
	if data == nil {
		t.Skip("mergecell_algebra.json fixture not present as sibling")
	}
	var fixture struct {
		Scenarios []mergeScenario `json:"scenarios"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	policies := map[string]MergePolicy[int]{
		"KeepLatest": KeepLatest[int](),
		"Sum":        Sum[int](),
		"Max":        Max[int](),
	}
	seen := 0
	for _, sc := range fixture.Scenarios {
		p := policies[sc.Policy]
		if p.Commutative != sc.Flags.Commutative || p.Idempotent != sc.Flags.Idempotent {
			t.Fatalf("%s flags mismatch", sc.Policy)
		}
		ctx := NewContext()
		mc := NewMergeCell(ctx, sc.Initial, p)
		runs := 0
		NewEffect(ctx, func(ctx *Context) func() {
			mc.Get()
			runs++
			return nil
		})
		for i, step := range sc.Steps {
			before := runs
			mc.Merge(step.Merge)
			fired := runs > before
			if mc.Get() != step.Expected.Value {
				t.Fatalf("%s step %d value: want %d got %d", sc.Policy, i, step.Expected.Value, mc.Get())
			}
			if fired != step.Expected.Invalidates {
				t.Fatalf("%s step %d invalidates: want %v got %v", sc.Policy, i, step.Expected.Invalidates, fired)
			}
		}
		seen++
	}
	if seen != 3 {
		t.Fatalf("want 3 scenarios, saw %d", seen)
	}
}
