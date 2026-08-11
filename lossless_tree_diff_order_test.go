package lazily

// Pins the canonical `(counter, peer)` ordering of LosslessTreeCrdt.Diff
// (#lzdifforderallbindings).
//
// The order is a CROSS-BINDING CONTRACT, not an implementation detail: the
// shared lazily-spec corpus addresses diff ops POSITIONALLY. For example
// `conformance/lossless-tree/non_contiguous_anti_entropy.json` says
// `deliver.only: [0, 2]`, which indexes into whatever `Diff` returned. That
// fixture only means the same thing in every binding while every binding
// returns ops in the same order.
//
// The corpus cannot police this. Measured in lazily-zig (commit e8a2a28,
// #lzzigdiffmutant): reversing the sort, or deleting it outright, left the
// entire suite green — including the anti-entropy fixture, because the two
// selected indices pick the same SET either way and ApplyUpdate is
// order-tolerant by design. Only a direct assertion catches it.

import (
	"sort"
	"testing"
)

func TestLosslessTreeDiffReturnsOpsInCanonicalCounterPeerOrder(t *testing.T) {
	a := NewLosslessTreeCrdt(1)
	para := a.CreateNode(TreeRoot, nil, TreeNodeSeedElement{Kind: "para"})
	base := a.CreateNode(para, nil, TreeNodeSeedLeaf{Kind: LeafKindTrivia, Text: "0"})

	b := a.Fork(2)

	// a runs ahead to counter 4; b's single op stays at counter 3, so when it
	// arrives it lands LAST in a's log while sorting EARLIER than a's counter-4
	// op. Without that skew the log order and the canonical order coincide and
	// the strictly-increasing assertion below would hold for an unsorted or
	// reversed Diff too, pinning nothing.
	one := a.CreateNode(para, &base, TreeNodeSeedLeaf{Kind: LeafKindTrivia, Text: "1"})
	a.CreateNode(para, &one, TreeNodeSeedLeaf{Kind: LeafKindTrivia, Text: "2"})
	b.CreateNode(para, &base, TreeNodeSeedLeaf{Kind: LeafKindTrivia, Text: "9"})

	fromB := b.Diff(a.Frontier())
	a.ApplyUpdate(fromB)

	all := a.Diff(NewTreeVersionFrontier())

	// Non-vacuity gate, derived from the LOG ALONE so a broken Diff cannot
	// influence it: assert the log is genuinely NOT already in canonical order.
	// If a future refactor makes them coincide this fails loudly instead of
	// letting the ordering check below silently become vacuous.
	if len(all.Ops) != len(a.log) {
		t.Fatalf("diff against an empty frontier must return the whole log: got %d ops, log has %d", len(all.Ops), len(a.log))
	}
	canonical := append([]TreeOp(nil), a.log...)
	sort.SliceStable(canonical, func(i, j int) bool { return canonical[i].Id.Compare(canonical[j].Id) < 0 })
	logIsCanonical := true
	for i, logged := range a.log {
		if logged.Id != canonical[i].Id {
			logIsCanonical = false
			break
		}
	}
	if logIsCanonical {
		t.Fatalf("test is vacuous: log order already equals canonical order (log=%v); rebuild the scenario so a late-arriving remote op sorts earlier", treeOpIds(a.log))
	}

	// The contract: strictly increasing by (counter, peer).
	for i := 1; i < len(all.Ops); i++ {
		if all.Ops[i-1].Id.Compare(all.Ops[i].Id) >= 0 {
			t.Fatalf("Diff must return ops in canonical (counter, peer) order; got %v (op %d %s does not precede op %d %s)",
				treeOpIds(all.Ops), i-1, all.Ops[i-1].Id, i, all.Ops[i].Id)
		}
	}
}

func treeOpIds(ops []TreeOp) []string {
	out := make([]string, len(ops))
	for i, op := range ops {
		out[i] = op.Id.String()
	}
	return out
}
