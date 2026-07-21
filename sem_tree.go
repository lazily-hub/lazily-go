// Memoized semantic tree — a reactive, incrementally-memoized fold tree.
//
// One Memo slot per node folds (node value, [child derived values]). Editing one
// node recomputes only its ANCESTOR CHAIN; a sibling subtree's derived slot stays
// cached. A node edit that does not change the folded value does not re-run a
// downstream consumer (the Memo equality guard).
//
// Composes over the reactive Context/Cell/Memo primitives in core.go. Ported from
// lazily-dart lib/src/sem_tree.dart (which mirrors lazily-js src/sem-tree.js).
// Conforms to lazily-spec conformance/collections/semtree_incremental.json.
package lazily

import (
	"fmt"
	"sort"
)

// FoldFn combines a node's value with its children's derived values.
type FoldFn[V, D any] func(value V, childDerived []D) D

// TreeNodeSpec is a node spec for building a SemTree.
type TreeNodeSpec[V any] struct {
	ID       string
	Value    V
	Children *TreeNodeChildren[V]
}

// TreeNodeChildren describes the ordered children of a TreeNodeSpec. Order is
// optional; when nil the child keys of Values are used (sorted for determinism —
// see note in BuildSemTree).
type TreeNodeChildren[V any] struct {
	Order  []string
	Values map[string]TreeNodeSpec[V]
}

// semChildKeys boxes the ordered child-key list behind a comparable pointer so it
// can live in a Cell (Cell requires a comparable value type, and Go slices are not
// comparable). Each mutation installs a fresh pointer, so the Cell's != guard
// always fires on a real change — mirroring Dart's list-identity semantics.
type semChildKeys struct {
	order []string
}

// semNode is one tree node: its value cell, its ordered-child-keys cell, the memo
// handles of its children, and its own folded memo slot.
type semNode[V comparable, D comparable] struct {
	id            string
	valueCell     *Source[V]
	childKeysCell *Source[*semChildKeys]
	childSlots    map[string]*Computed[D]
	slot          *Computed[D]
}

// SemTree is a memoized semantic tree.
//
// Build via BuildSemTree. The child-slot map is fixed at build time; inserting a
// brand-new child requires a fresh build (mirrors lazily-rs/lazily-kt). Removals
// mutate the parent's child-keys cell.
type SemTree[V comparable, D comparable] struct {
	ctx    *Context
	fold   FoldFn[V, D]
	nodes  map[string]*semNode[V, D]
	rootID string
}

// BuildSemTree builds a SemTree from rootSpec using fold.
func BuildSemTree[V comparable, D comparable](
	ctx *Context,
	rootSpec TreeNodeSpec[V],
	fold FoldFn[V, D],
) *SemTree[V, D] {
	tree := &SemTree[V, D]{
		ctx:   ctx,
		fold:  fold,
		nodes: map[string]*semNode[V, D]{},
	}
	tree.rootID = rootSpec.ID
	tree.build(rootSpec)
	return tree
}

func (t *SemTree[V, D]) build(spec TreeNodeSpec[V]) *semNode[V, D] {
	node := &semNode[V, D]{
		id:         spec.ID,
		valueCell:  NewSource[V](t.ctx, spec.Value),
		childSlots: map[string]*Computed[D]{},
	}
	t.nodes[spec.ID] = node

	childOrder := []string{}
	if kids := spec.Children; kids != nil {
		order := kids.Order
		if order == nil {
			// Dart falls back to values.keys (insertion order). Go maps have no
			// stable iteration order, so sort for determinism. The conformance
			// fixtures always supply an explicit order.
			order = sortedTreeChildKeys(kids.Values)
		}
		for _, childKey := range order {
			childSpec, ok := kids.Values[childKey]
			if !ok {
				continue
			}
			childNode := t.build(childSpec)
			childOrder = append(childOrder, childSpec.ID)
			node.childSlots[childSpec.ID] = childNode.slot
		}
	}

	node.childKeysCell = NewSource[*semChildKeys](t.ctx, &semChildKeys{order: childOrder})

	// Register the memo AFTER childKeysCell is set, so the memo observes it.
	node.slot = NewComputed[D](t.ctx, func(_ *Context) D {
		v := node.valueCell.Get()
		keys := node.childKeysCell.Get()
		ds := make([]D, 0, len(keys.order))
		for _, kid := range keys.order {
			if childSlot, ok := node.childSlots[kid]; ok {
				ds = append(ds, childSlot.Get())
			}
		}
		return t.fold(v, ds)
	})

	return node
}

// SetValue sets the value of node id. Returns an error if the node is absent.
func (t *SemTree[V, D]) SetValue(id string, value V) error {
	node, ok := t.nodes[id]
	if !ok {
		return fmt.Errorf("SemTree: unknown node %s", id)
	}
	node.valueCell.Set(value)
	return nil
}

// RemoveChild removes childID from parentID's ordered children. Returns an error
// if the parent is absent.
func (t *SemTree[V, D]) RemoveChild(parentID, childID string) error {
	node, ok := t.nodes[parentID]
	if !ok {
		return fmt.Errorf("SemTree: unknown parent node %s", parentID)
	}
	prev := node.childKeysCell.Peek().order
	filtered := make([]string, 0, len(prev))
	for _, k := range prev {
		if k != childID {
			filtered = append(filtered, k)
		}
	}
	node.childKeysCell.Set(&semChildKeys{order: filtered})
	return nil
}

// Value returns the root's derived value (reactive read).
func (t *SemTree[V, D]) Value() D {
	return t.nodes[t.rootID].slot.Get()
}

// NodeValue returns the derived value of node id and true, or the zero value and
// false if the node is absent.
func (t *SemTree[V, D]) NodeValue(id string) (D, bool) {
	node, ok := t.nodes[id]
	if !ok || node.slot == nil {
		var zero D
		return zero, false
	}
	return node.slot.Get(), true
}

// IsCached reports whether node id's derived value is currently cached.
func (t *SemTree[V, D]) IsCached(id string) bool {
	node, ok := t.nodes[id]
	if !ok || node.slot == nil {
		return false
	}
	return node.slot.cachedNow()
}

// RootHandle returns the root slot handle.
func (t *SemTree[V, D]) RootHandle() *Computed[D] {
	return t.nodes[t.rootID].slot
}

// NodeHandle returns the slot handle for node id and true, or nil and false if the
// node is absent.
func (t *SemTree[V, D]) NodeHandle(id string) (*Computed[D], bool) {
	node, ok := t.nodes[id]
	if !ok {
		return nil, false
	}
	return node.slot, true
}

func sortedTreeChildKeys[V any](m map[string]TreeNodeSpec[V]) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
