package lazily

// keyedOrder is the present set plus its authoritative key order, with the
// atomic-move algebra (#lzcellmove).
//
// This is the graph-agnostic half of every ReactiveMap flavor. It holds no
// context, no factory, and no closure: only K -> H handle bookkeeping and the
// key list. That is why ordering and atomic move bind the single-threaded,
// thread-safe, and async flavors alike — a move touches no entry handle and
// awaits nothing, so it is neither thread- nor async-coloured.
//
// What is deliberately NOT here is reactivity. Membership and order
// invalidation is a graph write, and each flavor must mint its own version
// cells on its own graph. A shared core cannot supply that; each flavor keeps a
// thin shell holding this core, its own lock, its own signals, and its own
// mint/observe.
//
// entries and order stay in lockstep: every key in entries appears exactly once
// in order and vice versa, including on every failure path. Reordering here
// cannot fail — it is an in-place rotation — so there is no allocating error
// path to desync on.
//
// Rust reference: lazily-rs/src/keyed_order.rs.
type keyedOrder[K comparable, H any] struct {
	entries map[K]H
	order   []K
}

// mapMutation reports what a present-set mutation did, so the caller knows
// which version cells to bump. A no-op must bump nothing: bumping on a warm
// insert would invalidate every Len/ContainsKey reader on a pure cache hit.
type mapMutation int

const (
	mutationNone mapMutation = iota
	mutationInserted
	mutationRemoved
)

func (m mapMutation) changed() bool { return m != mutationNone }

// mapMove reports what an ordering move did. moveMissing and moveUnchanged are
// distinct because the public Move* methods report false for a missing key but
// true for a no-op move — while neither may bump the order signal.
type mapMove int

const (
	moveMissing mapMove = iota
	moveUnchanged
	moveReordered
)

// applied reports whether the move happened at all (the bool the API returns).
func (m mapMove) applied() bool { return m != moveMissing }

// changed reports whether the order actually changed, i.e. whether to bump.
func (m mapMove) changed() bool { return m == moveReordered }

func newKeyedOrder[K comparable, H any]() keyedOrder[K, H] {
	return keyedOrder[K, H]{entries: map[K]H{}}
}

// get returns key's handle if present.
func (k *keyedOrder[K, H]) get(key K) (H, bool) {
	h, ok := k.entries[key]
	return h, ok
}

// contains reports whether key is in the present set.
func (k *keyedOrder[K, H]) contains(key K) bool {
	_, ok := k.entries[key]
	return ok
}

// keys returns a copy of the authoritative key list. A copy, because the
// internal slice must never escape its owner's lock.
func (k *keyedOrder[K, H]) keys() []K {
	out := make([]K, len(k.order))
	copy(out, k.order)
	return out
}

// length reports the present-set size.
func (k *keyedOrder[K, H]) length() int { return len(k.order) }

// position returns key's current 0-based position.
func (k *keyedOrder[K, H]) position(key K) (int, bool) {
	for i, existing := range k.order {
		if existing == key {
			return i, true
		}
	}
	return 0, false
}

// insert appends handle under key. A warm key keeps its existing handle
// (cell-identity: a key's node is stable for its lifetime) and reports
// mutationNone so the caller bumps nothing.
func (k *keyedOrder[K, H]) insert(key K, handle H) (H, mapMutation) {
	if existing, ok := k.entries[key]; ok {
		return existing, mutationNone
	}
	k.entries[key] = handle
	k.order = append(k.order, key)
	return handle, mutationInserted
}

// remove drops key, returning its handle so the caller can dispose the node on
// its own graph. The core never touches a handle.
func (k *keyedOrder[K, H]) remove(key K) (H, mapMutation) {
	handle, ok := k.entries[key]
	if !ok {
		var zero H
		return zero, mutationNone
	}
	delete(k.entries, key)
	for i, existing := range k.order {
		if existing == key {
			k.order = append(k.order[:i], k.order[i+1:]...)
			break
		}
	}
	return handle, mutationRemoved
}

// moveTo moves key to index, clamped to [0, len). The entry keeps the same
// handle, its dependents, and its CRDT lineage — that is what separates a
// reorder from a remove + re-mint.
func (k *keyedOrder[K, H]) moveTo(key K, index int) mapMove {
	from, ok := k.position(key)
	if !ok {
		return moveMissing
	}
	to := index
	if to < 0 {
		to = 0
	}
	if to > len(k.order)-1 {
		to = len(k.order) - 1
	}
	if from == to {
		return moveUnchanged
	}
	k.rotate(from, to)
	return moveReordered
}

// moveBefore moves key to just before anchor.
//
// The target is computed on the PRE-REMOVAL list: when key currently precedes
// anchor, lifting key out shifts anchor one slot left, so the insertion point is
// anchor-1. Getting this wrong lands the key on the far side of its anchor —
// the defect found in lazily-zig, where moveBefore("a","d") on [a,b,c,d]
// produced [b,c,d,a] instead of [b,c,a,d].
func (k *keyedOrder[K, H]) moveBefore(key, anchor K) mapMove {
	anchorIdx, ok := k.position(anchor)
	if !ok {
		return moveMissing
	}
	from, ok := k.position(key)
	if !ok {
		return moveMissing
	}
	target := anchorIdx
	if from < anchorIdx {
		target = anchorIdx - 1
	}
	return k.moveTo(key, target)
}

// moveAfter moves key to just after anchor. Same pre-removal reasoning.
func (k *keyedOrder[K, H]) moveAfter(key, anchor K) mapMove {
	anchorIdx, ok := k.position(anchor)
	if !ok {
		return moveMissing
	}
	from, ok := k.position(key)
	if !ok {
		return moveMissing
	}
	target := anchorIdx + 1
	if from <= anchorIdx {
		target = anchorIdx
	}
	return k.moveTo(key, target)
}

// rotate moves the element at from to to, in place. There is no intermediate
// state where the key is absent from order, and no allocation, so entries and
// order can never desync here.
func (k *keyedOrder[K, H]) rotate(from, to int) {
	moved := k.order[from]
	if from < to {
		copy(k.order[from:to], k.order[from+1:to+1])
	} else {
		copy(k.order[to+1:from+1], k.order[to:from])
	}
	k.order[to] = moved
}
