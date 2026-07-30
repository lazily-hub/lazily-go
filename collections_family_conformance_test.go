package lazily

import (
	"context"
	"fmt"
	"testing"
)

// The keyed-collection ordering contract replayed against ALL THREE execution
// flavors.
//
// collections_conformance_test.go already replays the ordering fixtures, but
// only against the single-threaded SourceMap. That is the blind spot this file
// closes: ThreadSafeReactiveMap and AsyncReactiveMap shipped PresentKeys /
// PresentCount and nothing else — no ordering surface, no reactive membership,
// and in the thread-safe case no reactive nodes at all. The coverage matrix read
// OK because *a* flavor passed.
//
// Invalidation is measured by RECOMPUTE COUNT rather than a cache flag. A
// counter inside the reader's own compute body is the one probe that cannot be
// satisfied by runner bookkeeping: it only moves if the library actually
// invalidated and re-ran the node.

// mapFlavor is the per-flavor driver the replay runs against.
type mapFlavor interface {
	name() string

	// mutations (the fixture op set)
	setValue(key string, value int)
	insert(key string, value int)
	remove(key string)
	moveTo(key string, index int)
	moveBefore(key, anchor string)
	moveAfter(key, anchor string)

	// untracked observations
	keysUntracked() []string
	valueUntracked(key string) (int, bool)
	// entryID is the entry's node identity: stable across a reorder, different
	// after a re-mint. This is what separates a move from a remove + insert.
	// It is compared with ==, so a handle pointer and a birth id both work.
	entryID(key string) (any, bool)

	// readers: build one, drive it, and report how many times it recomputed.
	newValueReader(key string) reader
	newMembershipReader() reader
	newOrderReader() reader
}

type reader interface {
	// drive settles the reader and returns its recompute count so far.
	drive() int
}

// --- single-threaded --------------------------------------------------------

type syncFlavor struct {
	ctx *Context
	m   *SourceMap[string, int]
}

func newSyncFlavor() *syncFlavor {
	ctx := NewContext()
	return &syncFlavor{ctx: ctx, m: NewSourceMap[string, int](ctx)}
}

func (f *syncFlavor) name() string                   { return "sync" }
func (f *syncFlavor) setValue(key string, value int) { f.m.Set(key, value) }
func (f *syncFlavor) insert(key string, value int)   { f.m.Entry(key, value) }
func (f *syncFlavor) remove(key string)              { f.m.Remove(key) }
func (f *syncFlavor) moveTo(key string, index int)   { f.m.MoveTo(key, index) }
func (f *syncFlavor) moveBefore(key, anchor string)  { f.m.MoveBefore(key, anchor) }
func (f *syncFlavor) moveAfter(key, anchor string)   { f.m.MoveAfter(key, anchor) }
func (f *syncFlavor) keysUntracked() []string        { return f.m.PresentKeys() }
func (f *syncFlavor) valueUntracked(k string) (int, bool) {
	return f.m.Get(k)
}

func (f *syncFlavor) entryID(k string) (any, bool) {
	h, ok := f.m.Handle(k)
	if !ok {
		return nil, false
	}
	return h, true // the node pointer IS the identity.
}

type syncReader struct {
	ctx   *Context
	slot  *Computed[int]
	count *int
}

func (r syncReader) drive() int {
	Get[int](r.ctx, r.slot)
	return *r.count
}

func (f *syncFlavor) reader(body func(c *Compute) int) reader {
	count := 0
	slot := NewSlot(f.ctx, func(c *Compute) int {
		count++
		return body(c)
	})
	return syncReader{ctx: f.ctx, slot: slot, count: &count}
}

func (f *syncFlavor) newValueReader(key string) reader {
	return f.reader(func(c *Compute) int {
		v, _ := f.m.Observe(c, key)
		return v
	})
}
func (f *syncFlavor) newMembershipReader() reader {
	return f.reader(func(c *Compute) int { return f.m.Len(c) })
}
func (f *syncFlavor) newOrderReader() reader {
	return f.reader(func(c *Compute) int { return orderDigest(f.m.Keys(c)) })
}

// --- thread-safe ------------------------------------------------------------

type threadSafeFlavor struct {
	ts *ThreadSafeContext
	m  *ThreadSafeSourceMap[string, int]
}

func newThreadSafeFlavor() *threadSafeFlavor {
	ts := NewThreadSafeContext()
	return &threadSafeFlavor{ts: ts, m: NewThreadSafeSourceMap[string, int](ts)}
}

func (f *threadSafeFlavor) name() string                   { return "thread-safe" }
func (f *threadSafeFlavor) setValue(key string, value int) { f.m.Set(key, value) }
func (f *threadSafeFlavor) insert(key string, value int)   { f.m.Set(key, value) }
func (f *threadSafeFlavor) remove(key string)              { f.m.Remove(key) }
func (f *threadSafeFlavor) moveTo(key string, index int)   { f.m.MoveTo(key, index) }
func (f *threadSafeFlavor) moveBefore(key, anchor string)  { f.m.MoveBefore(key, anchor) }
func (f *threadSafeFlavor) moveAfter(key, anchor string)   { f.m.MoveAfter(key, anchor) }
func (f *threadSafeFlavor) keysUntracked() []string        { return f.m.PresentKeys() }

func (f *threadSafeFlavor) valueUntracked(k string) (int, bool) {
	h, ok := f.m.Handle(k)
	if !ok {
		return 0, false
	}
	return h.Peek(), true
}

func (f *threadSafeFlavor) entryID(k string) (any, bool) {
	h, ok := f.m.Handle(k)
	if !ok {
		return nil, false
	}
	return h, true
}

type tsReader struct {
	ts    *ThreadSafeContext
	slot  *Computed[int]
	count *int
}

func (r tsReader) drive() int {
	r.ts.WithLock(func(ctx *Context) { Get[int](ctx, r.slot) })
	return *r.count
}

func (f *threadSafeFlavor) reader(body func(c *Compute) int) reader {
	count := 0
	var slot *Computed[int]
	f.ts.WithLock(func(ctx *Context) {
		slot = NewSlot(ctx, func(c *Compute) int {
			count++
			return body(c)
		})
	})
	return tsReader{ts: f.ts, slot: slot, count: &count}
}

func (f *threadSafeFlavor) newValueReader(key string) reader {
	return f.reader(func(c *Compute) int {
		v, _ := f.m.Observe(c, key)
		return v
	})
}
func (f *threadSafeFlavor) newMembershipReader() reader {
	return f.reader(func(c *Compute) int { return f.m.Len(c) })
}
func (f *threadSafeFlavor) newOrderReader() reader {
	return f.reader(func(c *Compute) int { return orderDigest(f.m.Keys(c)) })
}

// --- async ------------------------------------------------------------------

type asyncFlavor struct {
	c *AsyncContext
	m *AsyncSourceMap[string, int]
}

func newAsyncFlavor() *asyncFlavor {
	c := NewAsyncContext()
	return &asyncFlavor{c: c, m: NewAsyncSourceMap[string, int](c)}
}

func (f *asyncFlavor) name() string                   { return "async" }
func (f *asyncFlavor) setValue(key string, value int) { f.m.Set(key, value) }
func (f *asyncFlavor) insert(key string, value int)   { f.m.Set(key, value) }
func (f *asyncFlavor) remove(key string)              { f.m.Remove(key) }
func (f *asyncFlavor) moveTo(key string, index int)   { f.m.MoveTo(key, index) }
func (f *asyncFlavor) moveBefore(key, anchor string)  { f.m.MoveBefore(key, anchor) }
func (f *asyncFlavor) moveAfter(key, anchor string)   { f.m.MoveAfter(key, anchor) }
func (f *asyncFlavor) keysUntracked() []string        { return f.m.PresentKeys() }

func (f *asyncFlavor) valueUntracked(k string) (int, bool) {
	return f.m.ObserveTracked(nil, k)
}

func (f *asyncFlavor) entryID(k string) (any, bool) {
	id, ok := f.m.EntryID(k)
	if !ok {
		return nil, false
	}
	return id, true
}

type asyncReader struct {
	slot  *AsyncComputed[int]
	count *int
}

func (r asyncReader) drive() int {
	_, _ = r.slot.GetAsync(context.Background())
	return *r.count
}

func (f *asyncFlavor) reader(body func(cc *AsyncComputeContext) int) reader {
	count := 0
	slot := NewAsyncComputed(f.c, func(cc *AsyncComputeContext) (int, error) {
		count++
		return body(cc), nil
	})
	return asyncReader{slot: slot, count: &count}
}

func (f *asyncFlavor) newValueReader(key string) reader {
	return f.reader(func(cc *AsyncComputeContext) int {
		v, _ := f.m.ObserveTracked(cc, key)
		return v
	})
}
func (f *asyncFlavor) newMembershipReader() reader {
	return f.reader(func(cc *AsyncComputeContext) int { return f.m.Len(cc) })
}
func (f *asyncFlavor) newOrderReader() reader {
	return f.reader(func(cc *AsyncComputeContext) int { return orderDigest(f.m.Keys(cc)) })
}

// --- replay -----------------------------------------------------------------

// orderDigest is order-sensitive, so an order reader's VALUE changes on a
// reorder and not merely its cache state.
func orderDigest(keys []string) int {
	acc := 17
	for _, k := range keys {
		for _, ch := range k {
			acc = acc*31 + int(ch)
		}
		acc = acc*31 + 7
	}
	return acc
}

func stringSlice(v any) []string {
	raw, _ := v.([]any)
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		s, _ := item.(string)
		out = append(out, s)
	}
	return out
}

func sameOrder(a, b []string) bool {
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

// mapOf narrows a decoded JSON value to an object, or nil.
func mapOf(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func replayOrderingFixture(t *testing.T, flavor mapFlavor, name string) {
	t.Helper()
	fixture, ok := loadCollectionFixture(t, name)
	if !ok {
		return
	}
	where := func(step int) string {
		return fmt.Sprintf("%s %s step %d", flavor.name(), name, step)
	}

	consumeFixtureKeys(t, name, fixture, "initial", "steps")
	initial, _ := fixture["initial"].(map[string]any)
	if initial == nil {
		t.Fatalf("%s: fixture has no initial state", flavor.name())
	}
	seed := stringSlice(initial["order"])
	if len(seed) == 0 {
		t.Fatalf("%s: fixture %s seeds no keys", flavor.name(), name)
	}
	values, _ := initial["values"].(map[string]any)
	for _, key := range seed {
		v, present := values[key]
		if !present {
			t.Fatalf("%s: no initial value for key %s", flavor.name(), key)
		}
		num, _ := v.(float64)
		flavor.insert(key, int(num))
	}

	steps, _ := fixture["steps"].([]any)
	// A zero-step replay asserts nothing and still reports green.
	if len(steps) == 0 {
		t.Fatalf("%s: fixture %s has no steps - a vacuous replay would report green", flavor.name(), name)
	}

	matrices := 0

	for i, rawStep := range steps {
		step, _ := rawStep.(map[string]any)
		op, _ := step["op"].(map[string]any)
		expected := consumeKeys(t, where(i)+" expected", mapOf(step["expected"]),
			"invalidates", "handle_stable", "order", "membership", "values")
		consumeKeys(t, where(i)+" expected.invalidates", mapOf(expected["invalidates"]),
			"value", "membership", "order")
		if op == nil || expected == nil {
			t.Fatalf("%s: missing op or expected", where(i))
		}

		// Rebuild + settle readers from the CURRENT key set, so each step's
		// invalidation is measured against a fully settled graph.
		beforeKeys := flavor.keysUntracked()
		valueReaders := map[string]reader{}
		baseline := map[string]int{}
		for _, key := range beforeKeys {
			r := flavor.newValueReader(key)
			valueReaders[key] = r
			baseline[key] = r.drive()
		}
		membership := flavor.newMembershipReader()
		order := flavor.newOrderReader()
		membershipBase := membership.drive()
		orderBase := order.drive()

		idsBefore := map[string]any{}
		for _, key := range beforeKeys {
			if id, ok := flavor.entryID(key); ok {
				idsBefore[key] = id
			}
		}

		switch op["type"] {
		case "set_value":
			key, _ := op["key"].(string)
			num, _ := op["value"].(float64)
			flavor.setValue(key, int(num))
		case "insert":
			key, _ := op["key"].(string)
			num, _ := op["value"].(float64)
			flavor.insert(key, int(num))
			// `at` says where the new key lands; minting appends, so "end" is
			// already right. An unrecognised form must fail, not silently append.
			switch at := op["at"].(type) {
			case string:
				if at != "end" {
					t.Fatalf("%s: unsupported insert placement %q", where(i), at)
				}
			case float64:
				flavor.moveTo(key, int(at))
			}
		case "remove":
			key, _ := op["key"].(string)
			flavor.remove(key)
		case "move_to":
			key, _ := op["key"].(string)
			idx, _ := op["index"].(float64)
			flavor.moveTo(key, int(idx))
		case "move_before":
			key, _ := op["key"].(string)
			anchor, _ := op["before"].(string)
			flavor.moveBefore(key, anchor)
		case "move_after":
			key, _ := op["key"].(string)
			anchor, _ := op["after"].(string)
			flavor.moveAfter(key, anchor)
		default:
			t.Fatalf("%s: unsupported op %v - an unknown op must fail, never silently skip", where(i), op["type"])
		}

		wantOrder := stringSlice(expected["order"])
		gotOrder := flavor.keysUntracked()
		if !sameOrder(wantOrder, gotOrder) {
			t.Fatalf("%s: order = %v, want %v", where(i), gotOrder, wantOrder)
		}

		if wantValues, ok := expected["values"].(map[string]any); ok {
			for key, raw := range wantValues {
				num, _ := raw.(float64)
				got, present := flavor.valueUntracked(key)
				if !present {
					t.Fatalf("%s: value for %s is absent", where(i), key)
				}
				if got != int(num) {
					t.Fatalf("%s: value for %s = %d, want %d", where(i), key, got, int(num))
				}
			}
		}

		// The invalidation matrix, read from expected.invalidates — where the
		// fixtures actually nest it. lazily-rs read it off the step instead, so
		// its assertion never ran once.
		invalidates, ok := expected["invalidates"].(map[string]any)
		if !ok {
			t.Fatalf("%s: expected.invalidates is missing - the matrix is the contract", where(i))
		}
		matrices++

		dirty := map[string]bool{}
		for _, key := range stringSlice(invalidates["value"]) {
			dirty[key] = true
		}
		survivors := map[string]bool{}
		for _, key := range gotOrder {
			survivors[key] = true
		}
		for key, r := range valueReaders {
			if !survivors[key] {
				continue // removed by this op: no entry left to read
			}
			recomputed := r.drive() != baseline[key]
			if dirty[key] && !recomputed {
				t.Fatalf("%s: value reader for %s should have been invalidated", where(i), key)
			}
			if !dirty[key] && recomputed {
				t.Fatalf("%s: value reader for %s should have stayed cached - per-entry independence is the whole point", where(i), key)
			}
		}

		wantMembershipDirty, _ := invalidates["membership"].(bool)
		if got := membership.drive() != membershipBase; got != wantMembershipDirty {
			t.Fatalf("%s: membership reader invalidated=%v, want %v - a pure reorder must NOT invalidate set-identity readers",
				where(i), got, wantMembershipDirty)
		}

		wantOrderDirty, _ := invalidates["order"].(bool)
		if got := order.drive() != orderBase; got != wantOrderDirty {
			t.Fatalf("%s: order reader invalidated=%v, want %v", where(i), got, wantOrderDirty)
		}

		// Handle stability: the law that separates an atomic move from a remove +
		// re-mint. A reorder keeps the entry's node, so dependents and lineage
		// survive.
		if stable, ok := expected["handle_stable"].(map[string]any); ok {
			for key, raw := range stable {
				want, _ := raw.(bool)
				after, present := flavor.entryID(key)
				before, had := idsBefore[key]
				if want {
					if !had || !present || before != after {
						t.Fatalf("%s: entry identity for %s must survive the move - a reorder that re-mints is a remove + insert",
							where(i), key)
					}
				} else if had && present && before == after {
					t.Fatalf("%s: entry identity for %s should have changed", where(i), key)
				}
			}
		}
	}

	if matrices == 0 {
		t.Fatalf("%s: fixture %s asserted no invalidation matrix", flavor.name(), name)
	}
	t.Logf("ok %s %s (%d steps, %d invalidation matrices)", flavor.name(), name, len(steps), matrices)
}

func eachFlavor(t *testing.T, run func(t *testing.T, f mapFlavor)) {
	t.Helper()
	builders := []func() mapFlavor{
		func() mapFlavor { return newSyncFlavor() },
		func() mapFlavor { return newThreadSafeFlavor() },
		func() mapFlavor { return newAsyncFlavor() },
	}
	for _, build := range builders {
		f := build()
		t.Run(f.name(), func(t *testing.T) { run(t, f) })
	}
}

func TestCollectionsAtomicMoveAllFlavors(t *testing.T) {
	eachFlavor(t, func(t *testing.T, f mapFlavor) {
		replayOrderingFixture(t, f, "cellmap_atomic_move.json")
	})
}

func TestCollectionsIndependenceAllFlavors(t *testing.T) {
	eachFlavor(t, func(t *testing.T, f mapFlavor) {
		replayOrderingFixture(t, f, "cellmap_independence.json")
	})
}

// TestCollectionsDirectionalMovesAllFlavors covers a direction the canonical
// corpus does not.
//
// cellmap_atomic_move.json's only move_before step moves a key that already
// FOLLOWS its anchor (from=2, anchor=0), so it exercises only the branch where
// the insertion point is the anchor index itself. The branch where the key
// PRECEDES its anchor — and the target must therefore be anchor-1 — is never
// replayed. That is exactly the direction lazily-zig's moveBefore was wrong in:
// moveBefore("a","d") on [a,b,c,d] produced [b,c,d,a]. The canonical corpus
// would have scored that binding green.
func TestCollectionsDirectionalMovesAllFlavors(t *testing.T) {
	seed := []string{"a", "b", "c", "d"}
	build := func(f mapFlavor) {
		for i, k := range seed {
			f.insert(k, i+1)
		}
	}

	cases := []struct {
		what string
		run  func(f mapFlavor)
		want []string
	}{
		{"move_before key precedes anchor", func(f mapFlavor) { f.moveBefore("a", "d") }, []string{"b", "c", "a", "d"}},
		{"move_before key follows anchor", func(f mapFlavor) { f.moveBefore("d", "b") }, []string{"a", "d", "b", "c"}},
		{"move_after key precedes anchor", func(f mapFlavor) { f.moveAfter("a", "c") }, []string{"b", "c", "a", "d"}},
		{"move_after key follows anchor", func(f mapFlavor) { f.moveAfter("d", "a") }, []string{"a", "d", "b", "c"}},
		{"move_to past the end clamps", func(f mapFlavor) { f.moveTo("a", 99) }, []string{"b", "c", "d", "a"}},
		{"move_to below zero clamps", func(f mapFlavor) { f.moveTo("d", -5) }, []string{"d", "a", "b", "c"}},
		{"move on an absent key is a no-op", func(f mapFlavor) {
			f.moveBefore("zz", "a")
			f.moveTo("zz", 0)
		}, seed},
	}

	eachFlavor(t, func(t *testing.T, _ mapFlavor) {})
	for _, newFlavor := range []func() mapFlavor{
		func() mapFlavor { return newSyncFlavor() },
		func() mapFlavor { return newThreadSafeFlavor() },
		func() mapFlavor { return newAsyncFlavor() },
	} {
		flavorName := newFlavor().name()
		t.Run(flavorName, func(t *testing.T) {
			for _, tc := range cases {
				f := newFlavor()
				build(f)
				idBefore, _ := f.entryID("a")
				tc.run(f)
				if got := f.keysUntracked(); !sameOrder(got, tc.want) {
					t.Fatalf("%s: %s gave %v, want %v - the target must be computed on the pre-removal list",
						flavorName, tc.what, got, tc.want)
				}
				if idAfter, ok := f.entryID("a"); ok && idAfter != idBefore {
					t.Fatalf("%s: %s re-minted entry a - a reorder must keep the entry's node", flavorName, tc.what)
				}
			}
		})
	}
}
