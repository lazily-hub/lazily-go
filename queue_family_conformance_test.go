package lazily

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var queueFixtures = []string{
	"queuecell_spsc_push_pop.json",
	"queuecell_popped_head_observation.json",
	"queuecell_mpsc_multi_writer.json",
	"queuecell_bounded_backpressure.json",
	"queuecell_closure_lifecycle.json",
}

var topicFixtures = []string{
	"topiccell_broadcast_cursor_isolation.json",
	"topiccell_durable_replay_gc.json",
	"topiccell_ephemeral_lifecycle.json",
	"topiccell_offline_tail_bounds.json",
}

var workQueueFixtures = []string{
	"workqueue_competing_delivery.json",
	"workqueue_lease_deadletter.json",
}

// queueFamilyCapability is an enforced binding/flavor ledger. A source marker
// must exist exactly when the capability is shipped, and every shipped entry is
// exercised below. Future staged flavors stay explicit rather than disappearing
// behind a green skip.
type queueFamilyCapability struct {
	flavor, primitive, marker string
	shipped                   bool
}

var queueFamilyCapabilities = []queueFamilyCapability{
	{"sync", "QueueCell", "type QueueCell[", true},
	{"sync", "TopicCell", "type TopicCell[", true},
	{"sync", "WorkQueueCell", "type WorkQueueCell[", true},
	{"thread-safe", "QueueCell", "type ThreadSafeQueueCell[", true},
	{"thread-safe", "TopicCell", "type ThreadSafeTopicCell[", true},
	{"thread-safe", "WorkQueueCell", "type ThreadSafeWorkQueueCell[", true},
	{"async", "QueueCell", "type AsyncQueueCell[", true},
	{"async", "TopicCell", "type AsyncTopicCell[", true},
	{"async", "WorkQueueCell", "type AsyncWorkQueueCell[", true},
}

func packageSources(t *testing.T) string {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	var b strings.Builder
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") ||
			strings.HasSuffix(name, "_test.go") {
			continue
		}
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		b.Write(data)
	}
	return b.String()
}

func TestQueueFamilyCapabilityLedgerMatchesSource(t *testing.T) {
	sources := packageSources(t)
	if sources == "" {
		t.Fatal("read no package sources; capability ledger would be vacuous")
	}
	seen := map[string]bool{}
	shipped := 0
	for _, capability := range queueFamilyCapabilities {
		key := capability.flavor + "/" + capability.primitive
		if seen[key] {
			t.Fatalf("duplicate capability ledger entry %q", key)
		}
		seen[key] = true
		defined := strings.Contains(sources, capability.marker)
		if defined != capability.shipped {
			t.Fatalf("%s: source marker %q present=%v, ledger shipped=%v",
				key, capability.marker, defined, capability.shipped)
		}
		if capability.shipped {
			shipped++
		}
	}
	if len(seen) != 9 {
		t.Fatalf("capability ledger has %d entries, want 3 primitives x 3 flavors", len(seen))
	}
	if shipped == 0 {
		t.Fatal("all queue-family capabilities are staged; suite would test nothing")
	}
}

func loadQueueFamilyFixture(t *testing.T, name string) map[string]any {
	t.Helper()
	fixture, ok := loadCollectionFixture(t, name)
	if !ok {
		t.Fatalf("canonical fixture %s was not found", name)
	}
	consumeFixtureKeys(t, name, fixture, "config", "initial", "invariants", "steps")
	excuseKeys(t, fixture, "replay input: the config and seed state the replay is set up FROM; what they produce is asserted through each step's expected block",
		"config", "initial")
	excuseKey(t, fixture, "steps", "replay input: the step list drives the loop, and each step's own `expected` block is asserted there")
	if _, stated := fixture["invariants"]; stated {
		assertKeyEach(t, fixture, "invariants", func(key string, value any) {
			assertQueueInvariantDocumented(t, name, key, value)
		})
	}
	for i, raw := range jsList(fixture["steps"]) {
		step := jsMap(raw)
		if _, bad := step["invalidates"]; bad {
			t.Fatalf("%s step %d: invalidates is at step level; runner reads expected.invalidates",
				name, i)
		}
		expected := jsMap(step["expected"])
		if _, ok := expected["invalidates"]; !ok {
			t.Fatalf("%s step %d: expected.invalidates is missing", name, i)
		}
	}
	return fixture
}

// assertQueueInvariantDocumented consumes ONE entry of the fixture's
// `invariants` block.
//
// Unlike every other assertion key in this corpus, its values are prose — one
// sentence naming the behaviour the *steps* are constructed to exercise ("pop on
// closed+empty returns Closed, not Empty"). There is no machine-checkable claim
// here for the runner to evaluate beyond the step assertions that already
// evaluate it, so this is a declared exception rather than an unimplemented
// assertion: the block is consumed, and it is held to being real prose so an
// empty or non-string entry cannot pass itself off as documentation.
// The walk over the block lives in assertKeyEach rather than here
// (#lzsubblockkeyset), so an invariant added upstream reaches this check.
func assertQueueInvariantDocumented(t *testing.T, name, key string, value any) {
	t.Helper()
	text, ok := value.(string)
	if !ok || strings.TrimSpace(text) == "" {
		t.Fatalf("%s: invariant %q must be prose describing what the steps exercise, got %v",
			name, key, value)
	}
}

// queueFamilyConfig is the fixture's `config` block. Both fields used to be
// hard-coded in the runner — the visibility timeout as a literal 10 and
// max_deliveries as a per-filename switch — so a fixture that changed either
// would have replayed under the old values and still passed.
type queueFamilyConfig struct {
	VisibilityTimeout int64
	MaxDeliveries     uint64
}

func queueFamilyConfigOf(t *testing.T, name string, fixture map[string]any) queueFamilyConfig {
	t.Helper()
	initial := consumeKeys(t, name+" initial", jsMap(fixture["initial"]),
		"visibility_timeout", "max_deliveries", "pending", "in_flight", "dead_letters")
	if initial == nil {
		t.Fatalf("%s: initial state and lease config are required for the work-queue corpus", name)
	}
	excuseKeys(t, initial, "replay input: the two knobs the work queue is CONSTRUCTED with; what they produce is asserted through the lease/redelivery expectations each step states",
		"visibility_timeout", "max_deliveries")
	for _, key := range []string{"pending", "in_flight", "dead_letters"} {
		initialKey := key
		assertKeyWith(t, initial, initialKey, func(raw any) {
			if len(jsList(raw)) != 0 {
				t.Fatalf("%s: non-empty initial.%s is not supported by this runner", name, initialKey)
			}
		})
	}
	return queueFamilyConfig{
		VisibilityTimeout: int64(jsInt(initial["visibility_timeout"])),
		MaxDeliveries:     uint64(jsInt(initial["max_deliveries"])),
	}
}

type qfReader interface {
	drive() int
}

type qfSyncReader struct {
	slot  *Computed[int]
	count *int
}

func (r qfSyncReader) drive() int {
	r.slot.Get()
	return *r.count
}

type qfTSReader struct {
	ts    *ThreadSafeContext
	slot  *Computed[int]
	count *int
}

func (r qfTSReader) drive() int {
	r.ts.WithLock(func(ctx *Context) { Get(ctx, r.slot) })
	return *r.count
}

type qfAsyncReader struct {
	slot  *AsyncComputed[int]
	count *atomic.Int64
}

func (r qfAsyncReader) drive() int {
	if _, err := r.slot.GetAsync(context.Background()); err != nil {
		panic(err)
	}
	return int(r.count.Load())
}

func newQFSyncReader(ctx *Context, body func(*Compute) int) qfReader {
	count := 0
	slot := NewSlot(ctx, func(c *Compute) int {
		count++
		return body(c)
	})
	return qfSyncReader{slot: slot, count: &count}
}

func newQFTSReader(ts *ThreadSafeContext, body func(*Compute) int) qfReader {
	count := 0
	var slot *Computed[int]
	ts.WithLock(func(ctx *Context) {
		slot = NewSlot(ctx, func(c *Compute) int {
			count++
			return body(c)
		})
	})
	return qfTSReader{ts: ts, slot: slot, count: &count}
}

func newQFAsyncReader(ctx *AsyncContext, body func(*AsyncComputeContext) int) qfReader {
	count := &atomic.Int64{}
	slot := NewAsyncComputed(ctx, func(cc *AsyncComputeContext) (int, error) {
		count.Add(1)
		return body(cc), nil
	})
	return qfAsyncReader{slot: slot, count: count}
}

func assertInvalidationDelta(
	t *testing.T,
	label string,
	reader qfReader,
	before int,
	want bool,
) {
	t.Helper()
	after := reader.drive()
	delta := after - before
	wantDelta := 0
	if want {
		wantDelta = 1
	}
	if delta != wantDelta {
		t.Errorf("%s: recompute delta=%d, want %d (invalidates=%v)",
			label, delta, wantDelta, want)
	}
}

// --- QueueCell --------------------------------------------------------------

type queueFixtureModel interface {
	name() string
	tryPush(string) QueuePushError
	tryPop() (string, QueuePopError)
	close()
	batchPush([]string)
	elements() []string
	head() (string, bool)
	length() int
	isEmpty() bool
	isFull() bool
	isClosed() bool
	readers() map[string]qfReader
	dispose()
}

type syncQueueFixtureModel struct {
	ctx *Context
	q   *QueueCell[string, *VecDequeStorage[string]]
}

func newSyncQueueFixtureModel(capacity *int) *syncQueueFixtureModel {
	ctx := NewContext()
	var q *QueueCell[string, *VecDequeStorage[string]]
	if capacity == nil {
		q = NewQueueCell[string](ctx)
	} else {
		q = NewBoundedQueueCell[string](ctx, *capacity)
	}
	return &syncQueueFixtureModel{ctx: ctx, q: q}
}

func (m *syncQueueFixtureModel) name() string { return "sync" }
func (m *syncQueueFixtureModel) tryPush(v string) QueuePushError {
	return m.q.TryPush(v)
}
func (m *syncQueueFixtureModel) tryPop() (string, QueuePopError) { return m.q.TryPop() }
func (m *syncQueueFixtureModel) close()                          { m.q.Close() }
func (m *syncQueueFixtureModel) batchPush(values []string) {
	m.ctx.Batch(func() {
		for _, value := range values {
			if result := m.q.TryPush(value); !result.Ok() {
				panic(result)
			}
		}
	})
}
func (m *syncQueueFixtureModel) elements() []string { return m.q.Storage().Elements() }
func (m *syncQueueFixtureModel) head() (string, bool) {
	return m.q.Head()
}
func (m *syncQueueFixtureModel) length() int    { return m.q.Len() }
func (m *syncQueueFixtureModel) isEmpty() bool  { return m.q.IsEmpty() }
func (m *syncQueueFixtureModel) isFull() bool   { return m.q.IsFull() }
func (m *syncQueueFixtureModel) isClosed() bool { return m.q.IsClosed() }
func (m *syncQueueFixtureModel) readers() map[string]qfReader {
	h := m.q.ReaderHandles()
	return map[string]qfReader{
		"head": newQFSyncReader(m.ctx, func(c *Compute) int {
			head := Get(c, h.Head)
			if head.ok {
				return 1
			}
			return 0
		}),
		"len": newQFSyncReader(m.ctx, func(c *Compute) int { return Get(c, h.Len) }),
		"is_empty": newQFSyncReader(m.ctx, func(c *Compute) int {
			if Get(c, h.IsEmpty) {
				return 1
			}
			return 0
		}),
		"is_full": newQFSyncReader(m.ctx, func(c *Compute) int {
			if Get(c, h.IsFull) {
				return 1
			}
			return 0
		}),
		"closed": newQFSyncReader(m.ctx, func(c *Compute) int {
			if Get(c, h.IsClosed) {
				return 1
			}
			return 0
		}),
	}
}
func (m *syncQueueFixtureModel) dispose() {}

type tsQueueFixtureModel struct {
	ts *ThreadSafeContext
	q  *ThreadSafeQueueCell[string, *VecDequeStorage[string]]
}

func newTSQueueFixtureModel(capacity *int) *tsQueueFixtureModel {
	ts := NewThreadSafeContext()
	var q *ThreadSafeQueueCell[string, *VecDequeStorage[string]]
	if capacity == nil {
		q = NewThreadSafeQueueCell[string](ts)
	} else {
		q = NewBoundedThreadSafeQueueCell[string](ts, *capacity)
	}
	return &tsQueueFixtureModel{ts: ts, q: q}
}

func (m *tsQueueFixtureModel) name() string { return "thread-safe" }
func (m *tsQueueFixtureModel) tryPush(v string) QueuePushError {
	return m.q.TryPush(v)
}
func (m *tsQueueFixtureModel) tryPop() (string, QueuePopError) { return m.q.TryPop() }
func (m *tsQueueFixtureModel) close()                          { m.q.Close() }
func (m *tsQueueFixtureModel) batchPush(values []string) {
	m.ts.Batch(func() {
		for _, value := range values {
			if result := m.q.TryPush(value); !result.Ok() {
				panic(result)
			}
		}
	})
}
func (m *tsQueueFixtureModel) elements() []string { return m.q.Elements() }
func (m *tsQueueFixtureModel) head() (string, bool) {
	return m.q.Head()
}
func (m *tsQueueFixtureModel) length() int    { return m.q.Len() }
func (m *tsQueueFixtureModel) isEmpty() bool  { return m.q.IsEmpty() }
func (m *tsQueueFixtureModel) isFull() bool   { return m.q.IsFull() }
func (m *tsQueueFixtureModel) isClosed() bool { return m.q.IsClosed() }
func (m *tsQueueFixtureModel) readers() map[string]qfReader {
	h := m.q.ReaderHandles()
	return map[string]qfReader{
		"head": newQFTSReader(m.ts, func(c *Compute) int {
			head := Get(c, h.Head)
			if head.ok {
				return 1
			}
			return 0
		}),
		"len": newQFTSReader(m.ts, func(c *Compute) int { return Get(c, h.Len) }),
		"is_empty": newQFTSReader(m.ts, func(c *Compute) int {
			if Get(c, h.IsEmpty) {
				return 1
			}
			return 0
		}),
		"is_full": newQFTSReader(m.ts, func(c *Compute) int {
			if Get(c, h.IsFull) {
				return 1
			}
			return 0
		}),
		"closed": newQFTSReader(m.ts, func(c *Compute) int {
			if Get(c, h.IsClosed) {
				return 1
			}
			return 0
		}),
	}
}
func (m *tsQueueFixtureModel) dispose() {}

type asyncQueueFixtureModel struct {
	ctx *AsyncContext
	q   *AsyncQueueCell[string, *VecDequeStorage[string]]
}

func newAsyncQueueFixtureModel(capacity *int) *asyncQueueFixtureModel {
	ctx := NewAsyncContext()
	var q *AsyncQueueCell[string, *VecDequeStorage[string]]
	if capacity == nil {
		q = NewAsyncQueueCell[string](ctx)
	} else {
		q = NewBoundedAsyncQueueCell[string](ctx, *capacity)
	}
	return &asyncQueueFixtureModel{ctx: ctx, q: q}
}

func (m *asyncQueueFixtureModel) name() string { return "async" }
func (m *asyncQueueFixtureModel) tryPush(v string) QueuePushError {
	return m.q.TryPush(v)
}
func (m *asyncQueueFixtureModel) tryPop() (string, QueuePopError) { return m.q.TryPop() }
func (m *asyncQueueFixtureModel) close()                          { m.q.Close() }
func (m *asyncQueueFixtureModel) batchPush(values []string) {
	m.ctx.Batch(func() {
		for _, value := range values {
			if result := m.q.TryPush(value); !result.Ok() {
				panic(result)
			}
		}
	})
}
func (m *asyncQueueFixtureModel) elements() []string { return m.q.Elements() }
func (m *asyncQueueFixtureModel) head() (string, bool) {
	return m.q.Head(nil)
}
func (m *asyncQueueFixtureModel) length() int    { return m.q.Len(nil) }
func (m *asyncQueueFixtureModel) isEmpty() bool  { return m.q.IsEmpty(nil) }
func (m *asyncQueueFixtureModel) isFull() bool   { return m.q.IsFull(nil) }
func (m *asyncQueueFixtureModel) isClosed() bool { return m.q.IsClosed(nil) }
func (m *asyncQueueFixtureModel) readers() map[string]qfReader {
	return map[string]qfReader{
		"head": newQFAsyncReader(m.ctx, func(cc *AsyncComputeContext) int {
			_, ok := m.q.Head(cc)
			if ok {
				return 1
			}
			return 0
		}),
		"len": newQFAsyncReader(m.ctx, func(cc *AsyncComputeContext) int {
			return m.q.Len(cc)
		}),
		"is_empty": newQFAsyncReader(m.ctx, func(cc *AsyncComputeContext) int {
			if m.q.IsEmpty(cc) {
				return 1
			}
			return 0
		}),
		"is_full": newQFAsyncReader(m.ctx, func(cc *AsyncComputeContext) int {
			if m.q.IsFull(cc) {
				return 1
			}
			return 0
		}),
		"closed": newQFAsyncReader(m.ctx, func(cc *AsyncComputeContext) int {
			if m.q.IsClosed(cc) {
				return 1
			}
			return 0
		}),
	}
}
func (m *asyncQueueFixtureModel) dispose() { m.ctx.DisposeAsync() }

func queueCapacity(initial map[string]any) *int {
	value, bounded := initial["capacity"]
	if !bounded || value == nil {
		return nil
	}
	capacity := jsInt(value)
	return &capacity
}

func assertQueueFixtureState(
	t *testing.T,
	label string,
	model queueFixtureModel,
	expected map[string]any,
) {
	t.Helper()
	if _, stated := expected["elements"]; stated {
		assertKeyWith(t, expected, "elements", func(want any) {
			if got := model.elements(); !reflect.DeepEqual(got, jsStrList(want)) {
				t.Errorf("%s elements=%v, want %v", label, got, jsStrList(want))
			}
		})
	}
	if _, stated := expected["head"]; stated {
		assertKeyWith(t, expected, "head", func(want any) {
			got, exists := model.head()
			if want == nil {
				if exists {
					t.Errorf("%s head=%q, want none", label, got)
				}
			} else if !exists || got != jsStr(want) {
				t.Errorf("%s head=(%q,%v), want %q", label, got, exists, jsStr(want))
			}
		})
	}
	if _, stated := expected["len"]; stated {
		assertKey(t, expected, "len", model.length())
	}
	if _, stated := expected["is_empty"]; stated {
		assertKey(t, expected, "is_empty", model.isEmpty())
	}
	if _, stated := expected["is_full"]; stated {
		assertKey(t, expected, "is_full", model.isFull())
	}
	if _, stated := expected["closed"]; stated {
		assertKey(t, expected, "closed", model.isClosed())
	}
}

func replayQueueFixture(t *testing.T, name string, model queueFixtureModel) int {
	t.Helper()
	defer model.dispose()
	fixture := loadQueueFamilyFixture(t, name)
	initial := jsMap(fixture["initial"])
	for _, value := range jsStrList(initial["elements"]) {
		if result := model.tryPush(value); !result.Ok() {
			t.Fatalf("%s %s: seed push %q: %v", model.name(), name, value, result)
		}
	}
	if initial["closed"] == true {
		model.close()
	}
	readers := model.readers()
	for _, reader := range readers {
		reader.drive()
	}

	steps := jsList(fixture["steps"])
	for i, raw := range steps {
		step := jsMap(raw)
		op := jsMap(step["op"])
		qLabel := fmt.Sprintf("%s %s step %d", model.name(), name, i)
		expected := consumeKeys(t, qLabel+" expected", jsMap(step["expected"]),
			"invalidates", "elements", "head", "len", "is_empty", "is_full", "closed")
		invalidates := consumeKeys(t, qLabel+" expected.invalidates", jsMap(expected["invalidates"]),
			"value", "head", "len", "is_empty", "is_full", "closed")
		excuseKey(t, expected, "invalidates", "container: its reader classes are asserted key-by-key against the expected.invalidates block below")
		before := map[string]int{}
		for kind, reader := range readers {
			before[kind] = reader.drive()
		}

		var gotReturn any
		switch opType := jsStr(op["type"]); opType {
		case "push", "try_push":
			result := model.tryPush(jsStr(op["value"]))
			if !result.Ok() {
				gotReturn = result.String()
			}
		case "pop", "try_pop":
			value, result := model.tryPop()
			if result.Ok() {
				gotReturn = value
			} else {
				gotReturn = result.String()
			}
		case "close":
			model.close()
		case "batch":
			var values []string
			for _, rawInner := range jsList(op["ops"]) {
				inner := jsMap(rawInner)
				if jsStr(inner["type"]) != "push" {
					t.Fatalf("%s %s step %d: unsupported batch op %v",
						model.name(), name, i, inner)
				}
				values = append(values, jsStr(inner["value"]))
			}
			model.batchPush(values)
		default:
			t.Fatalf("%s %s step %d: unknown op %q",
				model.name(), name, i, opType)
		}

		label := fmt.Sprintf("%s %s step %d", model.name(), name, i)
		for kind := range invalidates {
			reader := readers[kind]
			if reader == nil {
				t.Fatalf("%s: no reader for invalidates.%s", label, kind)
			}
			readerKind := kind
			assertKeyWith(t, invalidates, kind, func(want any) {
				assertInvalidationDelta(t, label+" invalidates."+readerKind,
					reader, before[readerKind], want == true)
			})
		}
		if want, ok := step["returns"]; ok && want != nil &&
			!reflect.DeepEqual(gotReturn, want) {
			t.Errorf("%s returns=%v, want %v", label, gotReturn, want)
		}
		assertQueueFixtureState(t, label, model, expected)
	}
	return len(steps)
}

func TestQueueCellCorpusAllFlavors(t *testing.T) {
	totals := map[string]int{}
	for _, name := range queueFixtures {
		fixture := loadQueueFamilyFixture(t, name)
		capacity := queueCapacity(jsMap(fixture["initial"]))
		models := []queueFixtureModel{
			newSyncQueueFixtureModel(capacity),
			newTSQueueFixtureModel(capacity),
			newAsyncQueueFixtureModel(capacity),
		}
		for _, model := range models {
			totals[model.name()] += replayQueueFixture(t, name, model)
		}
	}
	for _, flavor := range []string{"sync", "thread-safe", "async"} {
		if totals[flavor] != 31 {
			t.Fatalf("%s QueueCell replayed %d steps, want all 31", flavor, totals[flavor])
		}
	}
}

// --- TopicCell --------------------------------------------------------------

type topicFixtureModel interface {
	name() string
	subscribe(string, TopicDurability)
	reconnect(string)
	disconnect(string)
	publish(string)
	advance(string) any
	gc() int
	restart()
	baseOffset() int
	elements() []string
	subscription(string) (TopicSubscriptionSnapshot, bool)
	read(string) ([]string, bool)
	newReader(string) qfReader
	dispose()
}

type syncTopicFixtureModel struct {
	ctx *Context
	t   *TopicCell[string]
}

func (m *syncTopicFixtureModel) name() string { return "sync" }
func (m *syncTopicFixtureModel) subscribe(id string, d TopicDurability) {
	m.t.Subscribe(id, d)
}
func (m *syncTopicFixtureModel) reconnect(id string)  { m.t.Reconnect(id) }
func (m *syncTopicFixtureModel) disconnect(id string) { m.t.Disconnect(id) }
func (m *syncTopicFixtureModel) publish(v string)     { m.t.Publish(v) }
func (m *syncTopicFixtureModel) advance(id string) any {
	value, ok := m.t.Read(id)
	m.t.Advance(id, 1)
	if !ok {
		return nil
	}
	return value
}
func (m *syncTopicFixtureModel) gc() int         { return m.t.GC() }
func (m *syncTopicFixtureModel) restart()        { m.t.Restart() }
func (m *syncTopicFixtureModel) baseOffset() int { return m.t.BaseOffset() }
func (m *syncTopicFixtureModel) elements() []string {
	return m.t.Elements()
}
func (m *syncTopicFixtureModel) subscription(id string) (TopicSubscriptionSnapshot, bool) {
	return m.t.Subscription(id)
}
func (m *syncTopicFixtureModel) read(id string) ([]string, bool) {
	return m.t.ReadStream(id)
}
func (m *syncTopicFixtureModel) newReader(id string) qfReader {
	handle := m.t.ReaderHandle(id)
	return newQFSyncReader(m.ctx, func(c *Compute) int {
		read := Get(c, handle)
		return len(read.Elements)
	})
}
func (m *syncTopicFixtureModel) dispose() {}

type tsTopicFixtureModel struct {
	ts *ThreadSafeContext
	t  *ThreadSafeTopicCell[string]
}

func (m *tsTopicFixtureModel) name() string { return "thread-safe" }
func (m *tsTopicFixtureModel) subscribe(id string, d TopicDurability) {
	m.t.Subscribe(id, d)
}
func (m *tsTopicFixtureModel) reconnect(id string)  { m.t.Reconnect(id) }
func (m *tsTopicFixtureModel) disconnect(id string) { m.t.Disconnect(id) }
func (m *tsTopicFixtureModel) publish(v string)     { m.t.Publish(v) }
func (m *tsTopicFixtureModel) advance(id string) any {
	value, ok := m.t.Read(id)
	m.t.Advance(id, 1)
	if !ok {
		return nil
	}
	return value
}
func (m *tsTopicFixtureModel) gc() int         { return m.t.GC() }
func (m *tsTopicFixtureModel) restart()        { m.t.Restart() }
func (m *tsTopicFixtureModel) baseOffset() int { return m.t.BaseOffset() }
func (m *tsTopicFixtureModel) elements() []string {
	return m.t.Elements()
}
func (m *tsTopicFixtureModel) subscription(id string) (TopicSubscriptionSnapshot, bool) {
	return m.t.Subscription(id)
}
func (m *tsTopicFixtureModel) read(id string) ([]string, bool) {
	return m.t.ReadStream(id)
}
func (m *tsTopicFixtureModel) newReader(id string) qfReader {
	handle := m.t.ReaderHandle(id)
	return newQFTSReader(m.ts, func(c *Compute) int {
		read := Get(c, handle)
		return len(read.Elements)
	})
}
func (m *tsTopicFixtureModel) dispose() {}

type asyncTopicFixtureModel struct {
	ctx *AsyncContext
	t   *AsyncTopicCell[string]
}

func (m *asyncTopicFixtureModel) name() string { return "async" }
func (m *asyncTopicFixtureModel) subscribe(id string, d TopicDurability) {
	m.t.Subscribe(id, d)
}
func (m *asyncTopicFixtureModel) reconnect(id string)  { m.t.Reconnect(id) }
func (m *asyncTopicFixtureModel) disconnect(id string) { m.t.Disconnect(id) }
func (m *asyncTopicFixtureModel) publish(v string)     { m.t.Publish(v) }
func (m *asyncTopicFixtureModel) advance(id string) any {
	value, ok := m.t.Read(nil, id)
	m.t.Advance(id, 1)
	if !ok {
		return nil
	}
	return value
}
func (m *asyncTopicFixtureModel) gc() int         { return m.t.GC() }
func (m *asyncTopicFixtureModel) restart()        { m.t.Restart() }
func (m *asyncTopicFixtureModel) baseOffset() int { return m.t.BaseOffset() }
func (m *asyncTopicFixtureModel) elements() []string {
	return m.t.Elements()
}
func (m *asyncTopicFixtureModel) subscription(id string) (TopicSubscriptionSnapshot, bool) {
	return m.t.Subscription(id)
}
func (m *asyncTopicFixtureModel) read(id string) ([]string, bool) {
	return m.t.ReadStream(nil, id)
}
func (m *asyncTopicFixtureModel) newReader(id string) qfReader {
	return newQFAsyncReader(m.ctx, func(cc *AsyncComputeContext) int {
		values, _ := m.t.ReadStream(cc, id)
		return len(values)
	})
}
func (m *asyncTopicFixtureModel) dispose() { m.ctx.DisposeAsync() }

func topicSnapshot(initial map[string]any) TopicSnapshot[string] {
	snapshot := TopicSnapshot[string]{
		BaseOffset: jsInt(initial["base_offset"]),
		Elements:   jsStrList(initial["elements"]),
	}
	subscriptions := jsMap(initial["subscriptions"])
	ids := make([]string, 0, len(subscriptions))
	for id := range subscriptions {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		raw := jsMap(subscriptions[id])
		snapshot.Subscriptions = append(snapshot.Subscriptions, TopicSubscriptionSnapshot{
			ID: id, Cursor: jsInt(raw["cursor"]),
			Durability: TopicDurability(jsStr(raw["durability"])),
			Connected:  raw["connected"] == true,
		})
	}
	return snapshot
}

func newTopicModels(snapshot TopicSnapshot[string]) []topicFixtureModel {
	syncCtx := NewContext()
	ts := NewThreadSafeContext()
	asyncCtx := NewAsyncContext()
	return []topicFixtureModel{
		&syncTopicFixtureModel{
			ctx: syncCtx, t: NewTopicCellFromSnapshot(syncCtx, snapshot),
		},
		&tsTopicFixtureModel{
			ts: ts, t: NewThreadSafeTopicCellFromSnapshot(ts, snapshot),
		},
		&asyncTopicFixtureModel{
			ctx: asyncCtx, t: NewAsyncTopicCellFromSnapshot(asyncCtx, snapshot),
		},
	}
}

func assertTopicState(
	t *testing.T,
	label string,
	model topicFixtureModel,
	expected map[string]any,
) {
	t.Helper()
	assertKey(t, expected, "base_offset", model.baseOffset())
	assertKeyWith(t, expected, "elements", func(rawElements any) {
		if got, want := model.elements(), jsStrList(rawElements); !slices.Equal(got, want) {
			t.Errorf("%s elements=%v, want %v", label, got, want)
		}
	})
	assertKeyEach(t, expected, "subscriptions", func(id string, raw any) {
		want := jsMap(raw)
		got, ok := model.subscription(id)
		if !ok {
			t.Errorf("%s subscription %q missing", label, id)
			return
		}
		if got.Cursor != jsInt(want["cursor"]) ||
			got.Durability != TopicDurability(jsStr(want["durability"])) ||
			got.Connected != (want["connected"] == true) {
			t.Errorf("%s subscription %q=%+v, want %v", label, id, got, want)
		}
	})
	assertKeyEach(t, expected, "reads", func(id string, raw any) {
		got, exists := model.read(id)
		want := jsStrList(raw)
		if !exists || !slices.Equal(got, want) {
			t.Errorf("%s read %q=(%v,%v), want %v", label, id, got, exists, want)
		}
	})
}

func replayTopicFixture(t *testing.T, name string, model topicFixtureModel) int {
	t.Helper()
	defer model.dispose()
	fixture := loadQueueFamilyFixture(t, name)
	readers := map[string]qfReader{}
	steps := jsList(fixture["steps"])
	for i, raw := range steps {
		step := jsMap(raw)
		op := jsMap(step["op"])
		expected := consumeKeys(t, fmt.Sprintf("%s %s step %d expected", model.name(), name, i),
			jsMap(step["expected"]),
			"invalidates", "base_offset", "elements", "subscriptions", "reads")

		// TopicCell invalidation is keyed by subscriber id, so the readable set
		// is the fixture's own subscriber vocabulary rather than a fixed list.
		invalidates := jsMap(expected["invalidates"])
		for id := range invalidates {
			if readers[id] == nil {
				readers[id] = model.newReader(id)
				readers[id].drive()
			}
		}
		before := map[string]int{}
		for id, reader := range readers {
			before[id] = reader.drive()
		}

		var gotReturn any
		switch opType := jsStr(op["type"]); opType {
		case "subscribe":
			model.subscribe(jsStr(op["subscriber"]),
				TopicDurability(jsStr(op["durability"])))
		case "reconnect":
			model.reconnect(jsStr(op["subscriber"]))
		case "disconnect":
			model.disconnect(jsStr(op["subscriber"]))
		case "publish":
			model.publish(jsStr(op["value"]))
		case "advance":
			gotReturn = model.advance(jsStr(op["subscriber"]))
		case "gc":
			gotReturn = model.gc()
		case "restart":
			model.restart()
		default:
			t.Fatalf("%s %s step %d: unknown op %q", model.name(), name, i, opType)
		}

		label := fmt.Sprintf("%s %s step %d", model.name(), name, i)
		// Keyed by the fixture's own subscriber vocabulary rather than a fixed
		// list, so the claim is read out of the block itself. An empty block is
		// a real claim too: no subscriber, nothing to invalidate.
		// Both directions (#lzsubblockkeyset). The tracker walks the block, so a
		// subscriber the fixture names but this replay never created is a failure
		// rather than a claim compared against nothing; the reader loop then still
		// holds every reader that EXISTS to the block's statement, defaulting to
		// "must not have invalidated" for the ones it does not name.
		stated := jsMap(expected["invalidates"])
		assertKeyEach(t, expected, "invalidates", func(id string, _ any) {
			if _, known := readers[id]; !known {
				t.Errorf("%s: expected.invalidates names subscriber %q, which this replay never created — "+
					"the claim is compared against nothing", label, id)
			}
		})
		for id, reader := range readers {
			assertInvalidationDelta(t, label+" invalidates."+id, reader, before[id], stated[id] == true)
		}
		if want, ok := step["returns"]; ok && want != nil {
			switch want := want.(type) {
			case float64:
				if gotReturn != int(want) {
					t.Errorf("%s returns=%v, want %d", label, gotReturn, int(want))
				}
			default:
				if !reflect.DeepEqual(gotReturn, want) {
					t.Errorf("%s returns=%v, want %v", label, gotReturn, want)
				}
			}
		}
		assertTopicState(t, label, model, expected)
	}
	return len(steps)
}

func TestTopicCellCorpusAllFlavors(t *testing.T) {
	totals := map[string]int{}
	for _, name := range topicFixtures {
		fixture := loadQueueFamilyFixture(t, name)
		for _, model := range newTopicModels(topicSnapshot(jsMap(fixture["initial"]))) {
			totals[model.name()] += replayTopicFixture(t, name, model)
		}
	}
	for _, flavor := range []string{"sync", "thread-safe", "async"} {
		if totals[flavor] != 29 {
			t.Fatalf("%s TopicCell replayed %d steps, want all 29", flavor, totals[flavor])
		}
	}
}

// --- WorkQueueCell ----------------------------------------------------------

type workQueueFixtureModel interface {
	name() string
	push(string) uint64
	claim(string, int64) (WorkQueueDelivery[string], bool)
	ack(string, uint64) bool
	nack(string, uint64) bool
	reapExpired(int64) int
	pending() []WorkQueueItem[string]
	inFlight() []WorkQueueDelivery[string]
	deadLetters() []WorkQueueDeadLetter[string]
	reads() [4]int
	readers() map[string]qfReader
	dispose()
}

type syncWorkQueueFixtureModel struct {
	ctx *Context
	q   *WorkQueueCell[string]
}

func (m *syncWorkQueueFixtureModel) name() string         { return "sync" }
func (m *syncWorkQueueFixtureModel) push(v string) uint64 { return m.q.Push(v) }
func (m *syncWorkQueueFixtureModel) claim(w string, n int64) (WorkQueueDelivery[string], bool) {
	return m.q.Claim(w, n)
}
func (m *syncWorkQueueFixtureModel) ack(w string, id uint64) bool {
	return m.q.Ack(w, id)
}
func (m *syncWorkQueueFixtureModel) nack(w string, id uint64) bool {
	return m.q.Nack(w, id)
}
func (m *syncWorkQueueFixtureModel) reapExpired(n int64) int { return m.q.ReapExpired(n) }
func (m *syncWorkQueueFixtureModel) pending() []WorkQueueItem[string] {
	return m.q.PendingItems()
}
func (m *syncWorkQueueFixtureModel) inFlight() []WorkQueueDelivery[string] {
	return m.q.InFlightDeliveries()
}
func (m *syncWorkQueueFixtureModel) deadLetters() []WorkQueueDeadLetter[string] {
	return m.q.DeadLetterItems()
}
func (m *syncWorkQueueFixtureModel) reads() [4]int {
	empty := 0
	if m.q.IsEmpty() {
		empty = 1
	}
	return [4]int{m.q.PendingLen(), empty, m.q.InFlightLen(), m.q.DeadLetterLen()}
}
func (m *syncWorkQueueFixtureModel) readers() map[string]qfReader {
	h := m.q.ReaderHandles()
	return map[string]qfReader{
		"pending_len": newQFSyncReader(m.ctx, func(c *Compute) int {
			return Get(c, h.PendingLen)
		}),
		"is_empty": newQFSyncReader(m.ctx, func(c *Compute) int {
			if Get(c, h.IsEmpty) {
				return 1
			}
			return 0
		}),
		"in_flight_len": newQFSyncReader(m.ctx, func(c *Compute) int {
			return Get(c, h.InFlightLen)
		}),
		"dead_letter_len": newQFSyncReader(m.ctx, func(c *Compute) int {
			return Get(c, h.DeadLetterLen)
		}),
	}
}
func (m *syncWorkQueueFixtureModel) dispose() {}

type tsWorkQueueFixtureModel struct {
	ts *ThreadSafeContext
	q  *ThreadSafeWorkQueueCell[string]
}

func (m *tsWorkQueueFixtureModel) name() string         { return "thread-safe" }
func (m *tsWorkQueueFixtureModel) push(v string) uint64 { return m.q.Push(v) }
func (m *tsWorkQueueFixtureModel) claim(w string, n int64) (WorkQueueDelivery[string], bool) {
	return m.q.Claim(w, n)
}
func (m *tsWorkQueueFixtureModel) ack(w string, id uint64) bool {
	return m.q.Ack(w, id)
}
func (m *tsWorkQueueFixtureModel) nack(w string, id uint64) bool {
	return m.q.Nack(w, id)
}
func (m *tsWorkQueueFixtureModel) reapExpired(n int64) int { return m.q.ReapExpired(n) }
func (m *tsWorkQueueFixtureModel) pending() []WorkQueueItem[string] {
	return m.q.PendingItems()
}
func (m *tsWorkQueueFixtureModel) inFlight() []WorkQueueDelivery[string] {
	return m.q.InFlightDeliveries()
}
func (m *tsWorkQueueFixtureModel) deadLetters() []WorkQueueDeadLetter[string] {
	return m.q.DeadLetterItems()
}
func (m *tsWorkQueueFixtureModel) reads() [4]int {
	empty := 0
	if m.q.IsEmpty() {
		empty = 1
	}
	return [4]int{m.q.PendingLen(), empty, m.q.InFlightLen(), m.q.DeadLetterLen()}
}
func (m *tsWorkQueueFixtureModel) readers() map[string]qfReader {
	h := m.q.ReaderHandles()
	return map[string]qfReader{
		"pending_len": newQFTSReader(m.ts, func(c *Compute) int {
			return Get(c, h.PendingLen)
		}),
		"is_empty": newQFTSReader(m.ts, func(c *Compute) int {
			if Get(c, h.IsEmpty) {
				return 1
			}
			return 0
		}),
		"in_flight_len": newQFTSReader(m.ts, func(c *Compute) int {
			return Get(c, h.InFlightLen)
		}),
		"dead_letter_len": newQFTSReader(m.ts, func(c *Compute) int {
			return Get(c, h.DeadLetterLen)
		}),
	}
}
func (m *tsWorkQueueFixtureModel) dispose() {}

type asyncWorkQueueFixtureModel struct {
	ctx *AsyncContext
	q   *AsyncWorkQueueCell[string]
}

func (m *asyncWorkQueueFixtureModel) name() string         { return "async" }
func (m *asyncWorkQueueFixtureModel) push(v string) uint64 { return m.q.Push(v) }
func (m *asyncWorkQueueFixtureModel) claim(w string, n int64) (WorkQueueDelivery[string], bool) {
	return m.q.Claim(w, n)
}
func (m *asyncWorkQueueFixtureModel) ack(w string, id uint64) bool {
	return m.q.Ack(w, id)
}
func (m *asyncWorkQueueFixtureModel) nack(w string, id uint64) bool {
	return m.q.Nack(w, id)
}
func (m *asyncWorkQueueFixtureModel) reapExpired(n int64) int { return m.q.ReapExpired(n) }
func (m *asyncWorkQueueFixtureModel) pending() []WorkQueueItem[string] {
	return m.q.PendingItems()
}
func (m *asyncWorkQueueFixtureModel) inFlight() []WorkQueueDelivery[string] {
	return m.q.InFlightDeliveries()
}
func (m *asyncWorkQueueFixtureModel) deadLetters() []WorkQueueDeadLetter[string] {
	return m.q.DeadLetterItems()
}
func (m *asyncWorkQueueFixtureModel) reads() [4]int {
	empty := 0
	if m.q.IsEmpty(nil) {
		empty = 1
	}
	return [4]int{
		m.q.PendingLen(nil), empty, m.q.InFlightLen(nil), m.q.DeadLetterLen(nil),
	}
}
func (m *asyncWorkQueueFixtureModel) readers() map[string]qfReader {
	return map[string]qfReader{
		"pending_len": newQFAsyncReader(m.ctx, func(cc *AsyncComputeContext) int {
			return m.q.PendingLen(cc)
		}),
		"is_empty": newQFAsyncReader(m.ctx, func(cc *AsyncComputeContext) int {
			if m.q.IsEmpty(cc) {
				return 1
			}
			return 0
		}),
		"in_flight_len": newQFAsyncReader(m.ctx, func(cc *AsyncComputeContext) int {
			return m.q.InFlightLen(cc)
		}),
		"dead_letter_len": newQFAsyncReader(m.ctx, func(cc *AsyncComputeContext) int {
			return m.q.DeadLetterLen(cc)
		}),
	}
}
func (m *asyncWorkQueueFixtureModel) dispose() { m.ctx.DisposeAsync() }

func newWorkQueueModels(cfg queueFamilyConfig) []workQueueFixtureModel {
	syncCtx := NewContext()
	ts := NewThreadSafeContext()
	asyncCtx := NewAsyncContext()
	return []workQueueFixtureModel{
		&syncWorkQueueFixtureModel{
			ctx: syncCtx,
			q:   NewWorkQueueCell[string](syncCtx, cfg.VisibilityTimeout, cfg.MaxDeliveries),
		},
		&tsWorkQueueFixtureModel{
			ts: ts,
			q:  NewThreadSafeWorkQueueCell[string](ts, cfg.VisibilityTimeout, cfg.MaxDeliveries),
		},
		&asyncWorkQueueFixtureModel{
			ctx: asyncCtx,
			q:   NewAsyncWorkQueueCell[string](asyncCtx, cfg.VisibilityTimeout, cfg.MaxDeliveries),
		},
	}
}

func assertWorkQueueState(
	t *testing.T,
	label string,
	model workQueueFixtureModel,
	expected map[string]any,
) {
	t.Helper()
	assertKeyWith(t, expected, "pending", func(rawPending any) {
		wantPending := jsList(rawPending)
		gotPending := model.pending()
		if len(gotPending) != len(wantPending) {
			t.Errorf("%s pending len=%d, want %d", label, len(gotPending), len(wantPending))
		} else {
			for i, raw := range wantPending {
				want := jsMap(raw)
				got := gotPending[i]
				if got.ItemID != uint64(jsInt(want["item_id"])) ||
					got.Value != jsStr(want["value"]) ||
					got.Attempts != uint64(jsInt(want["attempts"])) {
					t.Errorf("%s pending[%d]=%+v, want %v", label, i, got, want)
				}
			}
		}
	})
	assertKeyWith(t, expected, "in_flight", func(rawFlight any) {
		wantFlight := jsList(rawFlight)
		gotFlight := model.inFlight()
		if len(gotFlight) != len(wantFlight) {
			t.Errorf("%s in_flight len=%d, want %d", label, len(gotFlight), len(wantFlight))
		} else {
			for i, raw := range wantFlight {
				want := jsMap(raw)
				got := gotFlight[i]
				if got.DeliveryID != uint64(jsInt(want["delivery_id"])) ||
					got.ItemID != uint64(jsInt(want["item_id"])) ||
					got.Value != jsStr(want["value"]) ||
					got.Worker != jsStr(want["worker"]) ||
					got.Attempt != uint64(jsInt(want["attempt"])) ||
					got.Deadline != int64(jsInt(want["deadline"])) {
					t.Errorf("%s in_flight[%d]=%+v, want %v", label, i, got, want)
				}
			}
		}
	})
	assertKeyWith(t, expected, "dead_letters", func(rawDead any) {
		wantDead := jsList(rawDead)
		gotDead := model.deadLetters()
		if len(gotDead) != len(wantDead) {
			t.Errorf("%s dead_letters len=%d, want %d", label, len(gotDead), len(wantDead))
		} else {
			for i, raw := range wantDead {
				want := jsMap(raw)
				got := gotDead[i]
				if got.ItemID != uint64(jsInt(want["item_id"])) ||
					got.Value != jsStr(want["value"]) ||
					got.Attempts != uint64(jsInt(want["attempts"])) ||
					got.Reason != WorkQueueDeadLetterReason(jsStr(want["reason"])) {
					t.Errorf("%s dead_letters[%d]=%+v, want %v", label, i, got, want)
				}
			}
		}
	})
	// DESCENDED into rather than read by a fixed list of four names
	// (#lzsubblockkeyset): the child block owns the unconsumed-key check, so a
	// fifth read added upstream fails here instead of being walked past.
	kinds := []string{"pending_len", "is_empty", "in_flight_len", "dead_letter_len"}
	reads := assertKeySub(t, expected, "reads", kinds...)
	gotReads := model.reads()
	for i, kind := range kinds {
		index := i
		assertKeyWith(t, reads, kind, func(raw any) {
			want := 0
			if kind == "is_empty" {
				if raw == true {
					want = 1
				}
			} else {
				want = jsInt(raw)
			}
			if gotReads[index] != want {
				t.Errorf("%s reads.%s=%d, want %d", label, kind, gotReads[index], want)
			}
		})
	}
}

func replayWorkQueueFixture(t *testing.T, name string, model workQueueFixtureModel) int {
	t.Helper()
	defer model.dispose()
	fixture := loadQueueFamilyFixture(t, name)
	readers := model.readers()
	for _, reader := range readers {
		reader.drive()
	}
	steps := jsList(fixture["steps"])
	for i, raw := range steps {
		step := jsMap(raw)
		op := jsMap(step["op"])
		wLabel := fmt.Sprintf("%s %s step %d", model.name(), name, i)
		expected := consumeKeys(t, wLabel+" expected", jsMap(step["expected"]),
			"invalidates", "pending", "in_flight", "dead_letters", "reads")
		invalidates := consumeKeys(t, wLabel+" expected.invalidates", jsMap(expected["invalidates"]),
			"pending_len", "in_flight_len", "dead_letter_len", "is_empty")
		excuseKey(t, expected, "invalidates", "container: its four reader classes are asserted key-by-key against the expected.invalidates block below")
		before := map[string]int{}
		for kind, reader := range readers {
			before[kind] = reader.drive()
		}

		var gotReturn any
		switch opType := jsStr(op["type"]); opType {
		case "push":
			gotReturn = int(model.push(jsStr(op["value"])))
		case "claim":
			delivery, ok := model.claim(jsStr(op["worker"]), int64(jsInt(op["now"])))
			if ok {
				gotReturn = delivery
			}
		case "ack":
			gotReturn = model.ack(jsStr(op["worker"]), uint64(jsInt(op["delivery_id"])))
		case "nack":
			gotReturn = model.nack(jsStr(op["worker"]), uint64(jsInt(op["delivery_id"])))
		case "reap_expired":
			gotReturn = model.reapExpired(int64(jsInt(op["now"])))
		default:
			t.Fatalf("%s %s step %d: unknown op %q", model.name(), name, i, opType)
		}

		label := fmt.Sprintf("%s %s step %d", model.name(), name, i)
		for kind, reader := range readers {
			readerKind, readerFor := kind, reader
			assertKeyWith(t, invalidates, kind, func(want any) {
				assertInvalidationDelta(t, label+" invalidates."+readerKind,
					readerFor, before[readerKind], want == true)
			})
		}
		if want, ok := step["returns"]; ok {
			switch want := want.(type) {
			case nil:
				if gotReturn != nil {
					t.Errorf("%s returns=%v, want nil", label, gotReturn)
				}
			case float64:
				if gotReturn != int(want) {
					t.Errorf("%s returns=%v, want %d", label, gotReturn, int(want))
				}
			case bool:
				if gotReturn != want {
					t.Errorf("%s returns=%v, want %v", label, gotReturn, want)
				}
			case map[string]any:
				got, ok := gotReturn.(WorkQueueDelivery[string])
				if !ok || got.DeliveryID != uint64(jsInt(want["delivery_id"])) ||
					got.ItemID != uint64(jsInt(want["item_id"])) ||
					got.Value != jsStr(want["value"]) ||
					got.Worker != jsStr(want["worker"]) ||
					got.Attempt != uint64(jsInt(want["attempt"])) ||
					got.Deadline != int64(jsInt(want["deadline"])) {
					t.Errorf("%s returns=%v, want %v", label, gotReturn, want)
				}
			default:
				// Fail closed (#lzscenariobodyskip). Without this arm a
				// `returns` in any other JSON shape — a string, a list — fell
				// through UNASSERTED while the step still counted as replayed.
				// Sibling queue fixtures already state string returns, so this
				// is one corpus edit away from a silent false green.
				t.Fatalf("%s: unsupported `returns` shape %T (%v)", label, want, want)
			}
		}
		assertWorkQueueState(t, label, model, expected)
	}
	return len(steps)
}

func TestWorkQueueCellCorpusAllFlavors(t *testing.T) {
	totals := map[string]int{}
	for _, name := range workQueueFixtures {
		fixture := loadQueueFamilyFixture(t, name)
		cfg := queueFamilyConfigOf(t, name, fixture)
		for _, model := range newWorkQueueModels(cfg) {
			totals[model.name()] += replayWorkQueueFixture(t, name, model)
		}
	}
	for _, flavor := range []string{"sync", "thread-safe", "async"} {
		if totals[flavor] != 18 {
			t.Fatalf("%s WorkQueueCell replayed %d steps, want all 18", flavor, totals[flavor])
		}
	}
}

// Mutation-discriminating regression: the corpus expires only one lease per
// step, so reversing the required delivery-id sort would otherwise stay green.
func TestWorkQueueMultiExpiryOrderAllFlavors(t *testing.T) {
	for _, model := range newWorkQueueModels(queueFamilyConfig{VisibilityTimeout: 10, MaxDeliveries: 3}) {
		model := model
		t.Run(model.name(), func(t *testing.T) {
			defer model.dispose()
			model.push("a")
			model.push("b")
			first, _ := model.claim("w0", 0)
			second, _ := model.claim("w1", 0)
			if first.DeliveryID >= second.DeliveryID {
				t.Fatal("delivery ids are not monotone")
			}
			if got := model.reapExpired(11); got != 2 {
				t.Fatalf("reaped=%d, want 2", got)
			}
			pending := model.pending()
			if len(pending) != 2 || pending[0].Value != "a" || pending[1].Value != "b" {
				t.Fatalf("multi-expiry pending=%v, want deterministic [a b]", pending)
			}
		})
	}
}

func TestThreadSafeQueueFamilyConcurrency(t *testing.T) {
	t.Run("queue_push_and_read", func(t *testing.T) {
		ts := NewThreadSafeContext()
		q := NewThreadSafeQueueCell[string](ts)
		var wg sync.WaitGroup
		for worker := 0; worker < 4; worker++ {
			wg.Add(2)
			go func(worker int) {
				defer wg.Done()
				for i := 0; i < 50; i++ {
					if result := q.TryPush(fmt.Sprintf("%d-%d", worker, i)); !result.Ok() {
						t.Errorf("push: %v", result)
						return
					}
				}
			}(worker)
			go func() {
				defer wg.Done()
				for i := 0; i < 50; i++ {
					q.Len()
					q.Head()
				}
			}()
		}
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent queue push/read deadlocked")
		}
		if q.Len() != 200 {
			t.Fatalf("len=%d, want 200", q.Len())
		}
	})

	t.Run("exclusive_work_claim", func(t *testing.T) {
		ts := NewThreadSafeContext()
		q := NewThreadSafeWorkQueueCell[string](ts, 1000, 3)
		for i := 0; i < 200; i++ {
			q.Push(fmt.Sprintf("job-%d", i))
		}
		var (
			wg      sync.WaitGroup
			claimed sync.Map
			total   atomic.Int64
		)
		for worker := 0; worker < 8; worker++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				id := fmt.Sprintf("w%d", worker)
				for {
					delivery, ok := q.Claim(id, 0)
					if !ok {
						return
					}
					if _, loaded := claimed.LoadOrStore(delivery.ItemID, struct{}{}); loaded {
						t.Errorf("item %d claimed twice", delivery.ItemID)
					}
					total.Add(1)
					if !q.Ack(id, delivery.DeliveryID) {
						t.Errorf("ack %d failed", delivery.DeliveryID)
					}
				}
			}(worker)
		}
		wg.Wait()
		if total.Load() != 200 {
			t.Fatalf("claims=%d, want 200", total.Load())
		}
	})

	t.Run("topic_publish_and_read", func(t *testing.T) {
		ts := NewThreadSafeContext()
		topic := NewThreadSafeTopicCell[string](ts)
		for _, id := range []string{"a", "b", "c", "d"} {
			topic.Subscribe(id, TopicDurable)
		}
		var wg sync.WaitGroup
		for writer := 0; writer < 4; writer++ {
			wg.Add(1)
			go func(writer int) {
				defer wg.Done()
				for i := 0; i < 50; i++ {
					topic.Publish(fmt.Sprintf("%d-%d", writer, i))
				}
			}(writer)
		}
		for _, id := range []string{"a", "b", "c", "d"} {
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				for i := 0; i < 50; i++ {
					topic.ReadStream(id)
				}
			}(id)
		}
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent topic publish/read deadlocked")
		}
		if len(topic.Elements()) != 200 {
			t.Fatalf("retained elements=%d, want 200", len(topic.Elements()))
		}
	})
}

// Effects run during the operation, so these probes distinguish one coalesced
// frontier walk from several post-hoc clears that would look identical after
// the operation completed.
func TestThreadSafeQueueFamilyAtomicInvalidation(t *testing.T) {
	t.Run("queue_len_and_full", func(t *testing.T) {
		ts := NewThreadSafeContext()
		q := NewBoundedThreadSafeQueueCell[string](ts, 2)
		q.TryPush("a")
		q.TryPush("b")
		h := q.ReaderHandles()
		var runs atomic.Int64
		ts.WithLock(func(ctx *Context) {
			NewEffect(ctx, func(c *Compute) func() {
				runs.Add(1)
				Get(c, h.Len)
				Get(c, h.IsFull)
				return nil
			})
		})
		baseline := runs.Load()
		q.TryPop()
		if delta := runs.Load() - baseline; delta != 1 {
			t.Fatalf("one pop reran two-kind observer %d times, want 1", delta)
		}
	})

	t.Run("topic_two_subscribers", func(t *testing.T) {
		ts := NewThreadSafeContext()
		topic := NewThreadSafeTopicCell[string](ts)
		topic.Subscribe("alpha", TopicDurable)
		topic.Subscribe("beta", TopicDurable)
		alpha := topic.ReaderHandle("alpha")
		beta := topic.ReaderHandle("beta")
		var runs atomic.Int64
		ts.WithLock(func(ctx *Context) {
			NewEffect(ctx, func(c *Compute) func() {
				runs.Add(1)
				Get(c, alpha)
				Get(c, beta)
				return nil
			})
		})
		baseline := runs.Load()
		topic.Publish("x")
		if delta := runs.Load() - baseline; delta != 1 {
			t.Fatalf("one publish reran two-subscriber observer %d times, want 1", delta)
		}
	})

	t.Run("work_pending_and_empty", func(t *testing.T) {
		ts := NewThreadSafeContext()
		q := NewThreadSafeWorkQueueCell[string](ts, 10, 2)
		h := q.ReaderHandles()
		var runs atomic.Int64
		ts.WithLock(func(ctx *Context) {
			NewEffect(ctx, func(c *Compute) func() {
				runs.Add(1)
				Get(c, h.PendingLen)
				Get(c, h.IsEmpty)
				return nil
			})
		})
		baseline := runs.Load()
		q.Push("job")
		if delta := runs.Load() - baseline; delta != 1 {
			t.Fatalf("one push reran two-kind observer %d times, want 1", delta)
		}
	})
}

// The fixture loader uses encoding/json internally, but keeping a direct decode
// here pins the expected.invalidates nesting independently of that helper.
func TestQueueFamilyFixtureInvalidationNesting(t *testing.T) {
	for _, name := range append(append(
		append([]string{}, queueFixtures...), topicFixtures...), workQueueFixtures...) {
		var data []byte
		for _, path := range []string{
			filepath.Join("..", "lazily-spec", "conformance", "collections", name),
			filepath.Join("conformance", "collections", name),
			filepath.Join("testdata", "conformance", "collections", name),
		} {
			var err error
			data, err = os.ReadFile(path)
			if err == nil {
				break
			}
		}
		if data == nil {
			t.Fatalf("fixture %s missing", name)
		}
		var fixture map[string]any
		if err := json.Unmarshal(data, &fixture); err != nil {
			t.Fatal(err)
		}
		for i, raw := range jsList(fixture["steps"]) {
			step := jsMap(raw)
			if _, bad := step["invalidates"]; bad {
				t.Fatalf("%s step %d has step-level invalidates", name, i)
			}
			if _, ok := jsMap(step["expected"])["invalidates"]; !ok {
				t.Fatalf("%s step %d lacks expected.invalidates", name, i)
			}
		}
	}
}
