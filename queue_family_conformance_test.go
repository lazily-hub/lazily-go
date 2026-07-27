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
	if value, ok := expected["elements"]; ok {
		want := jsStrList(value)
		if got := model.elements(); !reflect.DeepEqual(got, want) {
			t.Errorf("%s elements=%v, want %v", label, got, want)
		}
	}
	if value, ok := expected["head"]; ok {
		got, exists := model.head()
		if value == nil {
			if exists {
				t.Errorf("%s head=%q, want none", label, got)
			}
		} else if !exists || got != jsStr(value) {
			t.Errorf("%s head=(%q,%v), want %q", label, got, exists, jsStr(value))
		}
	}
	if value, ok := expected["len"]; ok && model.length() != jsInt(value) {
		t.Errorf("%s len=%d, want %d", label, model.length(), jsInt(value))
	}
	if value, ok := expected["is_empty"]; ok && model.isEmpty() != (value == true) {
		t.Errorf("%s is_empty=%v, want %v", label, model.isEmpty(), value)
	}
	if value, ok := expected["is_full"]; ok && model.isFull() != (value == true) {
		t.Errorf("%s is_full=%v, want %v", label, model.isFull(), value)
	}
	if value, ok := expected["closed"]; ok && model.isClosed() != (value == true) {
		t.Errorf("%s closed=%v, want %v", label, model.isClosed(), value)
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
		expected := jsMap(step["expected"])
		invalidates := jsMap(expected["invalidates"])
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
		for kind, wantRaw := range invalidates {
			reader := readers[kind]
			if reader == nil {
				t.Fatalf("%s: no reader for invalidates.%s", label, kind)
			}
			assertInvalidationDelta(t, label+" invalidates."+kind,
				reader, before[kind], wantRaw == true)
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
	if got, want := model.baseOffset(), jsInt(expected["base_offset"]); got != want {
		t.Errorf("%s base_offset=%d, want %d", label, got, want)
	}
	if got, want := model.elements(), jsStrList(expected["elements"]); !slices.Equal(got, want) {
		t.Errorf("%s elements=%v, want %v", label, got, want)
	}
	wantSubscriptions := jsMap(expected["subscriptions"])
	for id, raw := range wantSubscriptions {
		want := jsMap(raw)
		got, ok := model.subscription(id)
		if !ok {
			t.Errorf("%s subscription %q missing", label, id)
			continue
		}
		if got.Cursor != jsInt(want["cursor"]) ||
			got.Durability != TopicDurability(jsStr(want["durability"])) ||
			got.Connected != (want["connected"] == true) {
			t.Errorf("%s subscription %q=%+v, want %v", label, id, got, want)
		}
	}
	for id, raw := range jsMap(expected["reads"]) {
		got, exists := model.read(id)
		want := jsStrList(raw)
		if !exists || !slices.Equal(got, want) {
			t.Errorf("%s read %q=(%v,%v), want %v", label, id, got, exists, want)
		}
	}
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
		expected := jsMap(step["expected"])
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
		for id, reader := range readers {
			want := invalidates[id] == true
			assertInvalidationDelta(t, label+" invalidates."+id, reader, before[id], want)
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

func newWorkQueueModels(maxDeliveries uint64) []workQueueFixtureModel {
	syncCtx := NewContext()
	ts := NewThreadSafeContext()
	asyncCtx := NewAsyncContext()
	return []workQueueFixtureModel{
		&syncWorkQueueFixtureModel{
			ctx: syncCtx, q: NewWorkQueueCell[string](syncCtx, 10, maxDeliveries),
		},
		&tsWorkQueueFixtureModel{
			ts: ts, q: NewThreadSafeWorkQueueCell[string](ts, 10, maxDeliveries),
		},
		&asyncWorkQueueFixtureModel{
			ctx: asyncCtx, q: NewAsyncWorkQueueCell[string](asyncCtx, 10, maxDeliveries),
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
	wantPending := jsList(expected["pending"])
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
	wantFlight := jsList(expected["in_flight"])
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
	wantDead := jsList(expected["dead_letters"])
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
	reads := jsMap(expected["reads"])
	gotReads := model.reads()
	kinds := []string{"pending_len", "is_empty", "in_flight_len", "dead_letter_len"}
	for i, kind := range kinds {
		want := 0
		if kind == "is_empty" {
			if reads[kind] == true {
				want = 1
			}
		} else {
			want = jsInt(reads[kind])
		}
		if gotReads[i] != want {
			t.Errorf("%s reads.%s=%d, want %d", label, kind, gotReads[i], want)
		}
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
		expected := jsMap(step["expected"])
		invalidates := jsMap(expected["invalidates"])
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
			assertInvalidationDelta(t, label+" invalidates."+kind,
				reader, before[kind], invalidates[kind] == true)
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
			}
		}
		assertWorkQueueState(t, label, model, expected)
	}
	return len(steps)
}

func TestWorkQueueCellCorpusAllFlavors(t *testing.T) {
	totals := map[string]int{}
	for _, name := range workQueueFixtures {
		loadQueueFamilyFixture(t, name)
		maxDeliveries := uint64(3)
		if name == "workqueue_lease_deadletter.json" {
			maxDeliveries = 2
		}
		for _, model := range newWorkQueueModels(maxDeliveries) {
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
	for _, model := range newWorkQueueModels(3) {
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
