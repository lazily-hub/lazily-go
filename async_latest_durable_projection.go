package lazily

import "sync"

// AsyncLatestDurableProjection publishes transitions to an AsyncContext source.
// The transition surface is synchronous because only the caller's sink I/O
// awaits; claim/ack/retry remain a deterministic state machine.
type AsyncLatestDurableProjection[K comparable, V comparable] struct {
	mu          sync.Mutex
	core        *LatestDurableProjectionCore[K, V]
	stateReader *AsyncSource[uint64]
	version     uint64
}

func NewAsyncLatestDurableProjection[K comparable, V comparable](ctx *AsyncContext, generation uint64) *AsyncLatestDurableProjection[K, V] {
	return &AsyncLatestDurableProjection[K, V]{
		core:        NewLatestDurableProjectionCore[K, V](generation),
		stateReader: NewAsyncSource(ctx, uint64(0)),
	}
}

func (projection *AsyncLatestDurableProjection[K, V]) publishLocked(change LatestDurableChange) {
	if !change.State {
		return
	}
	projection.version++
	projection.stateReader.Set(projection.version)
}

func (projection *AsyncLatestDurableProjection[K, V]) UpsertDesired(key K, epoch uint64, value V) LatestDurableUpsert {
	projection.mu.Lock()
	defer projection.mu.Unlock()
	change, outcome := projection.core.UpsertDesired(key, epoch, value)
	projection.publishLocked(change)
	return outcome
}

func (projection *AsyncLatestDurableProjection[K, V]) Claim(key K, generation uint64) LatestDurableClaim[K, V] {
	projection.mu.Lock()
	defer projection.mu.Unlock()
	change, outcome := projection.core.Claim(key, generation)
	projection.publishLocked(change)
	return outcome
}

func (projection *AsyncLatestDurableProjection[K, V]) AckApplied(key K, generation, epoch uint64) LatestDurableAck {
	projection.mu.Lock()
	defer projection.mu.Unlock()
	change, outcome := projection.core.AckApplied(key, generation, epoch)
	projection.publishLocked(change)
	return outcome
}

func (projection *AsyncLatestDurableProjection[K, V]) FailRetryable(key K, generation, epoch uint64) LatestDurableFailure {
	projection.mu.Lock()
	defer projection.mu.Unlock()
	change, outcome := projection.core.FailRetryable(key, generation, epoch)
	projection.publishLocked(change)
	return outcome
}

func (projection *AsyncLatestDurableProjection[K, V]) Reconnect(generation uint64) LatestDurableReconnect {
	projection.mu.Lock()
	defer projection.mu.Unlock()
	change, outcome := projection.core.Reconnect(generation)
	projection.publishLocked(change)
	return outcome
}

func (projection *AsyncLatestDurableProjection[K, V]) Generation() uint64 {
	projection.mu.Lock()
	defer projection.mu.Unlock()
	return projection.core.Generation()
}
func (projection *AsyncLatestDurableProjection[K, V]) Count() int {
	projection.mu.Lock()
	defer projection.mu.Unlock()
	return projection.core.Count()
}
func (projection *AsyncLatestDurableProjection[K, V]) State(key K) (LatestDurableKeyState[K, V], bool) {
	projection.mu.Lock()
	defer projection.mu.Unlock()
	return projection.core.State(key)
}
func (projection *AsyncLatestDurableProjection[K, V]) Snapshot() LatestDurableSnapshot[K, V] {
	projection.mu.Lock()
	defer projection.mu.Unlock()
	return projection.core.Snapshot()
}
func (projection *AsyncLatestDurableProjection[K, V]) StateReader() *AsyncSource[uint64] {
	return projection.stateReader
}
