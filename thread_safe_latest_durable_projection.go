package lazily

// ThreadSafeLatestDurableProjection linearizes core transitions and graph
// invalidation through one ThreadSafeContext critical section.
type ThreadSafeLatestDurableProjection[K comparable, V comparable] struct {
	ctx         *ThreadSafeContext
	core        *LatestDurableProjectionCore[K, V]
	stateReader *Source[uint64]
	version     uint64
}

func NewThreadSafeLatestDurableProjection[K comparable, V comparable](ctx *ThreadSafeContext, generation uint64) *ThreadSafeLatestDurableProjection[K, V] {
	projection := &ThreadSafeLatestDurableProjection[K, V]{
		ctx:  ctx,
		core: NewLatestDurableProjectionCore[K, V](generation),
	}
	ctx.WithLock(func(graph *Context) {
		projection.stateReader = NewSource(graph, uint64(0))
	})
	return projection
}

func (projection *ThreadSafeLatestDurableProjection[K, V]) publishLocked(change LatestDurableChange) {
	if !change.State {
		return
	}
	projection.version++
	projection.stateReader.Set(projection.version)
}

func (projection *ThreadSafeLatestDurableProjection[K, V]) UpsertDesired(key K, epoch uint64, value V) (outcome LatestDurableUpsert) {
	projection.ctx.WithLock(func(*Context) {
		change, next := projection.core.UpsertDesired(key, epoch, value)
		projection.publishLocked(change)
		outcome = next
	})
	return
}

func (projection *ThreadSafeLatestDurableProjection[K, V]) Claim(key K, generation uint64) (outcome LatestDurableClaim[K, V]) {
	projection.ctx.WithLock(func(*Context) {
		change, next := projection.core.Claim(key, generation)
		projection.publishLocked(change)
		outcome = next
	})
	return
}

func (projection *ThreadSafeLatestDurableProjection[K, V]) AckApplied(key K, generation, epoch uint64) (outcome LatestDurableAck) {
	projection.ctx.WithLock(func(*Context) {
		change, next := projection.core.AckApplied(key, generation, epoch)
		projection.publishLocked(change)
		outcome = next
	})
	return
}

func (projection *ThreadSafeLatestDurableProjection[K, V]) FailRetryable(key K, generation, epoch uint64) (outcome LatestDurableFailure) {
	projection.ctx.WithLock(func(*Context) {
		change, next := projection.core.FailRetryable(key, generation, epoch)
		projection.publishLocked(change)
		outcome = next
	})
	return
}

func (projection *ThreadSafeLatestDurableProjection[K, V]) Reconnect(generation uint64) (outcome LatestDurableReconnect) {
	projection.ctx.WithLock(func(*Context) {
		change, next := projection.core.Reconnect(generation)
		projection.publishLocked(change)
		outcome = next
	})
	return
}

func (projection *ThreadSafeLatestDurableProjection[K, V]) Generation() (value uint64) {
	projection.ctx.WithLock(func(*Context) { value = projection.core.Generation() })
	return
}
func (projection *ThreadSafeLatestDurableProjection[K, V]) Count() (value int) {
	projection.ctx.WithLock(func(*Context) { value = projection.core.Count() })
	return
}
func (projection *ThreadSafeLatestDurableProjection[K, V]) State(key K) (state LatestDurableKeyState[K, V], ok bool) {
	projection.ctx.WithLock(func(*Context) { state, ok = projection.core.State(key) })
	return
}
func (projection *ThreadSafeLatestDurableProjection[K, V]) Snapshot() (snapshot LatestDurableSnapshot[K, V]) {
	projection.ctx.WithLock(func(*Context) { snapshot = projection.core.Snapshot() })
	return
}
func (projection *ThreadSafeLatestDurableProjection[K, V]) StateReader() *Source[uint64] {
	return projection.stateReader
}
