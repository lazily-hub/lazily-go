package lazily

// LatestDurableProjection is the single-threaded reactive shell. StateReader is
// an aggregate dependency root; each true core transition advances it once.
type LatestDurableProjection[K comparable, V comparable] struct {
	core        *LatestDurableProjectionCore[K, V]
	stateReader *Source[uint64]
	version     uint64
}

func NewLatestDurableProjection[K comparable, V comparable](ctx *Context, generation uint64) *LatestDurableProjection[K, V] {
	return &LatestDurableProjection[K, V]{
		core:        NewLatestDurableProjectionCore[K, V](generation),
		stateReader: NewSource(ctx, uint64(0)),
	}
}

func (projection *LatestDurableProjection[K, V]) publish(change LatestDurableChange) {
	if !change.State {
		return
	}
	projection.version++
	projection.stateReader.Set(projection.version)
}

func (projection *LatestDurableProjection[K, V]) UpsertDesired(key K, epoch uint64, value V) LatestDurableUpsert {
	change, outcome := projection.core.UpsertDesired(key, epoch, value)
	projection.publish(change)
	return outcome
}

func (projection *LatestDurableProjection[K, V]) Claim(key K, generation uint64) LatestDurableClaim[K, V] {
	change, outcome := projection.core.Claim(key, generation)
	projection.publish(change)
	return outcome
}

func (projection *LatestDurableProjection[K, V]) AckApplied(key K, generation, epoch uint64) LatestDurableAck {
	change, outcome := projection.core.AckApplied(key, generation, epoch)
	projection.publish(change)
	return outcome
}

func (projection *LatestDurableProjection[K, V]) FailRetryable(key K, generation, epoch uint64) LatestDurableFailure {
	change, outcome := projection.core.FailRetryable(key, generation, epoch)
	projection.publish(change)
	return outcome
}

func (projection *LatestDurableProjection[K, V]) Reconnect(generation uint64) LatestDurableReconnect {
	change, outcome := projection.core.Reconnect(generation)
	projection.publish(change)
	return outcome
}

func (projection *LatestDurableProjection[K, V]) Generation() uint64 {
	return projection.core.Generation()
}
func (projection *LatestDurableProjection[K, V]) Count() int { return projection.core.Count() }
func (projection *LatestDurableProjection[K, V]) State(key K) (LatestDurableKeyState[K, V], bool) {
	return projection.core.State(key)
}
func (projection *LatestDurableProjection[K, V]) Snapshot() LatestDurableSnapshot[K, V] {
	return projection.core.Snapshot()
}
func (projection *LatestDurableProjection[K, V]) StateReader() *Source[uint64] {
	return projection.stateReader
}
