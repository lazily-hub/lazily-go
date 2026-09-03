// Latest-value durable projection authority (#lzlatestdurableprojection).
//
// Contract: lazily-spec v0.38.0, conformance/egress/latest_durable_projection.json.
// Formal model: lazily-formal v0.38.1, LazilyFormal.LatestDurableProjection.
package lazily

// LatestDurableRevision is one desired projection value at a monotone epoch.
type LatestDurableRevision[V comparable] struct {
	Epoch uint64
	Value V
}

// LatestDurableEnvelope is the exact attempt handed to a durable sink.
type LatestDurableEnvelope[K comparable, V comparable] struct {
	Generation uint64
	Key        K
	Epoch      uint64
	Value      V
}

// LatestDurableKeyState is the observable state for one key.
type LatestDurableKeyState[K comparable, V comparable] struct {
	Desired        *LatestDurableRevision[V]
	Inflight       *LatestDurableEnvelope[K, V]
	DurableThrough *uint64
}

// LatestDurableSnapshot is a detached copy of the full projection state.
type LatestDurableSnapshot[K comparable, V comparable] struct {
	Generation uint64
	Entries    map[K]LatestDurableKeyState[K, V]
}

type LatestDurableUpsertKind string

const (
	LatestDurableUpsertAccepted       LatestDurableUpsertKind = "accepted"
	LatestDurableUpsertUnchanged      LatestDurableUpsertKind = "unchanged"
	LatestDurableUpsertAlreadyDurable LatestDurableUpsertKind = "already_durable"
	LatestDurableUpsertStaleEpoch     LatestDurableUpsertKind = "stale_epoch"
	LatestDurableUpsertEpochConflict  LatestDurableUpsertKind = "epoch_conflict"
)

type LatestDurableUpsert struct {
	Kind           LatestDurableUpsertKind
	DurableThrough uint64
	Current        uint64
}

type LatestDurableClaimKind string

const (
	LatestDurableClaimClaimed         LatestDurableClaimKind = "claimed"
	LatestDurableClaimEmpty           LatestDurableClaimKind = "empty"
	LatestDurableClaimBusy            LatestDurableClaimKind = "busy"
	LatestDurableClaimStaleGeneration LatestDurableClaimKind = "stale_generation"
)

type LatestDurableClaim[K comparable, V comparable] struct {
	Kind     LatestDurableClaimKind
	Envelope LatestDurableEnvelope[K, V]
	Current  uint64
}

type LatestDurableAckKind string

const (
	LatestDurableAckAdvanced        LatestDurableAckKind = "advanced"
	LatestDurableAckUnchanged       LatestDurableAckKind = "unchanged"
	LatestDurableAckUnknownEpoch    LatestDurableAckKind = "unknown_epoch"
	LatestDurableAckStaleGeneration LatestDurableAckKind = "stale_generation"
)

type LatestDurableAck struct {
	Kind           LatestDurableAckKind
	DurableThrough uint64
	Current        uint64
}

type LatestDurableFailureKind string

const (
	LatestDurableFailurePending         LatestDurableFailureKind = "pending"
	LatestDurableFailureSuperseded      LatestDurableFailureKind = "superseded"
	LatestDurableFailureUnknownEpoch    LatestDurableFailureKind = "unknown_epoch"
	LatestDurableFailureStaleGeneration LatestDurableFailureKind = "stale_generation"
)

type LatestDurableFailure struct {
	Kind    LatestDurableFailureKind
	Current uint64
}

type LatestDurableReconnectKind string

const (
	LatestDurableReconnectAdvanced        LatestDurableReconnectKind = "advanced"
	LatestDurableReconnectUnchanged       LatestDurableReconnectKind = "unchanged"
	LatestDurableReconnectStaleGeneration LatestDurableReconnectKind = "stale_generation"
)

type LatestDurableReconnect struct {
	Kind       LatestDurableReconnectKind
	Generation uint64
	Current    uint64
	Requeued   int
	Superseded int
}

// LatestDurableChange reports whether a transition changed the snapshot.
type LatestDurableChange struct{ State bool }

type latestDurableEntry[K comparable, V comparable] struct {
	desired        *LatestDurableRevision[V]
	inflight       *LatestDurableEnvelope[K, V]
	durableThrough *uint64
}

func clonePtr[T any](value *T) *T {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func (entry *latestDurableEntry[K, V]) state() LatestDurableKeyState[K, V] {
	return LatestDurableKeyState[K, V]{
		Desired:        clonePtr(entry.desired),
		Inflight:       clonePtr(entry.inflight),
		DurableThrough: clonePtr(entry.durableThrough),
	}
}

// LatestDurableProjectionCore is the graph-independent per-key state machine.
type LatestDurableProjectionCore[K comparable, V comparable] struct {
	generation uint64
	entries    map[K]*latestDurableEntry[K, V]
}

func NewLatestDurableProjectionCore[K comparable, V comparable](generation uint64) *LatestDurableProjectionCore[K, V] {
	return &LatestDurableProjectionCore[K, V]{
		generation: generation,
		entries:    make(map[K]*latestDurableEntry[K, V]),
	}
}

func (core *LatestDurableProjectionCore[K, V]) Generation() uint64 { return core.generation }
func (core *LatestDurableProjectionCore[K, V]) Count() int         { return len(core.entries) }

func (core *LatestDurableProjectionCore[K, V]) entry(key K) *latestDurableEntry[K, V] {
	entry, ok := core.entries[key]
	if !ok {
		entry = &latestDurableEntry[K, V]{}
		core.entries[key] = entry
	}
	return entry
}

func (core *LatestDurableProjectionCore[K, V]) State(key K) (LatestDurableKeyState[K, V], bool) {
	entry, ok := core.entries[key]
	if !ok {
		return LatestDurableKeyState[K, V]{}, false
	}
	return entry.state(), true
}

func (core *LatestDurableProjectionCore[K, V]) Snapshot() LatestDurableSnapshot[K, V] {
	entries := make(map[K]LatestDurableKeyState[K, V], len(core.entries))
	for key, entry := range core.entries {
		entries[key] = entry.state()
	}
	return LatestDurableSnapshot[K, V]{Generation: core.generation, Entries: entries}
}

func (core *LatestDurableProjectionCore[K, V]) Pending(key K) bool {
	entry, ok := core.entries[key]
	return ok && entry.inflight == nil && entry.desired != nil
}

func (core *LatestDurableProjectionCore[K, V]) UpsertDesired(key K, epoch uint64, value V) (LatestDurableChange, LatestDurableUpsert) {
	entry := core.entry(key)
	if entry.durableThrough != nil && epoch <= *entry.durableThrough {
		return LatestDurableChange{}, LatestDurableUpsert{
			Kind: LatestDurableUpsertAlreadyDurable, DurableThrough: *entry.durableThrough,
		}
	}

	var newest *LatestDurableRevision[V]
	if entry.desired != nil {
		newest = entry.desired
	}
	if entry.inflight != nil && (newest == nil || entry.inflight.Epoch > newest.Epoch) {
		newest = &LatestDurableRevision[V]{Epoch: entry.inflight.Epoch, Value: entry.inflight.Value}
	}
	if newest != nil {
		if epoch < newest.Epoch {
			return LatestDurableChange{}, LatestDurableUpsert{Kind: LatestDurableUpsertStaleEpoch, Current: newest.Epoch}
		}
		if epoch == newest.Epoch {
			if value == newest.Value {
				return LatestDurableChange{}, LatestDurableUpsert{Kind: LatestDurableUpsertUnchanged}
			}
			return LatestDurableChange{}, LatestDurableUpsert{Kind: LatestDurableUpsertEpochConflict}
		}
	}
	entry.desired = &LatestDurableRevision[V]{Epoch: epoch, Value: value}
	return LatestDurableChange{State: true}, LatestDurableUpsert{Kind: LatestDurableUpsertAccepted}
}

func (core *LatestDurableProjectionCore[K, V]) Claim(key K, generation uint64) (LatestDurableChange, LatestDurableClaim[K, V]) {
	if generation != core.generation {
		return LatestDurableChange{}, LatestDurableClaim[K, V]{Kind: LatestDurableClaimStaleGeneration, Current: core.generation}
	}
	entry := core.entry(key)
	if entry.inflight != nil {
		return LatestDurableChange{}, LatestDurableClaim[K, V]{Kind: LatestDurableClaimBusy}
	}
	if entry.desired == nil {
		return LatestDurableChange{}, LatestDurableClaim[K, V]{Kind: LatestDurableClaimEmpty}
	}
	envelope := LatestDurableEnvelope[K, V]{Generation: core.generation, Key: key, Epoch: entry.desired.Epoch, Value: entry.desired.Value}
	entry.desired = nil
	entry.inflight = &envelope
	return LatestDurableChange{State: true}, LatestDurableClaim[K, V]{Kind: LatestDurableClaimClaimed, Envelope: envelope}
}

func (core *LatestDurableProjectionCore[K, V]) AckApplied(key K, generation, epoch uint64) (LatestDurableChange, LatestDurableAck) {
	if generation != core.generation {
		return LatestDurableChange{}, LatestDurableAck{Kind: LatestDurableAckStaleGeneration, Current: core.generation}
	}
	entry := core.entry(key)
	if entry.inflight == nil || entry.inflight.Epoch != epoch {
		if entry.durableThrough != nil && epoch <= *entry.durableThrough {
			return LatestDurableChange{}, LatestDurableAck{Kind: LatestDurableAckUnchanged, DurableThrough: *entry.durableThrough}
		}
		return LatestDurableChange{}, LatestDurableAck{Kind: LatestDurableAckUnknownEpoch}
	}
	entry.inflight = nil
	if entry.durableThrough == nil || epoch > *entry.durableThrough {
		durable := epoch
		entry.durableThrough = &durable
		return LatestDurableChange{State: true}, LatestDurableAck{Kind: LatestDurableAckAdvanced, DurableThrough: epoch}
	}
	return LatestDurableChange{State: true}, LatestDurableAck{Kind: LatestDurableAckUnchanged, DurableThrough: *entry.durableThrough}
}

func (core *LatestDurableProjectionCore[K, V]) FailRetryable(key K, generation, epoch uint64) (LatestDurableChange, LatestDurableFailure) {
	if generation != core.generation {
		return LatestDurableChange{}, LatestDurableFailure{Kind: LatestDurableFailureStaleGeneration, Current: core.generation}
	}
	entry := core.entry(key)
	if entry.inflight == nil || entry.inflight.Epoch != epoch {
		return LatestDurableChange{}, LatestDurableFailure{Kind: LatestDurableFailureUnknownEpoch}
	}
	inflight := entry.inflight
	entry.inflight = nil
	if entry.desired != nil && entry.desired.Epoch > inflight.Epoch {
		return LatestDurableChange{State: true}, LatestDurableFailure{Kind: LatestDurableFailureSuperseded}
	}
	entry.desired = &LatestDurableRevision[V]{Epoch: inflight.Epoch, Value: inflight.Value}
	return LatestDurableChange{State: true}, LatestDurableFailure{Kind: LatestDurableFailurePending}
}

func (core *LatestDurableProjectionCore[K, V]) Reconnect(newGeneration uint64) (LatestDurableChange, LatestDurableReconnect) {
	if newGeneration < core.generation {
		return LatestDurableChange{}, LatestDurableReconnect{Kind: LatestDurableReconnectStaleGeneration, Current: core.generation}
	}
	if newGeneration == core.generation {
		return LatestDurableChange{}, LatestDurableReconnect{Kind: LatestDurableReconnectUnchanged, Generation: core.generation}
	}
	requeued, superseded := 0, 0
	for _, entry := range core.entries {
		if entry.inflight == nil {
			continue
		}
		if entry.desired != nil && entry.desired.Epoch > entry.inflight.Epoch {
			superseded++
		} else {
			entry.desired = &LatestDurableRevision[V]{Epoch: entry.inflight.Epoch, Value: entry.inflight.Value}
			requeued++
		}
		entry.inflight = nil
	}
	core.generation = newGeneration
	return LatestDurableChange{State: true}, LatestDurableReconnect{
		Kind: LatestDurableReconnectAdvanced, Generation: newGeneration, Requeued: requeued, Superseded: superseded,
	}
}
