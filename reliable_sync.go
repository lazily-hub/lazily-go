package lazily

// Reliable sync protocol (#lzsync).
//
// Delivery-reliability over the Snapshot/Delta/CrdtSync planes
// (lazily-spec § Reliable Sync): gap recovery, at-least-once outbox, and
// OR-set / LWW liveness cells. The correctness backstop is lazily-formal
// ReliableSync.lean; the cross-language pins are
// lazily-spec/conformance/reliable-sync/.
//
// Three pure-protocol pieces (identical logic in every binding, no I/O / clock /
// storage engine baked in):
//
//   - ResyncCoordinator — receiver-side decision function over the inbound frame
//     stream (Apply / RequestSnapshot / Ignore), multi-epoch-span aware.
//   - DurableOutbox — sender-side at-least-once contract (append-before-send,
//     ack-through, replay-from-cursor). Ships InMemoryOutbox as the default; a
//     host plugs a durable store (agent-doc: SQLite) behind the interface.
//   - OrSet / WireLwwRegister — the liveness cells that ride the CrdtSync plane.
//
// The reverse-channel control frames are IpcMessageResyncRequest and
// IpcMessageOutboxAck — variants on the same framed, codec-negotiated,
// bidirectional message plane as Snapshot/Delta/CrdtSync, so they share one
// encode/decode path, one demux point, one FFI kind, and one in-band order. They
// match the conformance/reliable-sync/ fixtures and round-trip through JSON like
// the state frames.

import "sort"

// ---------------------------------------------------------------------------
// ResyncCoordinator — receiver decision function
// ---------------------------------------------------------------------------

// ResyncAction is the receiver decision for an inbound frame (spec §
// ResyncCoordinator). When the action is ResyncActionRequestSnapshot the ingest
// method also returns the from-epoch the sender must cover; the from-epoch is
// zero and meaningless for the other actions.
type ResyncAction int

const (
	// ResyncActionApply means apply the frame and advance the receiver epoch.
	ResyncActionApply ResyncAction = iota
	// ResyncActionRequestSnapshot means a gap was detected; request a fresh
	// Snapshot covering the returned from-epoch.
	ResyncActionRequestSnapshot
	// ResyncActionIgnore means drop the frame (already-applied re-delivery,
	// malformed, a duplicate request suppressed while resyncing, or a
	// reverse-channel control frame arriving at a data receiver).
	ResyncActionIgnore
)

// String renders the action name (parity with the fixture expect_action words).
func (a ResyncAction) String() string {
	switch a {
	case ResyncActionApply:
		return "Apply"
	case ResyncActionRequestSnapshot:
		return "RequestSnapshot"
	case ResyncActionIgnore:
		return "Ignore"
	default:
		return "Unknown"
	}
}

// ResyncCoordinator is the receiver-side reliable-sync coordinator.
//
// It holds lastEpoch (the highest epoch fully applied) and a resyncing flag (a
// RequestSnapshot is outstanding until a covering Snapshot lands, so further
// ahead-of-cursor deltas are ignored instead of re-requesting).
//
// Ingest advances lastEpoch on Apply — the caller MUST fold the frame's ops into
// its projection on Apply. This mirrors the ReliableSync.step Lean model.
type ResyncCoordinator struct {
	lastEpoch Epoch
	resyncing bool
}

// NewResyncCoordinator returns a coordinator at epoch 0 (fresh; a Snapshot seeds
// the first real epoch).
func NewResyncCoordinator() *ResyncCoordinator { return &ResyncCoordinator{} }

// NewResyncCoordinatorWithEpoch returns a coordinator that has already applied
// through lastEpoch.
func NewResyncCoordinatorWithEpoch(lastEpoch Epoch) *ResyncCoordinator {
	return &ResyncCoordinator{lastEpoch: lastEpoch}
}

// LastEpoch returns the highest epoch fully applied.
func (c *ResyncCoordinator) LastEpoch() Epoch { return c.lastEpoch }

// IsResyncing reports whether a resync request is outstanding (awaiting a
// covering snapshot).
func (c *ResyncCoordinator) IsResyncing() bool { return c.resyncing }

// IngestDelta classifies and folds an inbound Delta. On Apply this advances
// lastEpoch to delta.Epoch (multi-epoch-span aware) and clears resyncing. The
// second return value is the request-from epoch (only meaningful for
// ResyncActionRequestSnapshot).
func (c *ResyncCoordinator) IngestDelta(delta Delta) (ResyncAction, Epoch) {
	switch {
	case delta.BaseEpoch == c.lastEpoch:
		// Contiguous. Accept any span >= 1; reject an empty/backward epoch.
		if delta.Epoch >= delta.BaseEpoch+1 {
			c.lastEpoch = delta.Epoch
			c.resyncing = false
			return ResyncActionApply, 0
		}
		return ResyncActionIgnore, 0
	case delta.BaseEpoch < c.lastEpoch:
		// Already applied — a re-delivery (outbox replay / retry). Idempotent.
		return ResyncActionIgnore, 0
	default:
		// Gap: base_epoch > last_epoch. Request a covering snapshot once.
		if c.resyncing {
			return ResyncActionIgnore, 0
		}
		c.resyncing = true
		return ResyncActionRequestSnapshot, c.lastEpoch
	}
}

// IngestSnapshot adopts a Snapshot at snapshotEpoch — a full-state frame always
// applies, setting lastEpoch and clearing resyncing.
func (c *ResyncCoordinator) IngestSnapshot(snapshotEpoch Epoch) (ResyncAction, Epoch) {
	c.lastEpoch = snapshotEpoch
	c.resyncing = false
	return ResyncActionApply, 0
}

// Ingest classifies an inbound IpcMessage. CrdtSync is handled by the CRDT
// plane, and the reverse-channel control frames (ResyncRequest / OutboxAck) are
// for the sender's driver, not this data receiver, so both are Ignored here.
func (c *ResyncCoordinator) Ingest(msg IpcMessage) (ResyncAction, Epoch) {
	switch m := msg.(type) {
	case IpcMessageSnapshot:
		return c.IngestSnapshot(m.Value.Epoch)
	case IpcMessageDelta:
		return c.IngestDelta(m.Value)
	default:
		// CrdtSync / ResyncRequest / OutboxAck.
		return ResyncActionIgnore, 0
	}
}

// Ack returns the OutboxAck control frame that advertises this receiver's resume
// cursor on reconnect (and for periodic retention advance).
func (c *ResyncCoordinator) Ack() IpcMessage {
	return IpcMessageOutboxAck{Value: OutboxAck{ThroughEpoch: c.lastEpoch}}
}

// Span returns the number of epochs an applied Delta advances (epoch -
// base_epoch); 1 for an ordinary delta, > 1 for a coalesced multi-epoch flush.
func (d Delta) Span() Epoch { return d.Epoch - d.BaseEpoch }

// ---------------------------------------------------------------------------
// DurableOutbox — sender-side at-least-once contract
// ---------------------------------------------------------------------------

// OutboxEntry pairs a retained frame with its outbox retention key (the frame's
// accepted-event count).
type OutboxEntry struct {
	Epoch Epoch
	Msg   IpcMessage
}

// DurableOutbox is the sender-side at-least-once outbox contract (spec §
// DurableOutbox).
//
// Every frame is durably Appended BEFORE it is sent, retained until the peer
// proves receipt (AckThrough), and ReplayFrom a reconnect cursor re-sends
// everything the peer has not yet acked. Combined with the receiver's idempotent
// Ignore of already-applied deltas, this is at-least-once delivery with
// exactly-once effect.
type DurableOutbox interface {
	// Append persists msg at epoch before it is handed to the transport.
	Append(epoch Epoch, msg IpcMessage)
	// AckThrough records that the peer proved receipt through epoch; retained
	// frames <= epoch MAY be pruned.
	AckThrough(epoch Epoch)
	// ReplayFrom returns retained frames with epoch > cursor, ascending.
	ReplayFrom(cursor Epoch) []OutboxEntry
	// RetainedEpochs lists epochs still retained (not yet acked), ascending.
	RetainedEpochs() []Epoch
}

// InMemoryOutbox is the in-memory DurableOutbox — correct within a process
// lifetime; the default.
type InMemoryOutbox struct {
	entries      []OutboxEntry
	ackedThrough Epoch
}

// NewInMemoryOutbox returns an empty outbox.
func NewInMemoryOutbox() *InMemoryOutbox { return &InMemoryOutbox{} }

// AckedThrough returns the highest acked epoch (retention cursor).
func (o *InMemoryOutbox) AckedThrough() Epoch { return o.ackedThrough }

// Append records msg at epoch (append order preserved).
func (o *InMemoryOutbox) Append(epoch Epoch, msg IpcMessage) {
	o.entries = append(o.entries, OutboxEntry{Epoch: epoch, Msg: msg})
}

// AckThrough advances the retention cursor and prunes frames <= it.
func (o *InMemoryOutbox) AckThrough(epoch Epoch) {
	if epoch > o.ackedThrough {
		o.ackedThrough = epoch
	}
	kept := o.entries[:0]
	for _, e := range o.entries {
		if e.Epoch > o.ackedThrough {
			kept = append(kept, e)
		}
	}
	o.entries = kept
}

// ReplayFrom returns retained frames with epoch > cursor, ascending by epoch.
func (o *InMemoryOutbox) ReplayFrom(cursor Epoch) []OutboxEntry {
	out := make([]OutboxEntry, 0, len(o.entries))
	for _, e := range o.entries {
		if e.Epoch > cursor {
			out = append(out, e)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Epoch < out[j].Epoch })
	return out
}

// RetainedEpochs lists still-retained epochs, ascending.
func (o *InMemoryOutbox) RetainedEpochs() []Epoch {
	es := make([]Epoch, 0, len(o.entries))
	for _, e := range o.entries {
		es = append(es, e.Epoch)
	}
	sort.Slice(es, func(i, j int) bool { return es[i] < es[j] })
	return es
}

// ---------------------------------------------------------------------------
// Liveness cells (OR-set / LWW register) on the CrdtSync plane
// ---------------------------------------------------------------------------

// OrSet is an observed-remove set (OR-set) liveness cell.
//
// It models one entry's presence via add/remove tags: a (doc, pid) is present
// iff some add-tag is not shadowed by a remove that observed it. This gives the
// add-wins-over-stale-remove bias liveness needs (a re-open concurrent with a
// lagging close keeps the doc open). Join is the union of both tag sets, so it
// is a semilattice — out-of-order and duplicate delivery converge.
type OrSet struct {
	adds    map[string]struct{}
	removes map[string]struct{}
}

// NewOrSet returns an empty OR-set.
func NewOrSet() *OrSet {
	return &OrSet{adds: map[string]struct{}{}, removes: map[string]struct{}{}}
}

// Add mints a presence tag (an editor open / attach event mints a fresh tag).
func (s *OrSet) Add(tag string) { s.adds[tag] = struct{}{} }

// RemoveObserved removes, observing tags — only the add-tags this remove saw are
// shadowed.
func (s *OrSet) RemoveObserved(tags []string) {
	for _, t := range tags {
		s.removes[t] = struct{}{}
	}
}

// Present reports whether the entry is currently present (some add-tag not
// shadowed).
func (s *OrSet) Present() bool {
	for t := range s.adds {
		if _, shadowed := s.removes[t]; !shadowed {
			return true
		}
	}
	return false
}

// Join folds another replica's OR-set (union of adds and of removes).
func (s *OrSet) Join(other *OrSet) {
	for t := range other.adds {
		s.adds[t] = struct{}{}
	}
	for t := range other.removes {
		s.removes[t] = struct{}{}
	}
}

// Greater reports whether s dominates other in the (wall_time, logical, peer)
// total order used by the reliable-sync LWW liveness cells. It reuses the HLC
// stamp comparison so the wire and runtime orders agree.
func (s WireStamp) Greater(other WireStamp) bool {
	return HlcStampFromWire(s).Greater(HlcStampFromWire(other))
}

// WireLwwRegister is a last-writer-wins register liveness cell (per-pid alive,
// owner lease).
//
// Keyed by WireStamp ((wall_time, logical, peer) total order): the highest stamp
// wins, so an OS process-exit write (alive = false at a fresh stamp) dominates a
// stale re-assert. Join is the stamp-max, a semilattice.
type WireLwwRegister[V any] struct {
	stamp WireStamp
	value V
}

// NewWireLwwRegister returns a register holding value written at stamp.
func NewWireLwwRegister[V any](stamp WireStamp, value V) *WireLwwRegister[V] {
	return &WireLwwRegister[V]{stamp: stamp, value: value}
}

// Value returns the current value.
func (r *WireLwwRegister[V]) Value() V { return r.value }

// Stamp returns the current decisive stamp.
func (r *WireLwwRegister[V]) Stamp() WireStamp { return r.stamp }

// Set writes value at stamp iff it dominates the current stamp.
func (r *WireLwwRegister[V]) Set(stamp WireStamp, value V) {
	if stamp.Greater(r.stamp) {
		r.stamp = stamp
		r.value = value
	}
}

// Join folds another replica's register (keep the higher stamp).
func (r *WireLwwRegister[V]) Join(other *WireLwwRegister[V]) {
	if other.stamp.Greater(r.stamp) {
		r.stamp = other.stamp
		r.value = other.value
	}
}

// ---------------------------------------------------------------------------
// SyncDriver seams + loop
// ---------------------------------------------------------------------------

// Clock is the monotonic clock seam (spec § SyncDriver — policy injected, no
// runtime in core). The driver never schedules itself; the host calls Tick on
// its own cadence and supplies wall-free monotonic millis so the driver can
// timestamp progress and expose a stall signal without owning a clock source.
type Clock interface {
	// NowMillis returns milliseconds from an arbitrary fixed origin; monotonic,
	// non-decreasing.
	NowMillis() int64
}

// SnapshotProvider is the sender-side answer to a peer's ResyncRequest (spec §
// SyncDriver). When a receiver detects a gap it can no longer close from
// retained deltas, it asks for a covering Snapshot; the host plugs its
// projection in here to produce one at epoch >= fromEpoch.
type SnapshotProvider interface {
	// Snapshot returns a full-state IpcMessageSnapshot covering fromEpoch (its
	// epoch MUST be >= fromEpoch).
	Snapshot(fromEpoch Epoch) IpcMessage
}

// IpcSink is the outbound transport seam. Send returns a non-nil error when the
// frame could not be handed to the transport; the driver treats that as a stall
// (retain-and-retry), not a fatal error.
type IpcSink interface {
	Send(msg IpcMessage) error
}

// IpcSource is the inbound transport seam. Recv returns (msg, true, nil) for a
// frame, (_, false, nil) when the inbound queue is momentarily empty, and a
// non-nil error on a read failure (which the driver surfaces as DriverError).
type IpcSource interface {
	Recv() (msg IpcMessage, present bool, err error)
}

// Progress is what one SyncDriver.Tick accomplished (spec § SyncDriver).
//
// Applied are the inbound Snapshot/Delta/CrdtSync frames the host MUST fold into
// its projection this tick — the driver has already advanced the receiver cursor
// for them, so folding is the caller's remaining obligation.
type Progress struct {
	// Sent is the count of data frames pushed to the sink this tick (fresh
	// enqueues + reconnect replays).
	Sent int
	// Applied are inbound frames the host must fold into its projection.
	Applied []IpcMessage
	// ResyncRequested reports that a gap was detected inbound and a
	// ResyncRequest was emitted to the peer.
	ResyncRequested bool
	// SnapshotsServed is the count of inbound ResyncRequests answered with a
	// provider snapshot this tick.
	SnapshotsServed int
	// PeerAckedThrough is the peer's ack cursor after this tick (our outbox
	// retention / resume point).
	PeerAckedThrough Epoch
	// Retained is the count of outbox frames still unacked (retained for
	// reconnect replay).
	Retained int
}

// DriverError is a transport error surfaced by SyncDriver.Tick.
//
// A sink failure is not fatal — the frame is retained in the outbox and replayed
// on the next SyncDriver.OnReconnect, so it is reported as a stall, not an
// error. Only a source read failure is returned as a DriverError, signalling the
// host to re-establish the transport and call OnReconnect.
type DriverError struct {
	// Source is the inbound source read failure that stalled the tick.
	Source error
}

// Error implements the error interface.
func (e *DriverError) Error() string {
	return "reliable-sync source read failed: " + e.Source.Error()
}

// Unwrap exposes the underlying source error.
func (e *DriverError) Unwrap() error { return e.Source }

// SyncDriver is the full-duplex reliable-sync loop driver (spec § SyncDriver).
//
// One driver drives one peer connection over a caller-supplied IpcSink/IpcSource
// pair (agent-doc wraps its Unix-domain socket). It composes the three
// pure-protocol pieces into the loop shape the spec pins:
//
//  1. drain — pop host-enqueued outbound data frames, Append each to the
//     DurableOutbox before sending (at-least-once durability), send via the sink;
//  2. retain-on-fail — a send error leaves the frame in the outbox (unacked) and
//     stops the drain; it is re-sent on the next reconnect;
//  3. receive — read inbound frames, route control frames (OutboxAck → advance
//     retention; ResyncRequest → answer with a provider snapshot) and feed data
//     frames through the ResyncCoordinator (Apply → hand to the host + owe an
//     ack; RequestSnapshot → emit a ResyncRequest; Ignore → drop);
//  4. resync-on-reconnect — OnReconnect replays the unacked outbox suffix from
//     the peer's ack cursor and re-advertises our own receiver cursor, so a
//     dropped-frame gap converges.
//
// The driver owns no goroutines, no clock source, and no storage engine — the
// host injects all three and decides the tick cadence.
type SyncDriver struct {
	sink        IpcSink
	source      IpcSource
	outbox      DurableOutbox
	clock       Clock
	provider    SnapshotProvider
	coordinator *ResyncCoordinator
	// pending holds host-enqueued outbound data frames staged before
	// append-then-send.
	pending []OutboxEntry
	// peerAckedThrough is the highest epoch the peer has acked — our outbox
	// retention + reconnect resume cursor.
	peerAckedThrough Epoch
	// ackOwed records that we applied an inbound frame and owe the peer an
	// OutboxAck (retried until sent).
	ackOwed bool
	// replayPending records that a reconnect happened; the next tick replays the
	// unacked outbox suffix.
	replayPending bool
	// stalledSince is non-nil (millis) since the last sink send failure; nil when
	// the sink is healthy.
	stalledSince *int64
}

// NewSyncDriver returns a fresh driver at receiver epoch 0 (a Snapshot seeds the
// first epoch).
func NewSyncDriver(sink IpcSink, source IpcSource, outbox DurableOutbox, clock Clock, provider SnapshotProvider) *SyncDriver {
	return NewSyncDriverWithEpoch(sink, source, outbox, clock, provider, 0)
}

// NewSyncDriverWithEpoch returns a driver whose receiver has already applied
// through lastEpoch (resume).
func NewSyncDriverWithEpoch(sink IpcSink, source IpcSource, outbox DurableOutbox, clock Clock, provider SnapshotProvider, lastEpoch Epoch) *SyncDriver {
	return &SyncDriver{
		sink:        sink,
		source:      source,
		outbox:      outbox,
		clock:       clock,
		provider:    provider,
		coordinator: NewResyncCoordinatorWithEpoch(lastEpoch),
	}
}

// Enqueue stages an outbound data frame at epoch for the next tick's drain.
// epoch is the frame's accepted-event count (Delta.Epoch / Snapshot.Epoch); it
// becomes the outbox retention key.
func (d *SyncDriver) Enqueue(epoch Epoch, msg IpcMessage) {
	d.pending = append(d.pending, OutboxEntry{Epoch: epoch, Msg: msg})
}

// OnReconnect signals that the transport was re-established; the next Tick
// replays the unacked outbox suffix and re-advertises our receiver cursor.
func (d *SyncDriver) OnReconnect() {
	d.replayPending = true
	d.ackOwed = true
	d.stalledSince = nil
}

// LastEpoch returns the receiver's current applied epoch.
func (d *SyncDriver) LastEpoch() Epoch { return d.coordinator.LastEpoch() }

// IsStalled reports whether the sink is currently stalled (last send failed,
// awaiting reconnect).
func (d *SyncDriver) IsStalled() bool { return d.stalledSince != nil }

// StalledFor returns the millis the sink has been stalled as of now, or 0 when
// healthy — a backoff signal for the host scheduler.
func (d *SyncDriver) StalledFor(now int64) int64 {
	if d.stalledSince == nil {
		return 0
	}
	if now < *d.stalledSince {
		return 0
	}
	return now - *d.stalledSince
}

// Outbox borrows the underlying outbox (diagnostics / durable-store flush).
func (d *SyncDriver) Outbox() DurableOutbox { return d.outbox }

func (d *SyncDriver) stall(now int64) {
	n := now
	d.stalledSince = &n
}

// Tick runs one loop pass. See the type docs for the drain → retain → receive →
// resync shape. Sink failures retain-and-stall (not an error); only an inbound
// source read failure returns a *DriverError.
func (d *SyncDriver) Tick() (Progress, error) {
	now := d.clock.NowMillis()
	var progress Progress

	// 1. resync-on-reconnect: replay the unacked outbox suffix, oldest first.
	if d.replayPending {
		d.replayPending = false
		for _, entry := range d.outbox.ReplayFrom(d.peerAckedThrough) {
			if d.sink.Send(entry.Msg) == nil {
				progress.Sent++
			} else {
				d.stall(now)
				d.replayPending = true // finish the replay after the next reconnect
				break
			}
		}
	}

	// 2. drain fresh enqueues: append-before-send, retain-and-stop on failure.
	//    A pre-existing stall (a prior failed send, no reconnect yet) skips the
	//    drain entirely — do not push into a sink already known to be down.
	for d.stalledSince == nil && len(d.pending) > 0 {
		entry := d.pending[0]
		d.outbox.Append(entry.Epoch, entry.Msg)
		d.pending = d.pending[1:]
		if err := d.sink.Send(entry.Msg); err == nil {
			progress.Sent++
			d.stalledSince = nil
		} else {
			// Retained in the outbox (unacked) → replayed on reconnect.
			d.stall(now)
			break
		}
	}

	// 3. receive: route control frames + feed data frames through the coordinator.
	for {
		msg, present, err := d.source.Recv()
		if err != nil {
			return Progress{}, &DriverError{Source: err}
		}
		if !present {
			break
		}
		switch m := msg.(type) {
		case IpcMessageOutboxAck:
			if m.Value.ThroughEpoch > d.peerAckedThrough {
				d.peerAckedThrough = m.Value.ThroughEpoch
			}
			d.outbox.AckThrough(m.Value.ThroughEpoch)
		case IpcMessageResyncRequest:
			snap := d.provider.Snapshot(m.Value.FromEpoch)
			if d.sink.Send(snap) == nil {
				progress.SnapshotsServed++
			} else {
				d.stall(now)
			}
		case IpcMessageCrdtSync:
			// Idempotent anti-entropy plane — the host folds it directly.
			progress.Applied = append(progress.Applied, msg)
		default:
			// Snapshot / Delta → coordinator.
			action, fromEpoch := d.coordinator.Ingest(msg)
			switch action {
			case ResyncActionApply:
				d.ackOwed = true
				progress.Applied = append(progress.Applied, msg)
			case ResyncActionRequestSnapshot:
				req := IpcMessageResyncRequest{Value: ResyncRequest{FromEpoch: fromEpoch}}
				if d.sink.Send(req) == nil {
					progress.ResyncRequested = true
				} else {
					d.stall(now)
				}
			case ResyncActionIgnore:
			}
		}
	}

	// 4. advertise our receiver cursor if we applied anything (retry until sent).
	if d.ackOwed && d.sink.Send(d.coordinator.Ack()) == nil {
		d.ackOwed = false
	}

	progress.PeerAckedThrough = d.peerAckedThrough
	progress.Retained = len(d.outbox.RetainedEpochs())
	return progress, nil
}
