package lazily

// Command / RPC message plane (command-plane-v1).
//
// Editor and runtime integrations issue commands — Run Agent Doc, sync, focus,
// save, session operations — and need one reusable admission, dedupe,
// cancellation, generation-guard, progress, and reconnect story instead of a
// per-caller ad hoc request/response contract. This is that plane: an evented
// command message family that is an **additive sibling** to Snapshot / Delta /
// CrdtSync; it does not add new state-plane variants.
//
// The plane is feature-gated. Peers advertise `command-plane-v1` in the
// Capability Negotiation `features` array. A peer that lacks `command-plane-v1`
// fails closed before accepting command traffic; a command that requires the
// plane is not silently downgraded.
//
// Four externally-tagged frames make up the family:
//   - CommandSubmit    — admit a command (envelope + domain payload).
//   - CommandCancel    — preempt a still-non-terminal command.
//   - CommandEvents    — batch progress/detail events (UX/diagnostics only).
//   - CommandProjection — the folded, queryable image; also the reconnect
//     resync frame.
//
// Terminal authority is the causal receipt, not the event or the transport: a
// command becomes terminal only when a terminal CausalReceipt for its
// command_id folds in. A network ACK, controller admission, or accepted/queued
// event never resolves a unary call.
//
// Go port of lazily-js `src/index.js` (CommandProjection / CommandRpcClient)
// and lazily-kt `Command.kt`, conformant with lazily-spec
// `schemas/message-passing.json` and the shared `conformance/message-passing/`
// fixtures.
//
// Wire conventions (NORMATIVE, from message-passing.json):
//   - snake_case field names throughout.
//   - CommandMessage is externally tagged: {"CommandSubmit": {...}} etc.
//   - CommandProjectionEntry always emits all seven fields; nullable ones
//     (reason, terminal_receipt_id, last_event_id) emit JSON null, so they
//     carry no `omitempty`.

import (
	"encoding/json"
	"fmt"
	"sort"
)

// ---------------------------------------------------------------------------
// DedupePolicy / CommandPolicy (message-passing.json#/$defs/DedupePolicy, CommandPolicy)
// ---------------------------------------------------------------------------

// DedupePolicy is how the admitter collapses concurrent/duplicate submits.
type DedupePolicy string

const (
	// DedupePolicyNone performs no dedupe.
	DedupePolicyNone DedupePolicy = "none"
	// DedupePolicySameIdempotencyKey collapses by idempotency_key.
	DedupePolicySameIdempotencyKey DedupePolicy = "same_idempotency_key"
	// DedupePolicySameCommandId collapses by command_id.
	DedupePolicySameCommandId DedupePolicy = "same_command_id"
)

func dedupePolicyFromWire(v string) (DedupePolicy, error) {
	switch DedupePolicy(v) {
	case DedupePolicyNone, DedupePolicySameIdempotencyKey, DedupePolicySameCommandId:
		return DedupePolicy(v), nil
	}
	return "", fmt.Errorf("unknown dedupe policy: %q", v)
}

// CommandPolicy is the per-submit admission policy.
type CommandPolicy struct {
	Dedupe          DedupePolicy `json:"dedupe"`
	Supersede       bool         `json:"supersede"`
	CancelOnPreempt bool         `json:"cancel_on_preempt"`
}

// ---------------------------------------------------------------------------
// CommandSubmit (message-passing.json#/$defs/CommandSubmit)
// ---------------------------------------------------------------------------

// CommandSubmit admits a command. Lazily owns the envelope (command_id,
// correlation, idempotency, generation, policy, payload framing); the namespace
// owns the payload body, which lazily never interprets.
type CommandSubmit struct {
	CommandId           string        `json:"command_id"`
	CausationId         string        `json:"causation_id"`
	Source              string        `json:"source"`
	Target              string        `json:"target"`
	Namespace           string        `json:"namespace"`
	Name                string        `json:"name"`
	AuthorityGeneration int64         `json:"authority_generation"`
	IdempotencyKey      string        `json:"idempotency_key"`
	DeadlineMs          int64         `json:"deadline_ms"`
	Policy              CommandPolicy `json:"policy"`
	PayloadType         string        `json:"payload_type"`
	PayloadHash         string        `json:"payload_hash"`
	Payload             IpcValue      `json:"payload"`
	RequiredFeatures    []string      `json:"required_features"`
}

// MarshalJSON renders the canonical wire object with required_features always
// an array (never null).
func (s CommandSubmit) MarshalJSON() ([]byte, error) {
	type raw struct {
		CommandId           string        `json:"command_id"`
		CausationId         string        `json:"causation_id"`
		Source              string        `json:"source"`
		Target              string        `json:"target"`
		Namespace           string        `json:"namespace"`
		Name                string        `json:"name"`
		AuthorityGeneration int64         `json:"authority_generation"`
		IdempotencyKey      string        `json:"idempotency_key"`
		DeadlineMs          int64         `json:"deadline_ms"`
		Policy              CommandPolicy `json:"policy"`
		PayloadType         string        `json:"payload_type"`
		PayloadHash         string        `json:"payload_hash"`
		Payload             IpcValue      `json:"payload"`
		RequiredFeatures    []string      `json:"required_features"`
	}
	return json.Marshal(raw{
		CommandId: s.CommandId, CausationId: s.CausationId, Source: s.Source,
		Target: s.Target, Namespace: s.Namespace, Name: s.Name,
		AuthorityGeneration: s.AuthorityGeneration, IdempotencyKey: s.IdempotencyKey,
		DeadlineMs: s.DeadlineMs, Policy: s.Policy, PayloadType: s.PayloadType,
		PayloadHash: s.PayloadHash, Payload: s.Payload,
		RequiredFeatures: nonNilSlice(s.RequiredFeatures),
	})
}

// UnmarshalJSON decodes a CommandSubmit, validating the dedupe policy and the
// payload's externally-tagged IpcValue form.
func (s *CommandSubmit) UnmarshalJSON(b []byte) error {
	type raw struct {
		CommandId           string          `json:"command_id"`
		CausationId         string          `json:"causation_id"`
		Source              string          `json:"source"`
		Target              string          `json:"target"`
		Namespace           string          `json:"namespace"`
		Name                string          `json:"name"`
		AuthorityGeneration int64           `json:"authority_generation"`
		IdempotencyKey      string          `json:"idempotency_key"`
		DeadlineMs          int64           `json:"deadline_ms"`
		Policy              CommandPolicy   `json:"policy"`
		PayloadType         string          `json:"payload_type"`
		PayloadHash         string          `json:"payload_hash"`
		Payload             json.RawMessage `json:"payload"`
		RequiredFeatures    []string        `json:"required_features"`
	}
	var r raw
	if err := json.Unmarshal(b, &r); err != nil {
		return err
	}
	if _, err := dedupePolicyFromWire(string(r.Policy.Dedupe)); err != nil {
		return err
	}
	payload, err := unmarshalIpcValue(r.Payload)
	if err != nil {
		return err
	}
	s.CommandId = r.CommandId
	s.CausationId = r.CausationId
	s.Source = r.Source
	s.Target = r.Target
	s.Namespace = r.Namespace
	s.Name = r.Name
	s.AuthorityGeneration = r.AuthorityGeneration
	s.IdempotencyKey = r.IdempotencyKey
	s.DeadlineMs = r.DeadlineMs
	s.Policy = r.Policy
	s.PayloadType = r.PayloadType
	s.PayloadHash = r.PayloadHash
	s.Payload = payload
	s.RequiredFeatures = nonNilSlice(r.RequiredFeatures)
	return nil
}

// ---------------------------------------------------------------------------
// CommandCancel (message-passing.json#/$defs/CommandCancel)
// ---------------------------------------------------------------------------

// CommandCancel preempts a still-non-terminal command by command_id at a given
// authority_generation, with an optional reason. A stale-generation cancel is
// ignored. A cancel after a terminal outcome never rewrites it.
type CommandCancel struct {
	CommandId           string  `json:"command_id"`
	CausationId         string  `json:"causation_id"`
	Source              string  `json:"source"`
	AuthorityGeneration int64   `json:"authority_generation"`
	Reason              *string `json:"reason"`
}

// ---------------------------------------------------------------------------
// CommandEvent / CommandEvents (message-passing.json#/$defs/CommandEvent, CommandEventsFrame)
// ---------------------------------------------------------------------------

// CommandEventKind is a progress/detail event kind. These are UX/diagnostics
// only and are NEVER terminal proof; terminal proof folds through
// CausalReceipt. cancelled/superseded/timed_out are surfaced here for UX but
// their terminal authority is a matching rejected receipt.
type CommandEventKind string

const (
	CommandEventKindObserved   CommandEventKind = "observed"
	CommandEventKindAccepted   CommandEventKind = "accepted"
	CommandEventKindStarted    CommandEventKind = "started"
	CommandEventKindProgress   CommandEventKind = "progress"
	CommandEventKindCancelled  CommandEventKind = "cancelled"
	CommandEventKindSuperseded CommandEventKind = "superseded"
	CommandEventKindTimedOut   CommandEventKind = "timed_out"
)

func commandEventKindFromWire(v string) (CommandEventKind, error) {
	switch CommandEventKind(v) {
	case CommandEventKindObserved, CommandEventKindAccepted, CommandEventKindStarted,
		CommandEventKindProgress, CommandEventKindCancelled, CommandEventKindSuperseded,
		CommandEventKindTimedOut:
		return CommandEventKind(v), nil
	}
	return "", fmt.Errorf("unknown command event kind: %q", v)
}

// CommandEvent is one progress/detail event keyed by command_id.
type CommandEvent struct {
	EventId    string           `json:"event_id"`
	CommandId  string           `json:"command_id"`
	Kind       CommandEventKind `json:"kind"`
	Generation int64            `json:"generation"`
	Detail     *string          `json:"detail"`
}

// UnmarshalJSON decodes a CommandEvent, validating the kind enum.
func (e *CommandEvent) UnmarshalJSON(b []byte) error {
	type raw CommandEvent
	var r raw
	if err := json.Unmarshal(b, &r); err != nil {
		return err
	}
	if _, err := commandEventKindFromWire(string(r.Kind)); err != nil {
		return err
	}
	*e = CommandEvent(r)
	return nil
}

// CommandEvents is a batch of progress/detail events.
type CommandEvents struct {
	Events []CommandEvent `json:"events"`
}

// MarshalJSON emits { events } with events always an array (never null).
func (c CommandEvents) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Events []CommandEvent `json:"events"`
	}{nonNilSlice(c.Events)})
}

// ---------------------------------------------------------------------------
// CommandStatus / CommandProjectionEntry / CommandProjectionImage
// (message-passing.json#/$defs/CommandStatus, CommandProjectionEntry, CommandProjectionFrame)
// ---------------------------------------------------------------------------

// CommandStatus is the folded projection status. Submitted/Accepted/Running are
// non-terminal; Applied/Rejected/Cancelled/Superseded/TimedOut are terminal and
// backed by a terminal CausalReceipt.
type CommandStatus string

const (
	CommandStatusSubmitted  CommandStatus = "submitted"
	CommandStatusAccepted   CommandStatus = "accepted"
	CommandStatusRunning    CommandStatus = "running"
	CommandStatusApplied    CommandStatus = "applied"
	CommandStatusRejected   CommandStatus = "rejected"
	CommandStatusCancelled  CommandStatus = "cancelled"
	CommandStatusSuperseded CommandStatus = "superseded"
	CommandStatusTimedOut   CommandStatus = "timed_out"
)

func commandStatusFromWire(v string) (CommandStatus, error) {
	switch CommandStatus(v) {
	case CommandStatusSubmitted, CommandStatusAccepted, CommandStatusRunning,
		CommandStatusApplied, CommandStatusRejected, CommandStatusCancelled,
		CommandStatusSuperseded, CommandStatusTimedOut:
		return CommandStatus(v), nil
	}
	return "", fmt.Errorf("unknown command status: %q", v)
}

// isTerminalCommandStatus reports whether status is terminal.
func isTerminalCommandStatus(status CommandStatus) bool {
	switch status {
	case CommandStatusApplied, CommandStatusRejected, CommandStatusCancelled,
		CommandStatusSuperseded, CommandStatusTimedOut:
		return true
	}
	return false
}

// phaseRank is the monotonic forward-progress rank for non-terminal status.
// An event updates status only when the next rank is >= the current.
func phaseRank(status CommandStatus) int {
	switch status {
	case CommandStatusSubmitted:
		return 0
	case CommandStatusAccepted:
		return 1
	case CommandStatusRunning:
		return 2
	}
	return 3
}

// CommandProjectionEntry is the folded, queryable image of one command's state.
// Reason, TerminalReceiptId, and LastEventId are nullable wire fields: they
// marshal to JSON null when nil (the schema lists them as required), so they
// carry no `omitempty`.
type CommandProjectionEntry struct {
	CommandId         string        `json:"command_id"`
	Status            CommandStatus `json:"status"`
	Terminal          bool          `json:"terminal"`
	Generation        int64         `json:"generation"`
	Reason            *string       `json:"reason"`
	TerminalReceiptId *string       `json:"terminal_receipt_id"`
	LastEventId       *string       `json:"last_event_id"`
}

// UnmarshalJSON decodes an entry, validating the status enum.
func (e *CommandProjectionEntry) UnmarshalJSON(b []byte) error {
	type raw CommandProjectionEntry
	var r raw
	if err := json.Unmarshal(b, &r); err != nil {
		return err
	}
	if _, err := commandStatusFromWire(string(r.Status)); err != nil {
		return err
	}
	*e = CommandProjectionEntry(r)
	return nil
}

// with returns a copy with the patch fields applied (nil fields keep the old
// value).
func (e CommandProjectionEntry) with(patch CommandProjectionEntry) CommandProjectionEntry {
	out := e
	if patch.Status != "" {
		out.Status = patch.Status
	}
	if patch.Terminal || patch.Status != "" {
		out.Terminal = patch.Terminal
	}
	if patch.Reason != nil {
		out.Reason = patch.Reason
	}
	if patch.TerminalReceiptId != nil {
		out.TerminalReceiptId = patch.TerminalReceiptId
	}
	if patch.LastEventId != nil {
		out.LastEventId = patch.LastEventId
	}
	return out
}

// CommandProjectionImage is the resync snapshot: an authority generation plus
// the per-command folded entries.
type CommandProjectionImage struct {
	Generation int64                    `json:"generation"`
	Commands   []CommandProjectionEntry `json:"commands"`
}

// MarshalJSON emits commands always as an array (never null).
func (i CommandProjectionImage) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Generation int64                    `json:"generation"`
		Commands   []CommandProjectionEntry `json:"commands"`
	}{i.Generation, nonNilSlice(i.Commands)})
}

// ---------------------------------------------------------------------------
// CommandMessage (externally-tagged frame)
// ---------------------------------------------------------------------------

// CommandMessageTag identifies the frame variant.
type CommandMessageTag string

const (
	CommandMessageTagSubmit     CommandMessageTag = "CommandSubmit"
	CommandMessageTagCancel     CommandMessageTag = "CommandCancel"
	CommandMessageTagEvents     CommandMessageTag = "CommandEvents"
	CommandMessageTagProjection CommandMessageTag = "CommandProjection"
)

// CommandMessage is one externally-tagged frame of the command plane.
type CommandMessage struct {
	Tag        CommandMessageTag
	Submit     *CommandSubmit
	Cancel     *CommandCancel
	Events     *CommandEvents
	Projection *CommandProjectionImage
}

// NewCommandMessageSubmit wraps a CommandSubmit frame.
func NewCommandMessageSubmit(s CommandSubmit) CommandMessage {
	return CommandMessage{Tag: CommandMessageTagSubmit, Submit: &s}
}

// NewCommandMessageCancel wraps a CommandCancel frame.
func NewCommandMessageCancel(c CommandCancel) CommandMessage {
	return CommandMessage{Tag: CommandMessageTagCancel, Cancel: &c}
}

// NewCommandMessageEvents wraps a CommandEvents frame.
func NewCommandMessageEvents(e CommandEvents) CommandMessage {
	return CommandMessage{Tag: CommandMessageTagEvents, Events: &e}
}

// NewCommandMessageProjection wraps a CommandProjection frame.
func NewCommandMessageProjection(p CommandProjectionImage) CommandMessage {
	return CommandMessage{Tag: CommandMessageTagProjection, Projection: &p}
}

// MarshalJSON renders the externally-tagged wire form {"<Tag>": body}.
func (m CommandMessage) MarshalJSON() ([]byte, error) {
	switch m.Tag {
	case CommandMessageTagSubmit:
		if m.Submit == nil {
			return nil, fmt.Errorf("CommandMessage Submit is nil")
		}
		return taggedJSON(string(m.Tag), m.Submit)
	case CommandMessageTagCancel:
		if m.Cancel == nil {
			return nil, fmt.Errorf("CommandMessage Cancel is nil")
		}
		return taggedJSON(string(m.Tag), m.Cancel)
	case CommandMessageTagEvents:
		if m.Events == nil {
			return nil, fmt.Errorf("CommandMessage Events is nil")
		}
		return taggedJSON(string(m.Tag), m.Events)
	case CommandMessageTagProjection:
		if m.Projection == nil {
			return nil, fmt.Errorf("CommandMessage Projection is nil")
		}
		return taggedJSON(string(m.Tag), m.Projection)
	}
	return nil, fmt.Errorf("unknown CommandMessage tag: %q", m.Tag)
}

// UnmarshalJSON decodes an externally-tagged CommandMessage.
func (m *CommandMessage) UnmarshalJSON(b []byte) error {
	tag, body, err := splitTagged(b, "CommandMessage")
	if err != nil {
		return err
	}
	switch CommandMessageTag(tag) {
	case CommandMessageTagSubmit:
		var s CommandSubmit
		if err := json.Unmarshal(body, &s); err != nil {
			return err
		}
		m.Tag = CommandMessageTagSubmit
		m.Submit = &s
	case CommandMessageTagCancel:
		var c CommandCancel
		if err := json.Unmarshal(body, &c); err != nil {
			return err
		}
		m.Tag = CommandMessageTagCancel
		m.Cancel = &c
	case CommandMessageTagEvents:
		var e CommandEvents
		if err := json.Unmarshal(body, &e); err != nil {
			return err
		}
		m.Tag = CommandMessageTagEvents
		m.Events = &e
	case CommandMessageTagProjection:
		var p CommandProjectionImage
		if err := json.Unmarshal(body, &p); err != nil {
			return err
		}
		m.Tag = CommandMessageTagProjection
		m.Projection = &p
	default:
		return fmt.Errorf("unknown CommandMessage variant: %q", tag)
	}
	return nil
}

// CommandMessageFromWire decodes a CommandMessage from JSON bytes.
func CommandMessageFromWire(data []byte) (CommandMessage, error) {
	var m CommandMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return CommandMessage{}, err
	}
	return m, nil
}

// ---------------------------------------------------------------------------
// CommandApplyStatus (sealed result hierarchy)
// ---------------------------------------------------------------------------

// CommandApplyStatus is the result of folding a frame into a CommandProjection.
type CommandApplyStatus interface {
	isCommandApplyStatus()
}

// CommandApplyStatusKind enumerates the result variants.
type CommandApplyStatusKind string

const (
	CommandApplyStatusRecorded         CommandApplyStatusKind = "recorded"
	CommandApplyStatusDuplicate        CommandApplyStatusKind = "duplicate"
	CommandApplyStatusUnknown          CommandApplyStatusKind = "unknown"
	CommandApplyStatusStaleGeneration  CommandApplyStatusKind = "stale_generation"
	CommandApplyStatusTerminalConflict CommandApplyStatusKind = "terminal_conflict"
)

// CommandStatusRecorded means the frame updated the projection.
type CommandStatusRecorded struct{}

func (CommandStatusRecorded) isCommandApplyStatus() {}

// Kind returns the result variant tag.
func (CommandStatusRecorded) Kind() CommandApplyStatusKind { return CommandApplyStatusRecorded }

// CommandStatusDuplicate means the frame was an idempotent no-op (duplicate
// command_id / event_id / receipt_id / cancel causation_id).
type CommandStatusDuplicate struct{}

func (CommandStatusDuplicate) isCommandApplyStatus() {}

// Kind returns the result variant tag.
func (CommandStatusDuplicate) Kind() CommandApplyStatusKind { return CommandApplyStatusDuplicate }

// CommandStatusUnknown means the command_id was not in the projection.
type CommandStatusUnknown struct{}

func (CommandStatusUnknown) isCommandApplyStatus() {}

// Kind returns the result variant tag.
func (CommandStatusUnknown) Kind() CommandApplyStatusKind { return CommandApplyStatusUnknown }

// CommandStatusStaleGeneration means the frame's generation did not match the
// command's current authority generation; the frame was ignored.
type CommandStatusStaleGeneration struct {
	Expected int64
	Actual   int64
}

func (CommandStatusStaleGeneration) isCommandApplyStatus() {}

// Kind returns the result variant tag.
func (CommandStatusStaleGeneration) Kind() CommandApplyStatusKind {
	return CommandApplyStatusStaleGeneration
}

// CommandStatusTerminalConflict means a different terminal outcome already
// exists for this command_id (fail-closed).
type CommandStatusTerminalConflict struct {
	CommandId string
	Existing  CommandStatus
	Incoming  CommandStatus
}

func (CommandStatusTerminalConflict) isCommandApplyStatus() {}

// Kind returns the result variant tag.
func (CommandStatusTerminalConflict) Kind() CommandApplyStatusKind {
	return CommandApplyStatusTerminalConflict
}

// ---------------------------------------------------------------------------
// Projection fold helpers
// ---------------------------------------------------------------------------

// terminalStatusOf maps a terminal receipt outcome (+ reason) to the folded
// command status. A rejected receipt whose reason is "cancelled"/"superseded"/
// "timed_out" folds to the matching terminal status.
func terminalStatusOf(outcome ReceiptOutcome, reason *string) CommandStatus {
	if outcome == ReceiptOutcomeApplied {
		return CommandStatusApplied
	}
	if outcome == ReceiptOutcomeRejected {
		if reason != nil {
			switch *reason {
			case "cancelled":
				return CommandStatusCancelled
			case "superseded":
				return CommandStatusSuperseded
			case "timed_out":
				return CommandStatusTimedOut
			}
		}
		return CommandStatusRejected
	}
	return CommandStatusAccepted
}

// progressStatusOf maps an event kind to the non-terminal status it advances
// to, or "" when the event carries no status change (cancelled/superseded/
// timed_out are event-only signals; the receipt carries terminal authority).
func progressStatusOf(kind CommandEventKind) CommandStatus {
	switch kind {
	case CommandEventKindObserved, CommandEventKindAccepted:
		return CommandStatusAccepted
	case CommandEventKindStarted, CommandEventKindProgress:
		return CommandStatusRunning
	}
	return ""
}

// ---------------------------------------------------------------------------
// CommandProjection (the reducer)
// ---------------------------------------------------------------------------

// CommandProjection is the folded, queryable image of known command state. It
// is the reducer over CommandMessage frames and CausalReceipt events.
//
// Projection rules (lazily-spec § Command / RPC Message Plane):
//   - Terminal authority is the causal receipt, not the event or the transport.
//   - Generation guards: events/receipts outside the command's current
//     authority generation are ignored and retained only as audit data.
//   - Idempotency: a replayed submit/event/receipt (same id) is a no-op.
//   - Cancel before terminal only: a cancel terminally rejects a non-terminal
//     command; a cancel after applied is ignored.
//   - Terminal conflict fails closed: two terminal receipts at the same
//     generation with different outcomes is a conflict; consumers fail closed
//     rather than pick a winner.
//   - Reconnect equivalence: folding a CommandProjection image is equivalent to
//     folding the events and receipts it summarizes.
//
// Not safe for concurrent use.
type CommandProjection struct {
	generation     int64
	entries        map[string]CommandProjectionEntry
	seenEventIds   map[string]struct{}
	seenReceiptIds map[string]struct{}
	seenCancelIds  map[string]struct{}
	conflicts      map[string]struct{}
}

// NewCommandProjection creates an empty projection.
func NewCommandProjection() *CommandProjection {
	return &CommandProjection{
		entries:        map[string]CommandProjectionEntry{},
		seenEventIds:   map[string]struct{}{},
		seenReceiptIds: map[string]struct{}{},
		seenCancelIds:  map[string]struct{}{},
		conflicts:      map[string]struct{}{},
	}
}

// Generation is the highest authority generation observed so far.
func (p *CommandProjection) Generation() int64 { return p.generation }

// ApplyMessage dispatches a CommandMessage frame to the matching fold method.
func (p *CommandProjection) ApplyMessage(message CommandMessage) CommandApplyStatus {
	switch message.Tag {
	case CommandMessageTagSubmit:
		if message.Submit == nil {
			return CommandStatusUnknown{}
		}
		return p.Submit(*message.Submit)
	case CommandMessageTagCancel:
		if message.Cancel == nil {
			return CommandStatusUnknown{}
		}
		return p.Cancel(*message.Cancel)
	case CommandMessageTagEvents:
		if message.Events == nil {
			return CommandStatusUnknown{}
		}
		var last CommandApplyStatus = CommandStatusUnknown{}
		for _, e := range message.Events.Events {
			last = p.Event(e)
		}
		return last
	case CommandMessageTagProjection:
		if message.Projection == nil {
			return CommandStatusUnknown{}
		}
		return p.ApplyProjection(*message.Projection)
	}
	return CommandStatusUnknown{}
}

// Submit admits a command. A duplicate command_id is an idempotent no-op.
func (p *CommandProjection) Submit(s CommandSubmit) CommandApplyStatus {
	if _, ok := p.entries[s.CommandId]; ok {
		return CommandStatusDuplicate{}
	}
	if s.AuthorityGeneration > p.generation {
		p.generation = s.AuthorityGeneration
	}
	p.entries[s.CommandId] = CommandProjectionEntry{
		CommandId:  s.CommandId,
		Status:     CommandStatusSubmitted,
		Terminal:   false,
		Generation: s.AuthorityGeneration,
	}
	return CommandStatusRecorded{}
}

// Event folds one progress/detail event. Stale-generation and duplicate
// event_ids are no-ops. Status advances monotonically (never backward, never on
// a terminal command).
func (p *CommandProjection) Event(e CommandEvent) CommandApplyStatus {
	if _, dup := p.seenEventIds[e.EventId]; dup {
		return CommandStatusDuplicate{}
	}
	entry, ok := p.entries[e.CommandId]
	if !ok {
		return CommandStatusUnknown{}
	}
	if e.Generation != entry.Generation {
		return CommandStatusStaleGeneration{Expected: entry.Generation, Actual: e.Generation}
	}
	p.seenEventIds[e.EventId] = struct{}{}
	lastId := e.EventId
	entry.LastEventId = &lastId
	if next := progressStatusOf(e.Kind); next != "" && !entry.Terminal && phaseRank(next) >= phaseRank(entry.Status) {
		entry.Status = next
	}
	p.entries[e.CommandId] = entry
	return CommandStatusRecorded{}
}

// Cancel records a cancel request. A cancel is non-terminal by itself; the
// rejected receipt makes it terminal. Stale-generation and duplicate cancel
// causation_ids are no-ops.
func (p *CommandProjection) Cancel(c CommandCancel) CommandApplyStatus {
	if _, dup := p.seenCancelIds[c.CausationId]; dup {
		return CommandStatusDuplicate{}
	}
	entry, ok := p.entries[c.CommandId]
	if !ok {
		return CommandStatusUnknown{}
	}
	if c.AuthorityGeneration != entry.Generation {
		return CommandStatusStaleGeneration{Expected: entry.Generation, Actual: c.AuthorityGeneration}
	}
	p.seenCancelIds[c.CausationId] = struct{}{}
	return CommandStatusRecorded{}
}

// ObserveReceipt folds a causal receipt. This is the sole terminal authority:
// a terminal receipt (applied/rejected) flips the command to terminal. A
// differing terminal outcome at the same generation is a conflict (fail-closed).
func (p *CommandProjection) ObserveReceipt(r CausalReceipt) CommandApplyStatus {
	if _, dup := p.seenReceiptIds[r.ReceiptId]; dup {
		return CommandStatusDuplicate{}
	}
	entry, ok := p.entries[r.CausationId]
	if !ok {
		return CommandStatusUnknown{}
	}
	if r.Generation != entry.Generation {
		return CommandStatusStaleGeneration{Expected: entry.Generation, Actual: r.Generation}
	}
	if !r.IsTerminal() {
		p.seenReceiptIds[r.ReceiptId] = struct{}{}
		if !entry.Terminal && phaseRank(CommandStatusAccepted) >= phaseRank(entry.Status) {
			entry.Status = CommandStatusAccepted
			p.entries[r.CausationId] = entry
		}
		return CommandStatusRecorded{}
	}
	incoming := terminalStatusOf(r.Outcome, r.Reason)
	if entry.Terminal {
		if entry.Status == incoming {
			p.seenReceiptIds[r.ReceiptId] = struct{}{}
			return CommandStatusRecorded{}
		}
		p.conflicts[r.CausationId] = struct{}{}
		return CommandStatusTerminalConflict{
			CommandId: r.CausationId,
			Existing:  entry.Status,
			Incoming:  incoming,
		}
	}
	p.seenReceiptIds[r.ReceiptId] = struct{}{}
	entry.Terminal = true
	entry.Status = incoming
	entry.Reason = r.Reason
	rid := r.ReceiptId
	entry.TerminalReceiptId = &rid
	p.entries[r.CausationId] = entry
	return CommandStatusRecorded{}
}

// ApplyProjection folds a reconnect resync image. Equivalent to folding the
// events and receipts it summarizes.
func (p *CommandProjection) ApplyProjection(img CommandProjectionImage) CommandApplyStatus {
	if img.Generation > p.generation {
		p.generation = img.Generation
	}
	for _, entry := range img.Commands {
		p.entries[entry.CommandId] = entry
		if entry.LastEventId != nil {
			p.seenEventIds[*entry.LastEventId] = struct{}{}
		}
		if entry.TerminalReceiptId != nil {
			p.seenReceiptIds[*entry.TerminalReceiptId] = struct{}{}
		}
	}
	return CommandStatusRecorded{}
}

// Entry returns the folded entry for commandId, or false if unknown.
func (p *CommandProjection) Entry(commandId string) (CommandProjectionEntry, bool) {
	e, ok := p.entries[commandId]
	return e, ok
}

// TerminalFor returns the terminal entry for commandId, or false if the command
// is unknown or not yet terminal.
func (p *CommandProjection) TerminalFor(commandId string) (CommandProjectionEntry, bool) {
	e, ok := p.entries[commandId]
	if !ok || !e.Terminal {
		return CommandProjectionEntry{}, false
	}
	return e, true
}

// HasConflict reports whether commandId has a terminal conflict.
func (p *CommandProjection) HasConflict(commandId string) bool {
	_, ok := p.conflicts[commandId]
	return ok
}

// ToImage returns a snapshot of the projection sorted by command_id.
func (p *CommandProjection) ToImage() CommandProjectionImage {
	commands := make([]CommandProjectionEntry, 0, len(p.entries))
	for _, e := range p.entries {
		commands = append(commands, e)
	}
	sort.Slice(commands, func(i, j int) bool {
		return commands[i].CommandId < commands[j].CommandId
	})
	return CommandProjectionImage{Generation: p.generation, Commands: commands}
}

// ---------------------------------------------------------------------------
// RPC facade (CommandRpcClient)
// ---------------------------------------------------------------------------

// CommandTransport is the outbound sink for command frames.
type CommandTransport interface {
	Send(message CommandMessage)
}

// CommandTransportFunc adapts a function into a CommandTransport.
type CommandTransportFunc func(message CommandMessage)

// Send satisfies CommandTransport.
func (f CommandTransportFunc) Send(m CommandMessage) { f(m) }

// CallStateKind enumerates the unary-RPC resolution states.
type CallStateKind string

const (
	CallStateKindPending  CallStateKind = "pending"
	CallStateKindResolved CallStateKind = "resolved"
	CallStateKindConflict CallStateKind = "conflict"
)

// CallState is the unary-RPC resolution state. A call resolves only when the
// command projection reaches a terminal causal receipt.
type CallState struct {
	Kind  CallStateKind
	Entry CommandProjectionEntry // populated when Kind == Resolved
}

// CommandRpcClient is the RPC facade over the command plane. It builds and
// sends CommandSubmit/CommandCancel frames, folds replies into its projection,
// and exposes a polled unary-call resolution that completes only on a terminal
// causal receipt.
type CommandRpcClient struct {
	transport  CommandTransport
	Projection *CommandProjection
}

// NewCommandRpcClient constructs an RPC client over the given transport.
func NewCommandRpcClient(transport CommandTransport) *CommandRpcClient {
	return &CommandRpcClient{transport: transport, Projection: NewCommandProjection()}
}

// Submit builds and sends a CommandSubmit, folds it into the projection, and
// returns the command id.
func (c *CommandRpcClient) Submit(s CommandSubmit) string {
	msg := NewCommandMessageSubmit(s)
	c.transport.Send(msg)
	c.Projection.ApplyMessage(msg)
	return s.CommandId
}

// Cancel builds and sends a CommandCancel and folds it into the projection.
func (c *CommandRpcClient) Cancel(cancel CommandCancel) {
	msg := NewCommandMessageCancel(cancel)
	c.transport.Send(msg)
	c.Projection.ApplyMessage(msg)
}

// IngestCommand folds an inbound CommandMessage into the projection.
func (c *CommandRpcClient) IngestCommand(message CommandMessage) CommandApplyStatus {
	return c.Projection.ApplyMessage(message)
}

// IngestReceipt folds an inbound causal receipt into the projection.
func (c *CommandRpcClient) IngestReceipt(receipt CausalReceipt) CommandApplyStatus {
	return c.Projection.ObserveReceipt(receipt)
}

// PollCall returns the current resolution state of a unary call. Resolves only
// when the command projection reaches a terminal causal receipt — a transport
// ACK, controller admission, or accepted/queued event never resolves it.
func (c *CommandRpcClient) PollCall(commandId string) CallState {
	if c.Projection.HasConflict(commandId) {
		return CallState{Kind: CallStateKindConflict}
	}
	if entry, ok := c.Projection.TerminalFor(commandId); ok {
		return CallState{Kind: CallStateKindResolved, Entry: entry}
	}
	return CallState{Kind: CallStateKindPending}
}
