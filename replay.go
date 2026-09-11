package lazily

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

// Replay-equivalence proof for a reactive graph (`#lzreplaygo`).
//
// lazily-spec/docs/replay-safety.md draws the line: the pure cores are
// replay-safe, the reactive layer's command ORDERING is not, and cell VALUES are
// replay-stable either way. This file is the other half of that statement — it
// makes the stable half PROVABLE rather than asserted:
//
//	Given the same ReplayLog, a rebuilt graph observes the same values at every
//	checkpoint. Any deviation is a defect in the graph, not a tolerance.
//
// Three obligations follow (lazily-spec/docs/replay-equivalence.md), and the
// canonical corpus under conformance/replay checks each one:
//
//  1. THE FINGERPRINT IS BOUND TO ITS LOG. The discipline is taken from `tsift`,
//     whose cached excerpts are trustworthy because each records a body hash and
//     revalidates it against the source bytes before the excerpt is returned: a
//     stale body deterministically suppresses the cached answer rather than
//     returning a plausible-looking one. Here the event log is the source bytes.
//     ReplayHarness.Verify compares the recorded log digest BEFORE it compares a
//     single value, and refuses a fingerprint recorded against another log with
//     *ReplayLogMismatchError. Both halves matter: [+1,+2,+3] and [+3,+2,+1]
//     settle to the same sum, so a value-only comparison would pass and certify
//     nothing about the log in front of it — and a harness that compared anyway
//     would blame the graph for a stale test artifact.
//
//  2. DIVERGENCE IS LOCALIZED. Every event is checkpointed by default, and the
//     report names the FIRST diverging checkpoint's seq and the label of the cell
//     that differed. A coarser stride is allowed for long logs and is part of the
//     fingerprint: equal log digest plus equal stride is what makes two
//     checkpoint sequences comparable at all, so a stride difference is its own
//     fault (*ReplayStrideMismatchError), not a divergence.
//
//  3. THE OBSERVATION ENCODING IS CANONICAL, OR IT FAILS. See
//     ReplayCanonicalBytes. A value the encoding does not define returns
//     *ReplayEncodingError rather than falling back on fmt's default rendering,
//     which embeds a pointer address and would report a FALSE divergence on every
//     run — the exact failure a replay proof exists to make impossible, arriving
//     as a flaky test instead of a real one.
//
// HASHING. SHA-256 from the standard library, chosen over BLAKE2b/BLAKE3 because
// lazily-go has no module dependencies and neither is in the standard library.
// The property relied on is collision resistance over canonical bytes, which
// SHA-256 has. Per the spec a binding MAY pick its own hash and byte layout:
// fingerprints are pinned next to a test in one language and are never exchanged
// between bindings, so these digests are deliberately not wire-compatible with
// lazily-py's.
//
// Example:
//
//	log, err := NewReplayLog(
//		ReplayEvent{Seq: 0, Name: "add", Payload: int64(1)},
//		ReplayEvent{Seq: 1, Name: "add", Payload: int64(2)},
//	)
//	harness, err := NewReplayHarness(func() ReplayGraph { return newCounter() })
//	fingerprint, err := harness.Record(log) // pin it, or commit fingerprint.Wire()
//	_, err = harness.Verify(log, fingerprint)

// ReplayInitialSeq is the checkpoint sequence number for the state before any
// event was applied.
const ReplayInitialSeq int64 = -1

const (
	replayWireSchemaVersion = 1
	replayPreviewLimit      = 120
)

// ---------------------------------------------------------------------------
// errors
// ---------------------------------------------------------------------------

// ErrReplayProof is the sentinel every replay-proof failure unwraps to, so a
// caller can route on the family with errors.Is and on the exact fault with
// errors.As.
var ErrReplayProof = errors.New("replay-equivalence proof could not be completed as stated")

// ReplayEncodingError reports a value with no canonical byte encoding.
//
// Returned instead of falling back on fmt's %v, which renders a pointer or a
// channel as an ADDRESS: such a fallback reports a false divergence on every
// replay, which is the failure mode this file exists to make impossible.
type ReplayEncodingError struct {
	// Path locates the offending value inside the observed structure.
	Path string
	// TypeName is the Go type that has no defined encoding.
	TypeName string
}

func (e *ReplayEncodingError) Error() string {
	return fmt.Sprintf("%s: %s has no canonical encoding; observe a plain value, a struct, "+
		"or a map/slice/ReplaySet of them instead", e.Path, e.TypeName)
}

func (e *ReplayEncodingError) Unwrap() error { return ErrReplayProof }

// ReplayLogMismatchError reports a fingerprint recorded against a different
// event log.
//
// The tsift rule: revalidate the recorded hash against the source bytes and
// deterministically suppress the cached answer when they disagree. A stale
// fingerprint is never compared, so it can neither pass by coincidence nor be
// misreported as a value divergence.
type ReplayLogMismatchError struct {
	ExpectedDigest string
	ActualDigest   string
}

func (e *ReplayLogMismatchError) Error() string {
	return fmt.Sprintf("fingerprint was recorded against a different event log "+
		"(fingerprint log digest=%s, replayed log digest=%s); re-record the fingerprint against this log",
		e.ExpectedDigest, e.ActualDigest)
}

func (e *ReplayLogMismatchError) Unwrap() error { return ErrReplayProof }

// ReplayStrideMismatchError reports a fingerprint recorded at a different
// checkpoint stride.
//
// A distinct type from ReplayLogMismatchError because it is a distinct fault:
// the log is the right one, but the two checkpoint sequences were never
// comparable. Keeping the types apart is what lets a driver route on the fault
// rather than on a message string.
type ReplayStrideMismatchError struct {
	ExpectedStride int
	ActualStride   int
}

func (e *ReplayStrideMismatchError) Error() string {
	return fmt.Sprintf("fingerprint was recorded at stride %d but this harness samples at stride %d; re-record it",
		e.ExpectedStride, e.ActualStride)
}

func (e *ReplayStrideMismatchError) Unwrap() error { return ErrReplayProof }

// ReplayDivergenceError reports that a replayed graph observed a different value
// than the fingerprint.
type ReplayDivergenceError struct {
	// Divergences is ordered, and its first element is the earliest — the one
	// worth reading. Later checkpoints are almost always the same defect carried
	// forward, so they are not collected.
	Divergences []ReplayDivergence
}

// First is the earliest divergence.
func (e *ReplayDivergenceError) First() ReplayDivergence { return e.Divergences[0] }

func (e *ReplayDivergenceError) Error() string {
	if len(e.Divergences) == 0 {
		return "replay diverged from the fingerprint"
	}
	tail := ""
	if extra := len(e.Divergences) - 1; extra > 0 {
		tail = fmt.Sprintf(" (+%d more)", extra)
	}
	return fmt.Sprintf("replay diverged from the fingerprint: %s%s", e.Divergences[0], tail)
}

func (e *ReplayDivergenceError) Unwrap() error { return ErrReplayProof }

// replayErrorf builds a plain proof error that still unwraps to ErrReplayProof.
func replayErrorf(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrReplayProof}, args...)...)
}

// ---------------------------------------------------------------------------
// canonical encoding
// ---------------------------------------------------------------------------

// ReplaySet is an unordered collection of observed values.
//
// Go has no set type, and a set's equality class differs from a slice's: member
// ORDER is not part of the value. Observing a ReplaySet is how a graph says so —
// a plain []any is encoded as a SEQUENCE, where order IS part of the value.
type ReplaySet []any

// ReplayCanonicalBytes encodes value to type-tagged, length-framed, order-stable
// bytes.
//
// Every frame is `<tag><len>:<body>`, so no concatenation of members can be
// confused for another: ["a","bc"] and ["ab","c"] encode differently, which is
// the row of the contract that is easiest to get wrong. The tag is what keeps
// 1, "1", 1.0, true and the byte string 0x31 five different values.
//
// Map entries and ReplaySet members are ordered by their own encoded bytes, so
// neither Go's randomized map iteration nor the order a set was built in changes
// the result.
//
// Tags:
//
//	n  nil               b  bool          i  integer (signed or unsigned)
//	f  float             s  string        y  byte string
//	l  sequence          m  mapping       t  set            d  struct
//
// Anything else — a channel, a func, a complex number, an unsafe pointer —
// returns *ReplayEncodingError rather than degrading to a default rendering.
func ReplayCanonicalBytes(value any) ([]byte, error) {
	return appendReplayCanonical(make([]byte, 0, 64), reflect.ValueOf(value), "value")
}

// ReplayCanonicalDigest is the SHA-256 hex digest of ReplayCanonicalBytes.
func ReplayCanonicalDigest(value any) (string, error) {
	encoded, err := ReplayCanonicalBytes(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// appendReplayFrame writes one `<tag><len>:<body>` frame.
func appendReplayFrame(dst []byte, tag byte, body []byte) []byte {
	dst = append(dst, tag)
	dst = strconv.AppendInt(dst, int64(len(body)), 10)
	dst = append(dst, ':')
	return append(dst, body...)
}

var replaySetType = reflect.TypeOf(ReplaySet(nil))

func appendReplayCanonical(dst []byte, v reflect.Value, path string) ([]byte, error) {
	// An untyped nil, and a nil inside an interface, are the same value.
	if !v.IsValid() {
		return append(dst, 'n', '0', ':'), nil
	}
	switch v.Kind() {
	case reflect.Interface, reflect.Pointer:
		if v.IsNil() {
			return append(dst, 'n', '0', ':'), nil
		}
		return appendReplayCanonical(dst, v.Elem(), path)
	}

	// A ReplaySet is a slice by Kind, so it is resolved by TYPE first.
	if v.Type() == replaySetType {
		return appendReplaySet(dst, v, path)
	}

	switch v.Kind() {
	case reflect.Bool:
		if v.Bool() {
			return append(dst, 'b', '1', ':', '1'), nil
		}
		return append(dst, 'b', '1', ':', '0'), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return appendReplayFrame(dst, 'i', strconv.AppendInt(nil, v.Int(), 10)), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		// Signed and unsigned share the `i` tag: the VALUE is what the corpus
		// asks about, and uint64(1) and int64(1) are the same integer. Decimal
		// text keeps an integer past 2^53 exact, which float64 would not.
		return appendReplayFrame(dst, 'i', strconv.AppendUint(nil, v.Uint(), 10)), nil
	case reflect.Float32, reflect.Float64:
		// The IEEE-754 bit pattern, not a shortest-round-trip rendering: it is
		// exact, and it keeps distinct NaN payloads and ±0.0 distinct instead of
		// folding them together.
		return appendReplayFrame(dst, 'f', []byte(fmt.Sprintf("%016x", math.Float64bits(v.Float())))), nil
	case reflect.String:
		return appendReplayFrame(dst, 's', []byte(v.String())), nil
	case reflect.Slice, reflect.Array:
		if v.Type().Elem().Kind() == reflect.Uint8 && v.Type().Elem().PkgPath() == "" {
			return appendReplayFrame(dst, 'y', replayBytesOf(v)), nil
		}
		return appendReplaySequence(dst, v, path)
	case reflect.Map:
		return appendReplayMap(dst, v, path)
	case reflect.Struct:
		return appendReplayStruct(dst, v, path)
	}
	return nil, &ReplayEncodingError{Path: path, TypeName: v.Type().String()}
}

// replayBytesOf copies a []byte or [N]byte out of v.
func replayBytesOf(v reflect.Value) []byte {
	if v.Kind() == reflect.Slice {
		return v.Bytes()
	}
	out := make([]byte, v.Len())
	for i := range out {
		out[i] = byte(v.Index(i).Uint())
	}
	return out
}

func appendReplaySequence(dst []byte, v reflect.Value, path string) ([]byte, error) {
	var body []byte
	var err error
	for i := 0; i < v.Len(); i++ {
		body, err = appendReplayCanonical(body, v.Index(i), fmt.Sprintf("%s[%d]", path, i))
		if err != nil {
			return nil, err
		}
	}
	return appendReplayFrame(dst, 'l', body), nil
}

func appendReplaySet(dst []byte, v reflect.Value, path string) ([]byte, error) {
	members := make([]string, 0, v.Len())
	seen := make(map[string]struct{}, v.Len())
	for i := 0; i < v.Len(); i++ {
		encoded, err := appendReplayCanonical(nil, v.Index(i), path+"{}")
		if err != nil {
			return nil, err
		}
		// A set holds each member once, so an encoding seen twice contributes
		// once — otherwise ReplaySet{1, 1} and ReplaySet{1} would differ while
		// naming the same set.
		if _, duplicate := seen[string(encoded)]; duplicate {
			continue
		}
		seen[string(encoded)] = struct{}{}
		members = append(members, string(encoded))
	}
	sort.Strings(members)
	return appendReplayFrame(dst, 't', []byte(strings.Join(members, ""))), nil
}

func appendReplayMap(dst []byte, v reflect.Value, path string) ([]byte, error) {
	entries := make([]string, 0, v.Len())
	iter := v.MapRange()
	for iter.Next() {
		encoded, err := appendReplayCanonical(nil, iter.Key(), path+"[key]")
		if err != nil {
			return nil, err
		}
		encoded, err = appendReplayCanonical(encoded, iter.Value(), fmt.Sprintf("%s[%v]", path, iter.Key()))
		if err != nil {
			return nil, err
		}
		entries = append(entries, string(encoded))
	}
	// Sorted by the ENCODED bytes: Go randomizes map iteration, insertion order
	// is not part of the value, and mixed-type keys have no common ordering.
	sort.Strings(entries)
	return appendReplayFrame(dst, 'm', []byte(strings.Join(entries, ""))), nil
}

func appendReplayStruct(dst []byte, v reflect.Value, path string) ([]byte, error) {
	typ := v.Type()
	body, err := appendReplayCanonical(nil, reflect.ValueOf(typ.String()), path+".type")
	if err != nil {
		return nil, err
	}
	exported := 0
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if !field.IsExported() {
			// An unexported field is not observable state a fingerprint can
			// cover; a struct made entirely of them is refused below rather than
			// encoded as an empty value every instance would share.
			continue
		}
		exported++
		body, err = appendReplayCanonical(body, reflect.ValueOf(field.Name), path+"."+field.Name)
		if err != nil {
			return nil, err
		}
		body, err = appendReplayCanonical(body, v.Field(i), path+"."+field.Name)
		if err != nil {
			return nil, err
		}
	}
	if exported == 0 {
		return nil, &ReplayEncodingError{Path: path, TypeName: typ.String()}
	}
	return appendReplayFrame(dst, 'd', body), nil
}

// replayPreview renders a value for a divergence MESSAGE only. It never reaches
// a digest, so its instability is harmless here.
func replayPreview(value any) string {
	text := fmt.Sprintf("%v", value)
	if len(text) > replayPreviewLimit {
		return text[:replayPreviewLimit-1] + "…"
	}
	return text
}

// ---------------------------------------------------------------------------
// the log
// ---------------------------------------------------------------------------

// ReplayEvent is one entry of an ordered event log.
type ReplayEvent struct {
	Seq     int64
	Name    string
	Payload any
}

// ReplayRecord is an unnumbered event, for NewReplayLogFromRecords.
type ReplayRecord struct {
	Name    string
	Payload any
}

// ReplayLog is an ordered event log with a digest over its canonical bytes.
//
// Sequence numbers must strictly increase. They need NOT be contiguous: an
// ack-truncated durable outbox replays real epochs (see
// lazily-spec/docs/durable-outbox.md), and renumbering them would hide a
// truncated prefix that the log digest otherwise catches.
type ReplayLog struct {
	events []ReplayEvent
	digest string
}

// NewReplayLog validates the event order and digests the log.
func NewReplayLog(events ...ReplayEvent) (*ReplayLog, error) {
	owned := make([]ReplayEvent, len(events))
	copy(owned, events)
	previous := int64(0)
	for i, event := range owned {
		if event.Seq < 0 {
			return nil, replayErrorf("event seq must be non-negative, got %d", event.Seq)
		}
		if event.Name == "" {
			return nil, replayErrorf("event name must be non-empty (event at index %d)", i)
		}
		if i > 0 && event.Seq <= previous {
			return nil, replayErrorf("event log must be strictly increasing in seq, got %d after %d",
				event.Seq, previous)
		}
		previous = event.Seq
	}
	digest, err := ReplayCanonicalDigest(owned)
	if err != nil {
		return nil, err
	}
	return &ReplayLog{events: owned, digest: digest}, nil
}

// NewReplayLogFromRecords numbers records 0..n-1.
func NewReplayLogFromRecords(records ...ReplayRecord) (*ReplayLog, error) {
	events := make([]ReplayEvent, len(records))
	for i, record := range records {
		events[i] = ReplayEvent{Seq: int64(i), Name: record.Name, Payload: record.Payload}
	}
	return NewReplayLog(events...)
}

// Digest is the log's canonical digest — the bytes a fingerprint is bound to.
func (l *ReplayLog) Digest() string { return l.digest }

// Len is the number of events.
func (l *ReplayLog) Len() int { return len(l.events) }

// Event returns the i-th event.
func (l *ReplayLog) Event(i int) ReplayEvent { return l.events[i] }

// Events returns a copy of the event slice, so a caller cannot mutate the log
// its digest was taken over.
func (l *ReplayLog) Events() []ReplayEvent {
	out := make([]ReplayEvent, len(l.events))
	copy(out, l.events)
	return out
}

// ReplayOutbox is the one DurableStoreOutbox operation a replay log needs.
type ReplayOutbox interface {
	ReplayFrom(cursor Epoch) []OutboxEntry
}

// ReplayLogFromOutbox builds a ReplayLog from a reliable-sync outbox's retained
// frames.
//
// ReplayFrom is already the replay source a reconnect drains; this makes it the
// FINGERPRINTED one too. Outbox epochs become event seqs, so a truncated prefix
// shows up in the log digest instead of silently shifting every event. The
// payload is each frame's encoded wire bytes rather than the decoded message:
// the bytes are what was retained, and they have a defined canonical encoding.
func ReplayLogFromOutbox(outbox ReplayOutbox, cursor Epoch, name string) (*ReplayLog, error) {
	if name == "" {
		name = "frame"
	}
	entries := outbox.ReplayFrom(cursor)
	events := make([]ReplayEvent, 0, len(entries))
	for _, entry := range entries {
		frame, err := entry.Msg.EncodeJSON()
		if err != nil {
			return nil, replayErrorf("outbox frame at epoch %d could not be encoded: %v", entry.Epoch, err)
		}
		events = append(events, ReplayEvent{Seq: int64(entry.Epoch), Name: name, Payload: frame})
	}
	return NewReplayLog(events...)
}

// ---------------------------------------------------------------------------
// the fingerprint
// ---------------------------------------------------------------------------

// ReplayCell is one observed cell's label and value digest.
type ReplayCell struct {
	Label  string
	Digest string
}

// ReplayCheckpoint holds the per-cell digests observed after applying events
// through Seq. Seq is ReplayInitialSeq for the state before any event.
type ReplayCheckpoint struct {
	Seq int64
	// Cells is sorted by label, so a checkpoint has one representation.
	Cells []ReplayCell
}

// NewReplayCheckpoint digests every observed value.
func NewReplayCheckpoint(seq int64, observed map[string]any) (ReplayCheckpoint, error) {
	cells := make([]ReplayCell, 0, len(observed))
	for label, value := range observed {
		digest, err := ReplayCanonicalDigest(value)
		if err != nil {
			return ReplayCheckpoint{}, err
		}
		cells = append(cells, ReplayCell{Label: label, Digest: digest})
	}
	sort.Slice(cells, func(i, j int) bool { return cells[i].Label < cells[j].Label })
	return ReplayCheckpoint{Seq: seq, Cells: cells}, nil
}

// Digests is the label -> digest mapping.
func (c ReplayCheckpoint) Digests() map[string]string {
	out := make(map[string]string, len(c.Cells))
	for _, cell := range c.Cells {
		out[cell.Label] = cell.Digest
	}
	return out
}

// ReplayFingerprint is a recorded, log-bound observation of a replayed graph.
type ReplayFingerprint struct {
	logDigest   string
	stride      int
	checkpoints []ReplayCheckpoint
	digest      string
}

// NewReplayFingerprint validates and digests a fingerprint.
func NewReplayFingerprint(logDigest string, stride int, checkpoints []ReplayCheckpoint) (*ReplayFingerprint, error) {
	if stride < 1 {
		return nil, replayErrorf("stride must be >= 1, got %d", stride)
	}
	if len(checkpoints) == 0 {
		return nil, replayErrorf("a fingerprint needs at least the initial checkpoint")
	}
	owned := make([]ReplayCheckpoint, len(checkpoints))
	copy(owned, checkpoints)
	digest, err := ReplayCanonicalDigest([]any{logDigest, int64(stride), owned})
	if err != nil {
		return nil, err
	}
	return &ReplayFingerprint{logDigest: logDigest, stride: stride, checkpoints: owned, digest: digest}, nil
}

// LogDigest is the digest of the log this fingerprint was recorded against.
func (f *ReplayFingerprint) LogDigest() string { return f.logDigest }

// Stride is the checkpoint stride this fingerprint was sampled at.
func (f *ReplayFingerprint) Stride() int { return f.stride }

// Digest is the fingerprint's own canonical digest, so one can be compared or
// pinned as a single value.
func (f *ReplayFingerprint) Digest() string { return f.digest }

// Checkpoints returns a copy of the checkpoint sequence.
func (f *ReplayFingerprint) Checkpoints() []ReplayCheckpoint {
	out := make([]ReplayCheckpoint, len(f.checkpoints))
	copy(out, f.checkpoints)
	return out
}

// CheckpointSeqs is the sequence numbers the fingerprint covers, in order.
func (f *ReplayFingerprint) CheckpointSeqs() []int64 {
	out := make([]int64, len(f.checkpoints))
	for i, checkpoint := range f.checkpoints {
		out[i] = checkpoint.Seq
	}
	return out
}

// Final is the last checkpoint — the end state of the replay.
func (f *ReplayFingerprint) Final() ReplayCheckpoint { return f.checkpoints[len(f.checkpoints)-1] }

// ReplayFingerprintWire is the JSON-safe form of a fingerprint, so one can be
// committed next to a test.
type ReplayFingerprintWire struct {
	SchemaVersion int                    `json:"schema_version"`
	LogDigest     string                 `json:"log_digest"`
	Stride        int                    `json:"stride"`
	Checkpoints   []ReplayCheckpointWire `json:"checkpoints"`
}

// ReplayCheckpointWire is one checkpoint of ReplayFingerprintWire.
type ReplayCheckpointWire struct {
	Seq   int64             `json:"seq"`
	Cells map[string]string `json:"cells"`
}

// Wire converts the fingerprint to its JSON-safe form.
func (f *ReplayFingerprint) Wire() ReplayFingerprintWire {
	checkpoints := make([]ReplayCheckpointWire, len(f.checkpoints))
	for i, checkpoint := range f.checkpoints {
		checkpoints[i] = ReplayCheckpointWire{Seq: checkpoint.Seq, Cells: checkpoint.Digests()}
	}
	return ReplayFingerprintWire{
		SchemaVersion: replayWireSchemaVersion,
		LogDigest:     f.logDigest,
		Stride:        f.stride,
		Checkpoints:   checkpoints,
	}
}

// ReplayFingerprintFromWire rebuilds a fingerprint, refusing an unknown schema
// version rather than guessing at a layout it does not know.
func ReplayFingerprintFromWire(wire ReplayFingerprintWire) (*ReplayFingerprint, error) {
	if wire.SchemaVersion != replayWireSchemaVersion {
		return nil, replayErrorf("unsupported replay fingerprint schema_version %d, expected %d",
			wire.SchemaVersion, replayWireSchemaVersion)
	}
	checkpoints := make([]ReplayCheckpoint, len(wire.Checkpoints))
	for i, checkpoint := range wire.Checkpoints {
		cells := make([]ReplayCell, 0, len(checkpoint.Cells))
		for label, digest := range checkpoint.Cells {
			cells = append(cells, ReplayCell{Label: label, Digest: digest})
		}
		sort.Slice(cells, func(a, b int) bool { return cells[a].Label < cells[b].Label })
		checkpoints[i] = ReplayCheckpoint{Seq: checkpoint.Seq, Cells: cells}
	}
	return NewReplayFingerprint(wire.LogDigest, wire.Stride, checkpoints)
}

// ReplayDivergence is one cell that did not replay to its recorded digest.
type ReplayDivergence struct {
	Seq   int64
	Label string
	// Kind is "value", "missing" (the fingerprint has the cell and the replay
	// did not observe it) or "unexpected" (the replay observed a cell the
	// fingerprint does not carry).
	Kind     string
	Expected string
	Actual   string
	// Preview renders the replayed value for the message. It is never hashed.
	Preview string
}

func (d ReplayDivergence) String() string {
	where := fmt.Sprintf("event seq=%d", d.Seq)
	if d.Seq == ReplayInitialSeq {
		where = "initial state"
	}
	switch d.Kind {
	case "missing":
		return fmt.Sprintf("%s: cell %q was not observed on replay", where, d.Label)
	case "unexpected":
		return fmt.Sprintf("%s: cell %q appeared on replay but is not in the fingerprint", where, d.Label)
	}
	preview := ""
	if d.Preview != "" {
		preview = ", observed " + d.Preview
	}
	return fmt.Sprintf("%s: cell %q expected %s but replayed %s%s", where, d.Label, d.Expected, d.Actual, preview)
}

// ---------------------------------------------------------------------------
// the harness
// ---------------------------------------------------------------------------

// ReplayGraph is what the harness needs from the graph it rebuilds.
//
// Apply advances the graph by exactly one event; Observe returns the cell values
// the fingerprint covers, keyed by a stable label.
type ReplayGraph interface {
	Apply(event ReplayEvent)
	Observe() map[string]any
}

// ReplayHarness rebuilds a graph from an event log and proves it replays
// identically.
//
// The build function is called once per replay and must return a FRESH graph: a
// harness that reuses one instance proves nothing, since the state it would
// compare against is the state it already has.
//
// A ReplayHarness is immutable after construction and safe for concurrent use;
// the graphs it builds are not shared between replays.
type ReplayHarness struct {
	build  func() ReplayGraph
	stride int
}

// ReplayOption configures a ReplayHarness.
type ReplayOption func(*ReplayHarness)

// ReplayWithStride checkpoints every n-th event. The initial state and the final
// state are always checkpointed.
//
// The stride is recorded IN the fingerprint, so a fingerprint cannot be compared
// against a replay that sampled differently — equal log digest plus equal stride
// is what makes two checkpoint sequences comparable at all.
func ReplayWithStride(n int) ReplayOption {
	return func(h *ReplayHarness) { h.stride = n }
}

// NewReplayHarness builds a harness over a graph constructor.
func NewReplayHarness(build func() ReplayGraph, options ...ReplayOption) (*ReplayHarness, error) {
	if build == nil {
		return nil, replayErrorf("a replay harness needs a build function that returns a fresh graph")
	}
	harness := &ReplayHarness{build: build, stride: 1}
	for _, option := range options {
		option(harness)
	}
	if harness.stride < 1 {
		return nil, replayErrorf("stride must be >= 1, got %d", harness.stride)
	}
	return harness, nil
}

// Stride is the checkpoint stride this harness samples at.
func (h *ReplayHarness) Stride() int { return h.stride }

// Record replays log once and records what the graph observed.
func (h *ReplayHarness) Record(log *ReplayLog) (*ReplayFingerprint, error) {
	fingerprint, _, err := h.replay(log)
	return fingerprint, err
}

// Check replays log and RETURNS the divergences from fingerprint instead of
// failing on them, so a caller can report all of them.
//
// It still refuses a fingerprint recorded against a different log
// (*ReplayLogMismatchError) or at a different stride
// (*ReplayStrideMismatchError): comparing either would answer a question nobody
// asked. A stale fingerprint is an unanswerable question, not a report.
func (h *ReplayHarness) Check(log *ReplayLog, fingerprint *ReplayFingerprint) ([]ReplayDivergence, error) {
	replayed, observed, err := h.replay(log)
	if err != nil {
		return nil, err
	}
	if err := replayRevalidate(fingerprint, replayed); err != nil {
		return nil, err
	}
	return replayCompare(fingerprint, replayed, observed)
}

// Verify replays log and fails unless it matches fingerprint exactly, returning
// the freshly recorded fingerprint (which equals the one passed in).
func (h *ReplayHarness) Verify(log *ReplayLog, fingerprint *ReplayFingerprint) (*ReplayFingerprint, error) {
	replayed, observed, err := h.replay(log)
	if err != nil {
		return nil, err
	}
	if err := replayRevalidate(fingerprint, replayed); err != nil {
		return nil, err
	}
	divergences, err := replayCompare(fingerprint, replayed, observed)
	if err != nil {
		return nil, err
	}
	if len(divergences) > 0 {
		return nil, &ReplayDivergenceError{Divergences: divergences}
	}
	return replayed, nil
}

// Prove records log and re-replays it, failing on any divergence.
//
// This is the self-check: no external fingerprint is needed to catch a graph
// that is not a pure function of its log, because two replays of the same log in
// the same process already disagree.
func (h *ReplayHarness) Prove(log *ReplayLog, replays int) (*ReplayFingerprint, error) {
	if replays < 2 {
		return nil, replayErrorf("prove needs at least 2 replays to compare, got %d", replays)
	}
	fingerprint, err := h.Record(log)
	if err != nil {
		return nil, err
	}
	for i := 1; i < replays; i++ {
		if _, err := h.Verify(log, fingerprint); err != nil {
			return nil, err
		}
	}
	return fingerprint, nil
}

// replayRevalidate binds the fingerprint to these exact log bytes BEFORE any
// value is compared. The order is the obligation: a stale fingerprint compared
// anyway would be reported as a divergence, blaming the graph for a stale test
// artifact.
func replayRevalidate(fingerprint, replayed *ReplayFingerprint) error {
	if fingerprint == nil {
		return replayErrorf("no fingerprint to verify against")
	}
	if fingerprint.logDigest != replayed.logDigest {
		return &ReplayLogMismatchError{
			ExpectedDigest: fingerprint.logDigest,
			ActualDigest:   replayed.logDigest,
		}
	}
	if fingerprint.stride != replayed.stride {
		return &ReplayStrideMismatchError{
			ExpectedStride: fingerprint.stride,
			ActualStride:   replayed.stride,
		}
	}
	return nil
}

func (h *ReplayHarness) replay(log *ReplayLog) (*ReplayFingerprint, []map[string]any, error) {
	if log == nil {
		return nil, nil, replayErrorf("no event log to replay")
	}
	graph := h.build()
	if graph == nil {
		return nil, nil, replayErrorf("the build function returned no graph")
	}
	sample := graph.Observe()
	checkpoint, err := NewReplayCheckpoint(ReplayInitialSeq, sample)
	if err != nil {
		return nil, nil, err
	}
	checkpoints := []ReplayCheckpoint{checkpoint}
	observed := []map[string]any{sample}
	total := log.Len()
	for index := 0; index < total; index++ {
		event := log.Event(index)
		graph.Apply(event)
		if (index+1)%h.stride != 0 && index+1 != total {
			continue
		}
		sample = graph.Observe()
		checkpoint, err := NewReplayCheckpoint(event.Seq, sample)
		if err != nil {
			return nil, nil, err
		}
		checkpoints = append(checkpoints, checkpoint)
		observed = append(observed, sample)
	}
	fingerprint, err := NewReplayFingerprint(log.Digest(), h.stride, checkpoints)
	if err != nil {
		return nil, nil, err
	}
	return fingerprint, observed, nil
}

// replayCompare walks the checkpoints in order and stops at the FIRST one that
// diverged. Later checkpoints are almost always the same defect carried forward;
// the actionable answer is where it started.
func replayCompare(expected, actual *ReplayFingerprint, observed []map[string]any) ([]ReplayDivergence, error) {
	var divergences []ReplayDivergence
	shared := len(expected.checkpoints)
	if len(actual.checkpoints) < shared {
		shared = len(actual.checkpoints)
	}
	for index := 0; index < shared; index++ {
		want := expected.checkpoints[index]
		got := actual.checkpoints[index]
		wantCells := want.Digests()
		gotCells := got.Digests()
		labels := make([]string, 0, len(wantCells)+len(gotCells))
		for label := range wantCells {
			labels = append(labels, label)
		}
		for label := range gotCells {
			if _, both := wantCells[label]; !both {
				labels = append(labels, label)
			}
		}
		sort.Strings(labels)
		for _, label := range labels {
			wantDigest, inWant := wantCells[label]
			gotDigest, inGot := gotCells[label]
			if wantDigest == gotDigest {
				continue
			}
			kind := "value"
			switch {
			case !inGot:
				kind = "missing"
			case !inWant:
				kind = "unexpected"
			}
			divergence := ReplayDivergence{
				Seq:      want.Seq,
				Label:    label,
				Kind:     kind,
				Expected: wantDigest,
				Actual:   gotDigest,
			}
			if index < len(observed) {
				if value, sampled := observed[index][label]; sampled {
					divergence.Preview = replayPreview(value)
				}
			}
			divergences = append(divergences, divergence)
		}
		if len(divergences) > 0 {
			break
		}
	}
	if len(divergences) == 0 && len(expected.checkpoints) != len(actual.checkpoints) {
		// Same log digest and the same stride, so this cannot come from
		// sampling: Observe or Apply changed the checkpoint count.
		return nil, replayErrorf("fingerprint has %d checkpoints but the replay produced %d for the same log",
			len(expected.checkpoints), len(actual.checkpoints))
	}
	return divergences, nil
}
