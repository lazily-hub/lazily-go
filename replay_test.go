package lazily

import (
	"errors"
	"math"
	"strings"
	"testing"
)

// Unit cover for the replay-equivalence harness (`#lzreplaygo`). The canonical
// corpus is replayed in replay_conformance_test.go; these are the properties the
// corpus cannot state — Go-specific encoding classes, the error TYPES a driver
// routes on, the wire form, and the outbox source.

// replayCounter is the smallest possible ReplayGraph.
type replayCounter struct {
	total int64
	extra int64
	label string
}

func (c *replayCounter) Apply(event ReplayEvent) { c.total += event.Payload.(int64) }

func (c *replayCounter) Observe() map[string]any {
	observed := map[string]any{"total": c.total}
	if c.label != "" {
		observed[c.label] = c.extra
	}
	return observed
}

func replayTestLog(t *testing.T, payloads ...int64) *ReplayLog {
	t.Helper()
	events := make([]ReplayEvent, len(payloads))
	for i, payload := range payloads {
		events[i] = ReplayEvent{Seq: int64(i), Name: "add", Payload: payload}
	}
	log, err := NewReplayLog(events...)
	if err != nil {
		t.Fatalf("NewReplayLog: %v", err)
	}
	return log
}

func replayTestHarness(t *testing.T, build func() ReplayGraph, options ...ReplayOption) *ReplayHarness {
	t.Helper()
	harness, err := NewReplayHarness(build, options...)
	if err != nil {
		t.Fatalf("NewReplayHarness: %v", err)
	}
	return harness
}

// ---------------------------------------------------------------------------
// obligation 1: the fingerprint is bound to its log
// ---------------------------------------------------------------------------

func TestReplayFingerprintIsRefusedAgainstAnotherLog(t *testing.T) {
	// The two logs settle on the SAME total, so a value-only comparison would
	// pass and certify nothing about the log in front of it.
	first := replayTestLog(t, 1, 2, 3)
	second := replayTestLog(t, 3, 2, 1)
	if first.Digest() == second.Digest() {
		t.Fatalf("two differently-ordered logs share a digest %s — event order is part of the log", first.Digest())
	}
	harness := replayTestHarness(t, func() ReplayGraph { return &replayCounter{} })
	fingerprint, err := harness.Record(first)
	if err != nil {
		t.Fatalf("Record: %v", err)
	}

	_, err = harness.Verify(second, fingerprint)
	var mismatch *ReplayLogMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("Verify against another log = %v, want *ReplayLogMismatchError", err)
	}
	if !errors.Is(err, ErrReplayProof) {
		t.Fatalf("a log mismatch does not unwrap to ErrReplayProof, so a caller cannot route on the family")
	}
	if mismatch.ExpectedDigest != first.Digest() || mismatch.ActualDigest != second.Digest() {
		t.Fatalf("the mismatch names %s vs %s, want %s vs %s",
			mismatch.ExpectedDigest, mismatch.ActualDigest, first.Digest(), second.Digest())
	}

	// The reporting form refuses it too: a stale fingerprint is an unanswerable
	// question, not a report of zero divergences.
	divergences, err := harness.Check(second, fingerprint)
	if !errors.As(err, &mismatch) {
		t.Fatalf("Check against another log = %v, want *ReplayLogMismatchError", err)
	}
	if divergences != nil {
		t.Fatalf("Check returned %d divergences beside a log mismatch; it must not compare at all", len(divergences))
	}
}

func TestReplayLogRejectsAMalformedEventSequence(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		events []ReplayEvent
		want   string
	}{
		{"negative seq", []ReplayEvent{{Seq: -1, Name: "add"}}, "non-negative"},
		{"empty name", []ReplayEvent{{Seq: 0, Name: ""}}, "non-empty"},
		{
			"repeated seq",
			[]ReplayEvent{{Seq: 1, Name: "add"}, {Seq: 1, Name: "add"}},
			"strictly increasing",
		},
		{
			"descending seq",
			[]ReplayEvent{{Seq: 2, Name: "add"}, {Seq: 1, Name: "add"}},
			"strictly increasing",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := NewReplayLog(testCase.events...)
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("NewReplayLog = %v, want an error mentioning %q", err, testCase.want)
			}
			if !errors.Is(err, ErrReplayProof) {
				t.Fatalf("%v does not unwrap to ErrReplayProof", err)
			}
		})
	}
}

func TestReplayLogAcceptsNonContiguousSeqs(t *testing.T) {
	// An ack-truncated durable outbox replays REAL epochs; renumbering them
	// would hide a truncated prefix the log digest otherwise catches.
	contiguous, err := NewReplayLog(
		ReplayEvent{Seq: 0, Name: "frame", Payload: int64(1)},
		ReplayEvent{Seq: 1, Name: "frame", Payload: int64(2)},
	)
	if err != nil {
		t.Fatalf("NewReplayLog: %v", err)
	}
	truncated, err := NewReplayLog(
		ReplayEvent{Seq: 7, Name: "frame", Payload: int64(1)},
		ReplayEvent{Seq: 9, Name: "frame", Payload: int64(2)},
	)
	if err != nil {
		t.Fatalf("NewReplayLog with a gap: %v", err)
	}
	if contiguous.Digest() == truncated.Digest() {
		t.Fatalf("renumbering the epochs left the log digest unchanged")
	}
}

func TestReplayLogFromOutboxFingerprintsRetainedFrames(t *testing.T) {
	outbox := NewDurableStoreOutbox(NewInMemoryStore())
	for epoch := Epoch(1); epoch <= 3; epoch++ {
		outbox.Append(epoch, IpcMessageOutboxAck{Value: OutboxAck{ThroughEpoch: epoch}})
	}
	full, err := ReplayLogFromOutbox(outbox, 0, "")
	if err != nil {
		t.Fatalf("ReplayLogFromOutbox: %v", err)
	}
	if got := full.Len(); got != 3 {
		t.Fatalf("replay log has %d events, want 3", got)
	}
	for i, event := range full.Events() {
		if event.Seq != int64(i+1) {
			t.Fatalf("event %d carries seq %d, want the outbox epoch %d", i, event.Seq, i+1)
		}
		if event.Name != "frame" {
			t.Fatalf("event %d is named %q, want the default %q", i, event.Name, "frame")
		}
	}

	// Truncating the acknowledged prefix must be VISIBLE in the digest.
	outbox.AckThrough(2)
	truncated, err := ReplayLogFromOutbox(outbox, 0, "")
	if err != nil {
		t.Fatalf("ReplayLogFromOutbox after ack: %v", err)
	}
	if truncated.Len() != 1 || truncated.Event(0).Seq != 3 {
		t.Fatalf("after AckThrough(2) the log is %d events starting at %d, want 1 starting at 3",
			truncated.Len(), truncated.Event(0).Seq)
	}
	if truncated.Digest() == full.Digest() {
		t.Fatalf("a truncated prefix left the log digest unchanged")
	}
}

// ---------------------------------------------------------------------------
// obligation 2: divergence is localized, and the stride is part of the binding
// ---------------------------------------------------------------------------

func TestReplayDivergenceNamesTheFirstCheckpoint(t *testing.T) {
	log := replayTestLog(t, 1, 2, 3, 4)
	honest := replayTestHarness(t, func() ReplayGraph { return &replayCounter{} })
	fingerprint, err := honest.Record(log)
	if err != nil {
		t.Fatalf("Record: %v", err)
	}
	// A graph that takes a value from outside its log, from the second event on.
	skewed := replayTestHarness(t, func() ReplayGraph { return &replayDriftingCounter{driftAt: 1, drift: 100} })

	_, err = skewed.Verify(log, fingerprint)
	var divergence *ReplayDivergenceError
	if !errors.As(err, &divergence) {
		t.Fatalf("Verify of a drifting graph = %v, want *ReplayDivergenceError", err)
	}
	first := divergence.First()
	if first.Seq != 1 || first.Label != "total" || first.Kind != "value" {
		t.Fatalf("first divergence = %+v, want seq=1 label=total kind=value", first)
	}
	// Seq 0 still matched, so reporting only the final state would name the
	// wrong event; and later checkpoints are the same defect carried forward.
	if len(divergence.Divergences) != 1 {
		t.Fatalf("%d divergences collected, want only the first diverging checkpoint's", len(divergence.Divergences))
	}
	if !strings.Contains(divergence.Error(), "event seq=1") {
		t.Fatalf("the error message does not locate the divergence: %s", divergence.Error())
	}
}

// replayDriftingCounter is replayCounter plus a value taken from OUTSIDE the
// log, which is the defect a replay proof exists to find.
type replayDriftingCounter struct {
	total   int64
	driftAt int64
	drift   int64
}

func (c *replayDriftingCounter) Apply(event ReplayEvent) {
	c.total += event.Payload.(int64)
	if event.Seq == c.driftAt {
		c.total += c.drift
	}
}

func (c *replayDriftingCounter) Observe() map[string]any { return map[string]any{"total": c.total} }

func TestReplayStrideIsPartOfTheFingerprint(t *testing.T) {
	log := replayTestLog(t, 1, 2, 3, 4)
	sparse := replayTestHarness(t, func() ReplayGraph { return &replayCounter{} }, ReplayWithStride(2))
	fingerprint, err := sparse.Record(log)
	if err != nil {
		t.Fatalf("Record: %v", err)
	}
	// The initial state and the final state are always checkpointed.
	want := []int64{ReplayInitialSeq, 1, 3}
	got := fingerprint.CheckpointSeqs()
	if len(got) != len(want) {
		t.Fatalf("stride-2 checkpoints %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("stride-2 checkpoints %v, want %v", got, want)
		}
	}

	dense := replayTestHarness(t, func() ReplayGraph { return &replayCounter{} })
	_, err = dense.Verify(log, fingerprint)
	var mismatch *ReplayStrideMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("Verify at another stride = %v, want *ReplayStrideMismatchError", err)
	}
	if mismatch.ExpectedStride != 2 || mismatch.ActualStride != 1 {
		t.Fatalf("stride mismatch names %d vs %d, want 2 vs 1", mismatch.ExpectedStride, mismatch.ActualStride)
	}
	// It is its OWN fault, not a divergence and not a log mismatch: the log is
	// the right one, the two checkpoint sequences were never comparable.
	var divergence *ReplayDivergenceError
	var logMismatch *ReplayLogMismatchError
	if errors.As(err, &divergence) || errors.As(err, &logMismatch) {
		t.Fatalf("a stride mismatch is reported as %v, which routes a driver to the wrong repair", err)
	}
	if _, err := NewReplayHarness(func() ReplayGraph { return &replayCounter{} }, ReplayWithStride(0)); err == nil {
		t.Fatalf("stride 0 was accepted")
	}
}

func TestReplayReportsAMissingAndAnUnexpectedCell(t *testing.T) {
	log := replayTestLog(t, 1)
	wide := replayTestHarness(t, func() ReplayGraph { return &replayCounter{label: "extra"} })
	narrow := replayTestHarness(t, func() ReplayGraph { return &replayCounter{} })
	wideFingerprint, err := wide.Record(log)
	if err != nil {
		t.Fatalf("Record: %v", err)
	}
	narrowFingerprint, err := narrow.Record(log)
	if err != nil {
		t.Fatalf("Record: %v", err)
	}

	divergences, err := narrow.Check(log, wideFingerprint)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if len(divergences) != 1 || divergences[0].Kind != "missing" || divergences[0].Label != "extra" {
		t.Fatalf("divergences = %+v, want one `missing` for `extra`", divergences)
	}
	if !strings.Contains(divergences[0].String(), "initial state") {
		t.Fatalf("a divergence at the pre-event checkpoint reads as %q", divergences[0].String())
	}

	divergences, err = wide.Check(log, narrowFingerprint)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if len(divergences) != 1 || divergences[0].Kind != "unexpected" || divergences[0].Label != "extra" {
		t.Fatalf("divergences = %+v, want one `unexpected` for `extra`", divergences)
	}
}

func TestReplayProveCatchesAGraphThatIsNotAFunctionOfItsLog(t *testing.T) {
	log := replayTestLog(t, 1, 2)
	honest := replayTestHarness(t, func() ReplayGraph { return &replayCounter{} })
	if _, err := honest.Prove(log, 2); err != nil {
		t.Fatalf("Prove of a pure graph: %v", err)
	}
	if _, err := honest.Prove(log, 1); err == nil {
		t.Fatalf("Prove accepted a single replay, which compares nothing")
	}

	// No external fingerprint is needed: two replays of the same log in the same
	// process already disagree when the graph reads something outside it.
	builds := 0
	drifting := replayTestHarness(t, func() ReplayGraph {
		builds++
		return &replayCounter{total: int64(builds)}
	})
	_, err := drifting.Prove(log, 2)
	var divergence *ReplayDivergenceError
	if !errors.As(err, &divergence) {
		t.Fatalf("Prove of an impure graph = %v, want *ReplayDivergenceError", err)
	}
	if divergence.First().Seq != ReplayInitialSeq {
		t.Fatalf("the impurity is visible before any event, but was reported at seq %d", divergence.First().Seq)
	}
}

// ---------------------------------------------------------------------------
// obligation 3: the observation encoding is canonical, or it fails
// ---------------------------------------------------------------------------

type replayPoint struct {
	X int64
	Y int64
	// hidden is unexported, so it is not observable state a fingerprint covers.
	hidden int64
}

func replayDigest(t *testing.T, value any) string {
	t.Helper()
	digest, err := ReplayCanonicalDigest(value)
	if err != nil {
		t.Fatalf("ReplayCanonicalDigest(%v): %v", value, err)
	}
	return digest
}

func TestReplayCanonicalEncodingEqualityClasses(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		left  any
		right any
		equal bool
	}{
		{"map order", map[string]any{"a": int64(1), "b": int64(2)}, map[string]any{"b": int64(2), "a": int64(1)}, true},
		{"set order", ReplaySet{int64(1), int64(2), int64(3)}, ReplaySet{int64(3), int64(1), int64(2)}, true},
		{"set duplicates", ReplaySet{int64(1), int64(1)}, ReplaySet{int64(1)}, true},
		{"sequence order", []any{int64(1), int64(2)}, []any{int64(2), int64(1)}, false},
		{"a set is not a sequence", ReplaySet{int64(1)}, []any{int64(1)}, false},
		{"int and its text", int64(1), "1", false},
		{"int and the equal float", int64(1), 1.0, false},
		{"int and the boolean", int64(1), true, false},
		{"text and the equal bytes", "1", []byte{0x31}, false},
		{"member framing", []any{"a", "bc"}, []any{"ab", "c"}, false},
		{"adjacent integers past 2^53", int64(9007199254740993), int64(9007199254740994), false},
		{"signed and unsigned of one value", int64(7), uint64(7), true},
		{"nil and the empty string", nil, "", false},
		{"positive and negative zero", 0.0, math.Copysign(0, -1), false},
		{"a struct and its field values", replayPoint{X: 1, Y: 2}, []any{int64(1), int64(2)}, false},
		{"unexported state", replayPoint{X: 1, hidden: 1}, replayPoint{X: 1, hidden: 2}, true},
		{"typed nil pointer and nil", (*replayPoint)(nil), nil, true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			equal := replayDigest(t, testCase.left) == replayDigest(t, testCase.right)
			if equal != testCase.equal {
				t.Fatalf("digests equal = %v, want %v", equal, testCase.equal)
			}
		})
	}

	// Distinct types with the same field values are distinct values.
	type otherPoint struct{ X, Y int64 }
	if replayDigest(t, replayPoint{X: 1, Y: 2}) == replayDigest(t, otherPoint{X: 1, Y: 2}) {
		t.Fatalf("two different struct types with equal fields share a digest")
	}
	// A NaN payload is not folded away by a shortest-round-trip rendering, which
	// would render every NaN as the same text. (Go's own math.NaN() already sets
	// the low payload bit, so the second pattern sets a higher one.)
	otherNaN := math.Float64frombits(math.Float64bits(math.NaN()) | 4)
	if !math.IsNaN(otherNaN) || math.Float64bits(otherNaN) == math.Float64bits(math.NaN()) {
		t.Fatalf("the test's second NaN is not a distinct NaN bit pattern")
	}
	if replayDigest(t, math.NaN()) == replayDigest(t, otherNaN) {
		t.Fatalf("two distinct NaN bit patterns share a digest")
	}
}

func TestReplayEncodingFailsLoudlyRatherThanRenderingAnAddress(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		value any
	}{
		{"channel", make(chan struct{})},
		{"func", func() {}},
		{"complex", complex(1, 2)},
		{"struct with no exported field", struct{ hidden int }{hidden: 1}},
		{"nested in a map", map[string]any{"cell": make(chan struct{})}},
		{"nested in a slice", []any{int64(1), func() {}}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := ReplayCanonicalDigest(testCase.value)
			var encoding *ReplayEncodingError
			if !errors.As(err, &encoding) {
				t.Fatalf("ReplayCanonicalDigest = %v, want *ReplayEncodingError — a default rendering embeds an "+
					"address and would report a FALSE divergence on every run", err)
			}
			if !errors.Is(err, ErrReplayProof) {
				t.Fatalf("%v does not unwrap to ErrReplayProof", err)
			}
			if encoding.Path == "" || encoding.TypeName == "" {
				t.Fatalf("the encoding error locates nothing: %+v", encoding)
			}
		})
	}

	// The harness surfaces it rather than fingerprinting a partial observation.
	log := replayTestLog(t, 1)
	harness := replayTestHarness(t, func() ReplayGraph { return &replayOpaqueGraph{} })
	var encoding *ReplayEncodingError
	if _, err := harness.Record(log); !errors.As(err, &encoding) {
		t.Fatalf("Record of an unencodable observation = %v, want *ReplayEncodingError", err)
	}
}

// replayOpaqueGraph observes a value the encoding does not define.
type replayOpaqueGraph struct{}

func (replayOpaqueGraph) Apply(ReplayEvent) {}

func (replayOpaqueGraph) Observe() map[string]any { return map[string]any{"cell": make(chan struct{})} }

// ---------------------------------------------------------------------------
// the wire form
// ---------------------------------------------------------------------------

func TestReplayFingerprintWireRoundTrips(t *testing.T) {
	log := replayTestLog(t, 1, 2, 3)
	harness := replayTestHarness(t, func() ReplayGraph { return &replayCounter{} })
	fingerprint, err := harness.Record(log)
	if err != nil {
		t.Fatalf("Record: %v", err)
	}
	restored, err := ReplayFingerprintFromWire(fingerprint.Wire())
	if err != nil {
		t.Fatalf("ReplayFingerprintFromWire: %v", err)
	}
	if restored.Digest() != fingerprint.Digest() {
		t.Fatalf("the round-tripped fingerprint digests %s, want %s", restored.Digest(), fingerprint.Digest())
	}
	// A pinned fingerprint is still usable as the thing a replay is verified
	// against, which is the whole point of committing one next to a test.
	if _, err := harness.Verify(log, restored); err != nil {
		t.Fatalf("Verify against a round-tripped fingerprint: %v", err)
	}

	wire := fingerprint.Wire()
	wire.SchemaVersion++
	if _, err := ReplayFingerprintFromWire(wire); err == nil {
		t.Fatalf("an unknown schema_version was accepted; a layout this code does not know must be refused")
	}
}
