package lazily

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"
)

// Replay the canonical replay-equivalence corpus against replay.go
// (`#lzreplaygo`).
//
// Three fixtures, one obligation each (lazily-spec/docs/replay-equivalence.md):
// the fingerprint is bound to its log and that binding is revalidated before any
// value compare; a divergence is reported at the first checkpoint where the
// values parted; the observation encoding agrees with the family on which
// differences are differences.
//
// The corpus declares its subjects in PROSE, because a JSON fixture cannot carry
// a reactive graph. replayAccumulator below is this binding's copy of that
// declaration, kept to the letter — including that Observe exposes `sum` and
// `names` under exactly those labels.

const (
	replayLogBindingFixture   = "fingerprint_log_binding.json"
	replayDivergenceFixture   = "divergence_localization.json"
	replayEncodingFixture     = "canonical_encoding_equality.json"
	replayHarnessInputExcuse  = "replay input: it selects/drives the harness call below rather than stating an outcome"
	replayExpectedBlockExcuse = "container: the step's outcome is asserted key-by-key against this block below"
)

func replaySpecDir() string { return specPath("replay") }

// loadReplayFixture returns (fixture, true) when the sibling corpus is
// reachable, so an absent spec tree skips rather than fails (the same `present()`
// guard every other runner here uses).
func loadReplayFixture(t *testing.T, name string) (map[string]any, bool) {
	t.Helper()
	data, err := specReadFile(filepath.Join(replaySpecDir(), name))
	if err != nil {
		return nil, false
	}
	var fixture map[string]any
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return fixture, true
}

// ---------------------------------------------------------------------------
// the corpus's canonical subjects
// ---------------------------------------------------------------------------

// replayAccumulator is the corpus's `accumulator`, and its
// `drifting_accumulator` when a drift is configured.
//
// `drifting_accumulator` stands in for the one thing a replay proof is looking
// for — a graph that takes a value from OUTSIDE its log — with drift=0 as the
// honest run and a non-zero drift as the defect.
type replayAccumulator struct {
	sum     int64
	names   []string
	driftAt int64
	drift   int64
	drifts  bool
}

func (a *replayAccumulator) Apply(event ReplayEvent) {
	a.sum += replayInt64(event.Payload)
	a.names = append(a.names, event.Name)
	if a.drifts && event.Seq == a.driftAt {
		a.sum += a.drift
	}
}

func (a *replayAccumulator) Observe() map[string]any {
	names := make([]string, len(a.names))
	copy(names, a.names)
	return map[string]any{"sum": a.sum, "names": names}
}

// replayInt64 reads an integer that arrived through JSON as a float64.
func replayInt64(value any) int64 {
	switch n := value.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	}
	panic(fmt.Sprintf("replay conformance: %T is not an integer payload", value))
}

// replayBuilder resolves the fixture's declared subject to a build function.
// Each call must return a FRESH graph: a harness that reuses one instance proves
// nothing, since the state it would compare against is the state it already has.
func replayBuilder(t *testing.T, config, op map[string]any) func() ReplayGraph {
	t.Helper()
	switch subject := jsStr(config["subject"]); subject {
	case "accumulator":
		return func() ReplayGraph { return &replayAccumulator{} }
	case "drifting_accumulator":
		driftAt := int64(jsInt(config["drift_at"]))
		drift := int64(0)
		if raw, present := op["drift"]; present {
			drift = replayInt64(raw)
		}
		return func() ReplayGraph {
			return &replayAccumulator{driftAt: driftAt, drift: drift, drifts: true}
		}
	default:
		t.Fatalf("unknown canonical replay subject %q", subject)
		return nil
	}
}

// replayFinalSum drives a fresh subject over the whole log OUTSIDE the harness,
// so the fixture's `final_sum` is compared against the declared state machine
// rather than against the harness's own opinion of it.
func replayFinalSum(build func() ReplayGraph, log *ReplayLog) int64 {
	subject := build()
	for i := 0; i < log.Len(); i++ {
		subject.Apply(log.Event(i))
	}
	return subject.Observe()["sum"].(int64)
}

func replayLogOf(t *testing.T, where string, entries []any) *ReplayLog {
	t.Helper()
	events := make([]ReplayEvent, len(entries))
	for i, raw := range entries {
		entry := jsMap(raw)
		events[i] = ReplayEvent{
			Seq:     int64(jsInt(entry["seq"])),
			Name:    jsStr(entry["name"]),
			Payload: replayInt64(entry["payload"]),
		}
	}
	log, err := NewReplayLog(events...)
	if err != nil {
		t.Fatalf("%s: %v", where, err)
	}
	return log
}

// ---------------------------------------------------------------------------
// obligations 1 and 2
// ---------------------------------------------------------------------------

// replayExpectedKeys is the union of the assertion keys the two harness fixtures
// carry. A key present in a block and absent from this list fails consumeKeys; a
// key in this list and absent from the block is not a claim about that block.
var replayExpectedKeys = []string{
	"outcome", "checkpoint_seqs", "final_sum", "stride", "divergences",
	"first_divergent_seq", "first_divergent_label", "first_divergent_kind", "note",
}

func driveReplayHarnessFixture(t *testing.T, name string, minimumSteps int) {
	t.Helper()
	fixture, ok := loadReplayFixture(t, name)
	if !ok {
		t.Skip("lazily-spec conformance/replay not reachable")
	}
	if kind := jsStr(fixture["kind"]); kind != "Replay" {
		t.Fatalf("%s: fixture kind = %q, want Replay", name, kind)
	}
	if model := jsStr(fixture["model"]); model != "ReplayHarness" {
		t.Fatalf("%s: fixture model = %q, want ReplayHarness", name, model)
	}

	consumeFixtureKeys(t, name, fixture, "config", "steps")
	excuseKey(t, fixture, "config", replayHarnessInputExcuse)
	excuseKey(t, fixture, "steps", "replay input: the step list drives the loop below, and each step's own `expected` block is asserted there")

	config := consumeKeys(t, name+" config", jsMap(fixture["config"]),
		"subject", "stride", "logs", "drift_at")
	excuseKeys(t, config, replayHarnessInputExcuse, "subject", "drift_at", "logs")
	excuseKey(t, config, "stride", "replay input: the default checkpoint stride the harness samples at; every `record` step asserts the stride the resulting fingerprint carries")

	logs := map[string]*ReplayLog{}
	for key, raw := range jsMap(config["logs"]) {
		logs[key] = replayLogOf(t, name+" log "+key, jsList(raw))
	}
	defaultStride := 1
	if raw, present := config["stride"]; present {
		defaultStride = jsInt(raw)
	}

	steps := jsList(fixture["steps"])
	if len(steps) < minimumSteps {
		t.Fatalf("%s: %d steps, want at least %d — the corpus shrank or the runner is reading the wrong file",
			name, len(steps), minimumSteps)
	}

	fingerprints := map[string]*ReplayFingerprint{}
	for index, rawStep := range steps {
		op := jsMap(jsMap(rawStep)["op"])
		where := fmt.Sprintf("%s step %d (%s)", name, index, jsStr(op["type"]))
		step := consumeKeys(t, where, jsMap(rawStep), "op", "returns", "expected")
		excuseKey(t, step, "op", replayHarnessInputExcuse)
		excuseKey(t, step, "expected", replayExpectedBlockExcuse)
		expected := consumeKeys(t, where+" expected", jsMap(step["expected"]), replayExpectedKeys...)

		if jsStr(op["type"]) == "log_digest_equal" {
			// Two logs that settle to the same final sum must still have
			// different digests: event ORDER is part of the log.
			equal := logs[jsStr(op["left"])].Digest() == logs[jsStr(op["right"])].Digest()
			assertKey(t, step, "returns", equal)
			continue
		}

		build := replayBuilder(t, config, op)
		stride := defaultStride
		if raw, present := op["stride"]; present {
			stride = jsInt(raw)
		}
		harness, err := NewReplayHarness(build, ReplayWithStride(stride))
		if err != nil {
			t.Fatalf("%s: %v", where, err)
		}
		log := logs[jsStr(op["log"])]

		switch opType := jsStr(op["type"]); opType {
		case "record":
			fingerprint, err := harness.Record(log)
			if err != nil {
				t.Fatalf("%s: %v", where, err)
			}
			fingerprints[jsStr(op["into"])] = fingerprint
			assertKey(t, expected, "outcome", "recorded")
			assertKey(t, expected, "checkpoint_seqs", fingerprint.CheckpointSeqs())
			assertKey(t, expected, "stride", fingerprint.Stride())
			finalSum := replayFinalSum(build, log)
			assertKey(t, expected, "final_sum", finalSum)
			// The fingerprint must have observed the value the subject ENDS ON,
			// not merely some value. This is the one place the recorded digest
			// and the declared state meet: without it the fixture would accept a
			// harness that observed something else entirely and still recorded a
			// well-formed, log-bound, correctly-strided fingerprint.
			wantDigest, err := ReplayCanonicalDigest(finalSum)
			if err != nil {
				t.Fatalf("%s: %v", where, err)
			}
			if got := fingerprint.Final().Digests()["sum"]; got != wantDigest {
				t.Fatalf("%s: the recorded `sum` digest %s is not the digest of the subject's final sum %d (%s)",
					where, got, finalSum, wantDigest)
			}

		case "prove":
			if _, err := harness.Prove(log, jsInt(op["replays"])); err != nil {
				t.Fatalf("%s: %v", where, err)
			}
			assertKey(t, expected, "outcome", "ok")
			assertKey(t, expected, "divergences", 0)

		case "verify":
			outcome, divergences := "ok", 0
			var first ReplayDivergence
			var localized bool
			var logMismatch *ReplayLogMismatchError
			var strideMismatch *ReplayStrideMismatchError
			var divergence *ReplayDivergenceError
			_, err := harness.Verify(log, fingerprints[jsStr(op["fingerprint"])])
			switch {
			case err == nil:
			case errors.As(err, &logMismatch):
				// Routed on the TYPE, not on a message: a driver that matched on
				// the text would break the moment the message was reworded, and
				// could not tell a stale log from a stale stride.
				outcome = "log_mismatch"
			case errors.As(err, &strideMismatch):
				outcome = "stride_mismatch"
			case errors.As(err, &divergence):
				outcome, first, localized = "divergent", divergence.First(), true
			default:
				t.Fatalf("%s: %v", where, err)
			}
			assertKey(t, expected, "outcome", outcome)
			if !localized {
				assertKey(t, expected, "divergences", divergences)
				continue
			}
			assertKey(t, expected, "first_divergent_seq", first.Seq)
			assertKey(t, expected, "first_divergent_label", first.Label)
			assertKey(t, expected, "first_divergent_kind", first.Kind)

		case "check":
			divergences, err := harness.Check(log, fingerprints[jsStr(op["fingerprint"])])
			var logMismatch *ReplayLogMismatchError
			if errors.As(err, &logMismatch) {
				// The non-raising reporting form refuses a stale fingerprint
				// too: an unanswerable question is not a report.
				assertKey(t, expected, "outcome", "log_mismatch")
				assertKey(t, expected, "divergences", 0)
				continue
			}
			if err != nil {
				t.Fatalf("%s: %v", where, err)
			}
			assertKey(t, expected, "outcome", "ok")
			assertKey(t, expected, "divergences", len(divergences))

		default:
			t.Fatalf("%s: unknown canonical replay operation %q", where, opType)
		}
	}
}

func TestReplayFingerprintLogBindingConformance(t *testing.T) {
	driveReplayHarnessFixture(t, replayLogBindingFixture, 8)
}

func TestReplayDivergenceLocalizationConformance(t *testing.T) {
	driveReplayHarnessFixture(t, replayDivergenceFixture, 7)
}

// ---------------------------------------------------------------------------
// obligation 3
// ---------------------------------------------------------------------------

// replayOpaqueValue is the corpus's `opaque` tag: a value this encoding does not
// define. A channel is the honest choice — it has no value semantics at all, and
// fmt renders it as an ADDRESS, which is exactly the fallback the contract
// forbids.
func replayOpaqueValue() any { return make(chan struct{}) }

// replayTaggedValue builds the Go value a corpus value declaration names.
//
// The corpus tags every value because JSON cannot distinguish int 1 from float
// 1.0, and it carries integers as decimal STRINGS so a value beyond 2^53 stays
// exact.
func replayTaggedValue(t *testing.T, tagged any) any {
	t.Helper()
	declaration := jsMap(tagged)
	switch tag := jsStr(declaration["t"]); tag {
	case "int":
		n, err := strconv.ParseInt(jsStr(declaration["v"]), 10, 64)
		if err != nil {
			t.Fatalf("canonical value: %v", err)
		}
		return n
	case "str":
		return jsStr(declaration["v"])
	case "float":
		f, err := strconv.ParseFloat(jsStr(declaration["v"]), 64)
		if err != nil {
			t.Fatalf("canonical value: %v", err)
		}
		return f
	case "bool":
		return declaration["v"].(bool)
	case "bytes":
		raw, err := hex.DecodeString(jsStr(declaration["v"]))
		if err != nil {
			t.Fatalf("canonical value: %v", err)
		}
		return raw
	case "seq":
		out := []any{}
		for _, item := range jsList(declaration["v"]) {
			out = append(out, replayTaggedValue(t, item))
		}
		return out
	case "set":
		out := ReplaySet{}
		for _, item := range jsList(declaration["v"]) {
			out = append(out, replayTaggedValue(t, item))
		}
		return out
	case "map":
		out := map[string]any{}
		for _, entry := range jsList(declaration["v"]) {
			pair := jsList(entry)
			out[jsStr(pair[0])] = replayTaggedValue(t, pair[1])
		}
		return out
	case "opaque":
		return replayOpaqueValue()
	default:
		t.Fatalf("unknown canonical value tag %q", tag)
		return nil
	}
}

func replayValueDigest(t *testing.T, values map[string]any, key string) string {
	t.Helper()
	digest, err := ReplayCanonicalDigest(replayTaggedValue(t, values[key]))
	if err != nil {
		t.Fatalf("canonical digest of %q: %v", key, err)
	}
	return digest
}

func TestReplayCanonicalEncodingEqualityConformance(t *testing.T) {
	name := replayEncodingFixture
	fixture, ok := loadReplayFixture(t, name)
	if !ok {
		t.Skip("lazily-spec conformance/replay not reachable")
	}
	if kind := jsStr(fixture["kind"]); kind != "Replay" {
		t.Fatalf("%s: fixture kind = %q, want Replay", name, kind)
	}
	if model := jsStr(fixture["model"]); model != "CanonicalEncoding" {
		t.Fatalf("%s: fixture model = %q, want CanonicalEncoding", name, model)
	}

	consumeFixtureKeys(t, name, fixture, "config", "steps")
	excuseKey(t, fixture, "config", replayHarnessInputExcuse)
	excuseKey(t, fixture, "steps", "replay input: the step list drives the loop below, and each step's own `expected` block is asserted there")
	config := consumeKeys(t, name+" config", jsMap(fixture["config"]), "values")
	excuseKey(t, config, "values", replayHarnessInputExcuse)
	values := jsMap(config["values"])

	steps := jsList(fixture["steps"])
	// 14 steps: the eleven original equality classes plus the three member-length
	// rows lazily-spec 4010d99 added (#lzreplayframing). Pinned at what a clone
	// of the PUBLISHED corpus carries, exactly — a smaller number would let a
	// corpus that quietly dropped the length rows still report green here.
	if len(steps) < 14 {
		t.Fatalf("%s: %d steps, want at least 14 — the corpus shrank or the runner is reading the wrong file",
			name, len(steps))
	}

	// A runner that only ever saw `false` would pass every inequality claim with
	// a wholly broken encoding, so both outcomes must really occur.
	outcomes := map[bool]bool{}
	for index, rawStep := range steps {
		op := jsMap(jsMap(rawStep)["op"])
		where := fmt.Sprintf("%s step %d (%s)", name, index, jsStr(op["type"]))
		step := consumeKeys(t, where, jsMap(rawStep), "op", "returns", "expected")
		excuseKey(t, step, "op", replayHarnessInputExcuse)
		excuseKey(t, step, "expected", replayExpectedBlockExcuse)
		expected := consumeKeys(t, where+" expected", jsMap(step["expected"]), "outcome", "note")

		switch opType := jsStr(op["type"]); opType {
		case "digest_equal":
			// The fixture never names a hex digest — a binding's choice of hash
			// stays free — so every step asks only whether two declared values
			// digest the SAME.
			equal := replayValueDigest(t, values, jsStr(op["left"])) == replayValueDigest(t, values, jsStr(op["right"]))
			assertKey(t, step, "returns", equal)
			outcomes[equal] = true

		case "digest_defined":
			_, err := ReplayCanonicalDigest(replayTaggedValue(t, values[jsStr(op["value"])]))
			var encoding *ReplayEncodingError
			defined := true
			if errors.As(err, &encoding) {
				defined = false
			} else if err != nil {
				t.Fatalf("%s: %v", where, err)
			}
			assertKey(t, step, "returns", defined)
			assertKey(t, expected, "outcome", "encoding_error")

		default:
			t.Fatalf("%s: unknown canonical encoding operation %q", where, opType)
		}
	}
	if !outcomes[true] || !outcomes[false] {
		t.Fatalf("%s: the run observed only %v — a runner that never sees both outcomes proves nothing about the encoding",
			name, outcomes)
	}
}
