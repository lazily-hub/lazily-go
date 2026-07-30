package lazily

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"testing"
)

// Unconsumed-assertion-key guard (#lzassertunknownkeys).
//
// A conformance runner that silently ignores an assertion key reports a fixture
// as replayed while never checking the field the fixture exists for: the fixture
// round-trips, the suite goes green, and the assertion proves nothing. That is a
// level below "was the fixture opened" (which conformance_manifest_test.go
// already answers) — it is "having opened it, was the assertion consumed".
//
// Go makes this failure mode the *default*. `json.Unmarshal` into a struct drops
// any object key that has no matching field, with no error and no diagnostic. So
// a runner can look exhaustive — a named field per assertion, a `default:` arm
// on every op switch — while never seeing the key at all. lazily-kt's
// `assertAssertions` had the map-shaped version of the same hole: a
// `delta_zero_copy_arrow.json` `backend` discriminator that fell through
// unmatched, so the fixture would have passed while never testing the one thing
// it exists for.
//
// The seam is `json.Decoder.DisallowUnknownFields`: decoding an object key with
// no destination field becomes a hard error naming the key. Wrapping it with the
// fixture label names both halves of the failure — which fixture, which key.
//
// Consequences that are deliberate, not accidental:
//
//   - Every struct field declared here is a promise that the runner reads it.
//     Adding a field to silence a decode failure re-opens the hole in a quieter
//     form, so a field is only added together with the assertion that consumes
//     it. Fixture prose (`description`, `notes`) is the one exception, and is
//     modelled by conformanceDoc so it reads as documentation rather than as an
//     unchecked assertion.
//   - A corpus key that no binding implements now fails this binding loudly
//     instead of being invisibly skipped in all nine at once.
//
// Runners that already decode assertions into `map[string]json.RawMessage` and
// `t.Fatalf` on an unmatched key (the IPC conformance suite) already fail closed
// and are left alone.

// conformanceDoc carries the prose keys the shared corpus attaches to fixtures
// and scenarios. They are documentation, not assertions: embedding this is how a
// runner says "these keys are known and deliberately not asserted", which keeps
// the strict decode from conflating prose with an unchecked expectation.
type conformanceDoc struct {
	Description string          `json:"description"`
	Notes       json.RawMessage `json:"notes"`
	Note        json.RawMessage `json:"note"`
	Comment     string          `json:"comment"`
}

// conformanceMeta adds the corpus's own routing labels to the prose keys: `kind`
// names the fixture family and `model` names the primitive under test. They pick
// which runner replays a fixture rather than stating anything the replay must
// observe, so — like the prose keys — they are declared here to keep the strict
// decode from mistaking taxonomy for an unchecked expectation.
type conformanceMeta struct {
	conformanceDoc
	Kind  string `json:"kind"`
	Model string `json:"model"`
}

// strictJSON decodes data into v, rejecting any key that v does not model.
//
// label should identify the fixture and the block being decoded (for example
// `delta_zero_copy_arrow.json assertions`) so the error names the fixture as well
// as the offending key.
func strictJSON(label string, data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%s: %w (an assertion key the runner does not consume is a silently skipped assertion — implement it, do not delete it)", label, err)
	}
	// A second value in the stream means the fixture is not the single document
	// it claims to be; that is drift worth failing on too.
	if err := dec.Decode(new(json.RawMessage)); err != io.EOF {
		return fmt.Errorf("%s: trailing content after the JSON document", label)
	}
	return nil
}

// mustStrictJSON is strictJSON with the t.Fatalf that every call site would
// otherwise write by hand.
func mustStrictJSON(t *testing.T, label string, data []byte, v any) {
	t.Helper()
	if err := strictJSON(label, data, v); err != nil {
		t.Fatalf("%v", err)
	}
}

// consumeKeys is the map-shaped counterpart of DisallowUnknownFields, for the
// runners that decode fixtures into `map[string]any` and read them by name. It
// fails when the block carries a key the runner does not consume, naming both the
// key and the block.
//
// `consumed` is the list of keys the surrounding code reads, and it carries the
// same promise a struct field does: adding a name here without adding the read
// that goes with it re-opens the hole. It returns the block so a call site reads
// as `expected := consumeKeys(t, label, jsMap(step["expected"]), ...)` and the
// declaration sits directly above the reads it licenses.
func consumeKeys(t *testing.T, label string, obj map[string]any, consumed ...string) map[string]any {
	t.Helper()
	if extra := unconsumedKeys(obj, consumed); len(extra) > 0 {
		t.Fatalf("%s: assertion key(s) %v are present in the fixture and consumed by nothing "+
			"(the runner reads only %v) — implement them, do not delete them", label, extra, consumed)
	}
	return obj
}

// conformanceMetaKeys is the map-shaped conformanceMeta: the prose and taxonomy
// keys every fixture root may carry.
var conformanceMetaKeys = []string{"description", "notes", "note", "comment", "kind", "model"}

// consumeFixtureKeys is consumeKeys for a fixture root, which additionally
// tolerates the corpus-wide prose and taxonomy keys.
func consumeFixtureKeys(t *testing.T, label string, obj map[string]any, consumed ...string) map[string]any {
	t.Helper()
	return consumeKeys(t, label, obj, append(append([]string{}, conformanceMetaKeys...), consumed...)...)
}

// unconsumedKeys is the decision consumeKeys reports, split out so it can be
// tested without a t whose Fatalf would abort the caller.
func unconsumedKeys(obj map[string]any, consumed []string) []string {
	known := make(map[string]bool, len(consumed))
	for _, k := range consumed {
		known[k] = true
	}
	var extra []string
	for k := range obj {
		if !known[k] {
			extra = append(extra, k)
		}
	}
	sort.Strings(extra)
	return extra
}

// TestConsumeKeysRejectsUnconsumedKey is the map-shaped half of the guard's own
// mutation check.
func TestConsumeKeysRejectsUnconsumedKey(t *testing.T) {
	extra := unconsumedKeys(map[string]any{"held": true, "backend": "arrow"}, []string{"held"})
	if !reflect.DeepEqual(extra, []string{"backend"}) {
		t.Fatalf("unconsumed key not reported: %v", extra)
	}
	if extra := unconsumedKeys(map[string]any{"held": true}, []string{"held", "fence"}); len(extra) != 0 {
		t.Fatalf("consumed key reported as extra: %v", extra)
	}
	if extra := unconsumedKeys(nil, []string{"held"}); len(extra) != 0 {
		t.Fatalf("nil block reported extras: %v", extra)
	}
}

// TestStrictJSONRejectsUnconsumedKey is the mutation check for the guard itself:
// without it, the bogus key below decodes clean and the assertion it displaces is
// never noticed.
func TestStrictJSONRejectsUnconsumedKey(t *testing.T) {
	type expected struct {
		Held bool `json:"held"`
	}

	var ok expected
	if err := strictJSON("fake.json expected", []byte(`{"held":true}`), &ok); err != nil {
		t.Fatalf("consumed key rejected: %v", err)
	}
	if !ok.Held {
		t.Fatal("held not decoded")
	}

	var bad expected
	err := strictJSON("fake.json expected", []byte(`{"held":true,"backend":"arrow"}`), &bad)
	if err == nil {
		t.Fatal("unconsumed key accepted")
	}
	if !bytes.Contains([]byte(err.Error()), []byte(`"backend"`)) {
		t.Fatalf("error does not name the offending key: %v", err)
	}
	if !bytes.Contains([]byte(err.Error()), []byte("fake.json")) {
		t.Fatalf("error does not name the fixture: %v", err)
	}

	var trailing expected
	if err := strictJSON("fake.json expected", []byte(`{"held":true} {"held":false}`), &trailing); err == nil {
		t.Fatal("trailing document accepted")
	}
}
