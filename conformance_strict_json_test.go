package lazily

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
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
// The Go field names are deliberately prefixed. TestConformanceStructFieldsAreRead
// resolves reads by NAME, not by type, so a field called `Note` here would be
// reported as read the moment any unrelated struct in the package grew a `Note`
// of its own. A name no other struct can plausibly claim keeps that scan exact
// for the fields whose whole point is that nothing reads them.
type conformanceDoc struct {
	ProseDescription string          `json:"description"`
	ProseNotes       json.RawMessage `json:"notes"`
	ProseNote        json.RawMessage `json:"note"`
	ProseComment     string          `json:"comment"`
}

// conformanceMeta adds the corpus's own routing labels to the prose keys: `kind`
// names the fixture family and `model` names the primitive under test. They pick
// which runner replays a fixture rather than stating anything the replay must
// observe, so — like the prose keys — they are declared here to keep the strict
// decode from mistaking taxonomy for an unchecked expectation.
type conformanceMeta struct {
	conformanceDoc
	MetaKind string `json:"kind"`
	Model    string `json:"model"`
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
	// Rung 3: having declared these keys as read, the runner now owes a
	// disposition for each one. See trackAssertions.
	trackAssertions(t, label, obj, consumed)
	return obj
}

// conformanceMetaKeys is the map-shaped conformanceMeta: the prose and taxonomy
// keys every fixture root may carry.
// `generator` is root-only provenance owned by lazily-spec; it is deliberately
// absent from annotationKeys so it cannot be hidden inside an assertion block.
var conformanceMetaKeys = []string{"description", "notes", "note", "comment", "kind", "model", "generator"}

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

// ---------------------------------------------------------------------------
// Rung 3: a key that was READ is not thereby ASSERTED (#lzconsumednotasserted)
// ---------------------------------------------------------------------------
//
// consumeKeys above proves consumption: every key the fixture carries is named
// in the list the runner declares it reads. It does not prove assertion. A
// runner can name a key, read it, and then do nothing with it. Three shapes:
//
//  1. a named skip inside a consuming loop (`if id == "sibling_a_cached" {
//     continue }`) — the read marks the key consumed and the comparison never
//     happens;
//  2. a value bound but never compared — in Go the compiler catches an unused
//     local, but NOT a decoded struct field, so this shape hides in the
//     strictJSON runners (see TestConformanceStructFieldsAreRead below);
//  3. a comparison against a hardcoded literal rather than the fixture's value,
//     so editing the fixture changes nothing.
//
// The tracker therefore records a second set beside the declared read set: the
// keys that reached a comparison against the fixture's OWN value. A key becomes
// asserted only by going through assertKey / assertKeyWith, so an arm that
// compares against a literal never marks it. Verification runs at test teardown
// and fails on three conditions:
//
//   - a key present in the fixture that no runner named — consumeKeys, above;
//   - a key that was read but never asserted — assertionBlock.verify;
//   - a stale excuse: a key that is excused AND asserted in the same run, which
//     means the excuse has gone stale and is now hiding nothing. This is the
//     same both-directions rule the KNOWN_UNCOVERED allowlist carries.
//
// Where a key genuinely cannot be asserted at a call site — it is replay input,
// a discriminator that selects a code path, or a value folded into an expected
// object compared elsewhere — the runner says so out loud with excuseKey and a
// reason. Excusing is the fallback; implementing the assertion is preferred.

// assertionBlock is the per-block bookkeeping consumeKeys opens: what the runner
// declared it reads, what it actually asserted, and what it excused.
type assertionBlock struct {
	label    string
	obj      map[string]any
	declared []string

	mu       sync.Mutex
	asserted map[string]bool
	excused  map[string]string
	// keySetChecked records the object-valued keys that reached a KEY-SET
	// comparison rather than a hand-picked list of sub-fields
	// (#lzsubblockkeyset). See the rung-6 section below.
	keySetChecked map[string]bool
}

var (
	assertionBlocksMu sync.Mutex
	// Keyed by the block map's runtime pointer: the runners hand the raw map
	// around (`expected["order"]`), so identity is the only handle available.
	// The record holds the map, which keeps it alive until the cleanup that
	// removes the record, so an address is never reused under a live entry.
	assertionBlocks = map[uintptr][]*assertionBlock{}
)

// annotationKeys are the RESERVED ANNOTATION NAMES (#lzprosekeyconvention): a
// key by one of these names inside a per-step or per-scenario block annotates
// the block rather than stating anything the replay must observe, and is exempt
// from all three conditions above.
//
// This is exemption BY NAME, and it is deliberately narrower than "any English
// paragraph". A paragraph that states an OBLIGATION is a prose key, is declared
// as such by the corpus in `assertions.prose`, and must be DISCHARGED — see the
// rung-5 tracker below, whose demand for a discharge is driven by the corpus
// declaration and therefore overrides this table.
var annotationKeys = map[string]bool{
	"description": true, "notes": true, "note": true, "comment": true,
	"why": true, "kind": true, "model": true, "generator": true,
}

func assertionBlockID(obj map[string]any) (uintptr, bool) {
	if obj == nil {
		return 0, false
	}
	return reflect.ValueOf(obj).Pointer(), true
}

// trackAssertions opens the disposition ledger for a block and schedules its
// verification for test teardown.
func trackAssertions(t *testing.T, label string, obj map[string]any, declared []string) {
	t.Helper()
	id, ok := assertionBlockID(obj)
	if !ok {
		return
	}
	blk := &assertionBlock{
		label:         label,
		obj:           obj,
		declared:      append([]string{}, declared...),
		asserted:      map[string]bool{},
		excused:       map[string]string{},
		keySetChecked: map[string]bool{},
	}
	assertionBlocksMu.Lock()
	assertionBlocks[id] = append(assertionBlocks[id], blk)
	assertionBlocksMu.Unlock()
	// Rung 5 opens here, on the FIRST tracked block of the replay, so that a key
	// asserted before the `prose`-carrying block is consumed still lands in the
	// fixture-scoped asserted set a discharge is checked against.
	ledger := openProseLedger(t)
	ledger.recordCarried(obj)
	if _, declares := obj["prose"]; declares {
		ledger.declareBlock(t, label, obj, blk)
	}
	t.Cleanup(func() {
		assertionBlocksMu.Lock()
		stack := assertionBlocks[id]
		for i := len(stack) - 1; i >= 0; i-- {
			if stack[i] == blk {
				assertionBlocks[id] = append(stack[:i:i], stack[i+1:]...)
				break
			}
		}
		if len(assertionBlocks[id]) == 0 {
			delete(assertionBlocks, id)
		}
		assertionBlocksMu.Unlock()
		blk.verify(t)
	})
}

// lookupAssertionBlock finds the ledger a block map was registered under, or nil
// when the caller is asserting against an untracked map.
func lookupAssertionBlock(obj map[string]any) *assertionBlock {
	id, ok := assertionBlockID(obj)
	if !ok {
		return nil
	}
	assertionBlocksMu.Lock()
	defer assertionBlocksMu.Unlock()
	stack := assertionBlocks[id]
	if len(stack) == 0 {
		return nil
	}
	return stack[len(stack)-1]
}

func (b *assertionBlock) markAsserted(key string) {
	b.mu.Lock()
	b.asserted[key] = true
	b.mu.Unlock()
}

func (b *assertionBlock) verify(t *testing.T) {
	for _, problem := range b.problems() {
		t.Errorf("%s", problem)
	}
}

// problems is the decision verify reports, split out so it can be tested
// without a t whose Errorf would redden the caller.
func (b *assertionBlock) problems() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []string
	// THE CORPUS DECLARATION IS EVALUATED ON THE RAW BLOCK, BEFORE ANY NAME-BASED
	// EXEMPTION (#lzprosekeyconvention). A tracker that subtracts its reserved-name
	// set first makes the declaration invisible: `note` is a reserved annotation
	// name AND a declared prose key in both frame-codec fixtures, so it would be
	// exempt from the unread guard, exempt from the unasserted guard, and never
	// discharged — the fixture skips the whole convention while the binding still
	// reports conforming. Two of the nine bindings hit exactly this.
	//
	// Inside a block that declares `prose` the name exemption is off ENTIRELY: the
	// corpus wins, so a `note` sitting in a declaring block but absent from its
	// array needs an assertion or an excuse like any other key.
	declaresAnyProse := len(proseDeclaration(b.obj)) > 0
	for _, key := range b.declared {
		// A key the corpus declares prose is DISCHARGED, never asserted and
		// never excused. Its disposition is the rung-5 tracker's business, and
		// asserting or excusing it is a failure reported THERE.
		if b.declaresProse(key) {
			continue
		}
		if annotationKeys[key] && !declaresAnyProse {
			continue
		}
		if _, present := b.obj[key]; !present {
			// A key the fixture does not carry cannot be asserted; the runner
			// declaring it read-if-present is not a claim about this fixture.
			continue
		}
		reason, excused := b.excused[key]
		// Rung 6: a key whose fixture VALUE is a JSON object owes a check of its
		// KEY SET, not of five sub-fields somebody remembered (#lzsubblockkeyset).
		_, objectValued := b.obj[key].(map[string]any)
		switch {
		case b.asserted[key] && excused:
			out = append(out, fmt.Sprintf("%s: key %q is both asserted and excused (%q) — the excuse has gone stale and now hides nothing; delete it",
				b.label, key, reason))
		case excused && strings.TrimSpace(reason) == "":
			out = append(out, fmt.Sprintf("%s: key %q is excused with an empty reason — an excuse without a reason is a silent skip", b.label, key))
		case b.asserted[key] && objectValued && !b.keySetChecked[key]:
			out = append(out, fmt.Sprintf("%s: object-valued key %q was consumed without a key-set check — the sub-keys "+
				"beneath it are compared by nothing, so a field added upstream lands unasserted with the suite still green. "+
				"Descend into it with assertKeySub, walk every sub-key with assertKeyEach, or compare its KEY SET "+
				"against what the run produced with assertKeySet (#lzsubblockkeyset)", b.label, key))
		case b.asserted[key] || excused:
			// dispositioned
		default:
			out = append(out, fmt.Sprintf("%s: key %q is read by the runner but never reaches a comparison against the fixture's own value "+
				"— assert it with assertKey/assertKeyWith, or say why it cannot be asserted here with excuseKey", b.label, key))
		}
	}
	return out
}

// declaresProse reports whether the block's own `assertions.prose` array names
// key. The corpus decides which keys are prose; a binding that decided for
// itself is how one rule got four treatments (#lzprosekeyconvention).
func (b *assertionBlock) declaresProse(key string) bool {
	for _, name := range proseDeclaration(b.obj) {
		if name == key {
			return true
		}
	}
	return false
}

// proseDeclaration reads a block's `prose` array. A block that carries no
// `prose` key declares nothing.
func proseDeclaration(obj map[string]any) []string {
	raw, present := obj["prose"]
	if !present {
		return nil
	}
	list, ok := raw.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(list))
	for _, entry := range list {
		if name, ok := entry.(string); ok {
			out = append(out, name)
		}
	}
	return out
}

// assertKey compares actual against the fixture's value for key and marks the
// key asserted. This is the one path by which a key becomes asserted, so a
// comparison written against a hardcoded literal never satisfies the guard.
func assertKey(t *testing.T, block map[string]any, key string, actual any) {
	t.Helper()
	assertKeyWith(t, block, key, func(want any) {
		t.Helper()
		if !jsonValueEqual(want, actual) {
			t.Errorf("%s: %s = %v, want %v (the fixture's value)", assertionLabel(block), key, actual, want)
		}
		// An object compared WHOLE has had its key set compared: jsonEquivalent
		// rejects a size difference before it looks at a single value, so a
		// sub-key added upstream reddens here without anyone naming it. That is
		// the rung-6 obligation discharged by construction, so record it
		// (#lzsubblockkeyset). The shape the rung exists to catch is the
		// hand-picked sub-field walk inside an assertKeyWith callback, which
		// never sees the sixth field at all.
		if _, objectValued := want.(map[string]any); objectValued {
			if blk := lookupAssertionBlock(block); blk != nil {
				blk.markKeySetChecked(key)
			}
		}
	})
}

// assertKeyWith hands the fixture's own value to the caller's check and marks
// key asserted only after that check returns. It exists for comparisons that
// are not equality — a tolerance, a set containment, an invariant derived from
// the value. The point is that the fixture's value reaches the comparison, not
// that the comparison is `==`.
func assertKeyWith(t *testing.T, block map[string]any, key string, check func(want any)) {
	t.Helper()
	want, present := block[key]
	if !present {
		t.Errorf("%s: key %q is asserted but the fixture block does not carry it", assertionLabel(block), key)
		return
	}
	check(want)
	if blk := lookupAssertionBlock(block); blk != nil {
		blk.markAsserted(key)
	}
	// Rung 5: the fixture-scoped record a discharge claim is checked against.
	// By NAME and in any block, because an obligation stated in `assertions` is
	// routinely carried by a per-scenario `expect` key.
	if ledger := lookupProseLedger(t); ledger != nil {
		ledger.markAsserted(key)
	}
}

// excuseKey records that key cannot be asserted at this call site, and why. The
// reason must be non-empty and must say where the fact is proven instead, or why
// it is unprovable here. Excusing a key the same run also asserts is a failure.
func excuseKey(t *testing.T, block map[string]any, key, reason string) {
	t.Helper()
	if strings.TrimSpace(reason) == "" {
		t.Fatalf("%s: excuseKey(%q) needs a reason", assertionLabel(block), key)
	}
	blk := lookupAssertionBlock(block)
	if blk == nil {
		return
	}
	blk.mu.Lock()
	blk.excused[key] = reason
	blk.mu.Unlock()
}

// excuseKeys is excuseKey for the run of keys that share one reason — typically
// a block's replay input, which drives the run rather than stating an
// expectation.
func excuseKeys(t *testing.T, block map[string]any, reason string, keys ...string) {
	t.Helper()
	for _, key := range keys {
		excuseKey(t, block, key, reason)
	}
}

// ---------------------------------------------------------------------------
// Rung 6: an OBJECT-VALUED assertion key is checked by its KEY SET
// (#lzsubblockkeyset)
// ---------------------------------------------------------------------------
//
// Rung 2 (consumeKeys) proves that every key a BLOCK carries is named. It says
// nothing about the keys one level DOWN, inside an assertion key whose value is
// itself a JSON object. A runner that reads five named sub-fields of such a key
// is comparing the sixth against nothing: a field added upstream lands
// unasserted and the suite stays green. That is the null form of rung 2,
// relocated inside an assertion key instead of beside one — found by planting a
// key in `arena_blob.json`'s `descriptor` object, which reddened not one binding
// while every scalar sibling reddened all nine.
//
// The cheap fix is a per-call-site field count, and it is the wrong one: it
// relies on each site remembering, which is the same thing that failed. So the
// obligation lives in the TRACKER. An object-valued key must be consumed through
// one of exactly two entry points:
//
//	assertKeySub — DESCEND. The child object becomes a tracked block of its own,
//	   so an unrecognised sub-key fails exactly the way an unrecognised top-level
//	   key does, and every sub-key then owes its own assertion or excuse.
//	assertKeySet — KEY SET. The object's key set is compared, in BOTH directions,
//	   against the set the run really produced. A token the fixture declares and
//	   nothing replayed, and a token the run produced that the fixture omits, are
//	   both failures. This is the entry point for a VOCABULARY whose values are
//	   glosses rather than expectations.
//	assertKeyEach — WALK. The TRACKER drives the iteration and hands every sub-key
//	   the fixture carries to the caller's comparison, so a field added upstream
//	   reaches a check by construction. This is the entry point for the keyed
//	   expectation blocks — `expected.reads`, `expected.scopes`, `initial.values` —
//	   where the sub-keys are a per-entry expectation rather than a fixed schema.
//	   It is `assertKeySub` with the consumed list derived from the object instead
//	   of restated, which is exactly the restatement that rots.
//
// A fourth path discharges the obligation without a new entry point: plain
// assertKey on an object value compares the object WHOLE, and jsonEquivalent
// rejects a size difference before it compares a single field, so the key set is
// compared by construction there too.
//
// `assertionBlock.problems` above then fails any object-valued key that was
// asserted through neither. That guard is the point of the rung: a call site
// that reaches for plain `assertKey`/`assertKeyWith` on an object value gets a
// red suite instead of a silent hole, so the NEXT object-valued key the corpus
// grows cannot land unnoticed. The excuse and prose channels stay open for a key
// that genuinely carries no obligation — both already demand a recorded reason.

// assertKeySub consumes an object-valued key by DESCENDING into it. It marks the
// key asserted on the parent block, then opens a tracked block on the child, so
// `consumed` carries the same promise the parent's list does: a sub-key the
// fixture holds and this list omits fails, and a sub-key named here still owes an
// assertKey or an excuseKey of its own.
//
// A JSON `null` returns nil rather than failing: the corpus writes an absent
// sub-block that way, its key set is empty, and the caller still has to say what
// absence means. Anything else that is not an object is a fixture the runner
// cannot descend into and fails here.
func assertKeySub(t *testing.T, block map[string]any, key string, consumed ...string) map[string]any {
	t.Helper()
	label := assertionLabel(block)
	// Recorded BEFORE the comparison runs. A t.Fatalf inside the check aborts the
	// goroutine, and a rung-6 mark applied afterwards would never land — the
	// teardown would then report "consumed without a key-set check" on top of the
	// real failure and point at the wrong bug.
	if blk := lookupAssertionBlock(block); blk != nil {
		blk.markKeySetChecked(key)
	}
	var child map[string]any
	assertKeyWith(t, block, key, func(want any) {
		t.Helper()
		if want == nil {
			return
		}
		sub, ok := want.(map[string]any)
		if !ok {
			t.Fatalf("%s: key %q is %T, want a JSON object to descend into", label, key, want)
			return
		}
		child = sub
	})
	if child == nil {
		return nil
	}
	return consumeKeys(t, label+"."+key, child, consumed...)
}

// assertKeySet consumes an object-valued key by comparing its KEY SET against
// the set the run really produced — for a vocabulary, the tokens the replay loop
// really dispatched on. The comparison is set equality in both directions, so a
// declared token nothing replayed and a replayed token the fixture omits both
// fail.
//
// inspect, when non-nil, is called once per declared entry with its value, for
// the checks that are about the VALUES rather than the key set (a gloss that is
// present but blank, say). It is not what discharges the key.
func assertKeySet(t *testing.T, block map[string]any, key string, observed map[string]bool, inspect func(name string, value any)) {
	t.Helper()
	label := assertionLabel(block)
	if blk := lookupAssertionBlock(block); blk != nil {
		blk.markKeySetChecked(key)
	}
	assertKeyWith(t, block, key, func(want any) {
		t.Helper()
		sub, ok := want.(map[string]any)
		if !ok {
			t.Fatalf("%s: key %q is %T, want a JSON object whose KEY SET is the assertion", label, key, want)
			return
		}
		declared := make([]string, 0, len(sub))
		for name := range sub {
			declared = append(declared, name)
		}
		assertSameStringSet(t, label+"."+key, declared, observed)
		if inspect != nil {
			sort.Strings(declared)
			for _, name := range declared {
				inspect(name, sub[name])
			}
		}
	})
}

// assertKeyEach consumes an object-valued key by WALKING it: every sub-key the
// fixture carries is handed to check, in sorted order so a failure is the same
// one on every run. The iteration lives here rather than in the call site, which
// is what makes it a key-set check — a sub-key added upstream reaches the
// caller's comparison without anyone naming it, and the hand-picked walk that
// stops at five fields cannot be written through this entry point.
func assertKeyEach(t *testing.T, block map[string]any, key string, check func(name string, value any)) {
	t.Helper()
	label := assertionLabel(block)
	if blk := lookupAssertionBlock(block); blk != nil {
		blk.markKeySetChecked(key)
	}
	assertKeyWith(t, block, key, func(want any) {
		t.Helper()
		sub, ok := want.(map[string]any)
		if !ok {
			t.Fatalf("%s: key %q is %T, want a JSON object to walk", label, key, want)
			return
		}
		names := make([]string, 0, len(sub))
		for name := range sub {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			check(name, sub[name])
		}
	})
}

// markKeySetChecked records that key reached one of the two rung-6 entry points.
func (b *assertionBlock) markKeySetChecked(key string) {
	b.mu.Lock()
	if b.keySetChecked == nil {
		b.keySetChecked = map[string]bool{}
	}
	b.keySetChecked[key] = true
	b.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Rung 5: a PROSE key is DISCHARGED, never asserted and never excused
// (#lzprosekeyconvention)
// ---------------------------------------------------------------------------
//
// An `assertions` block mixes two kinds of key. Most carry a value a runner can
// compare against observed behaviour — a list, a count, a vocabulary. A few
// carry an English paragraph that states an obligation and nothing comparable.
// Rungs 3 and 4 have no answer for the second kind, and the nine bindings each
// invented one: lazily-go's was an `excuseKey` whose reason NAMES the assertion
// that discharges the paragraph — falsifiable in principle, checked by nothing.
//
// The corpus now declares which keys are prose, in `assertions.prose`. The
// declaration is itself a key of the block, so rung 2 sees it: a runner that
// ignores it fails with an unconsumed key, which is what makes the rollout
// self-enforcing.
//
// A prose key is discharged by NAMING the executable keys that carry its
// obligation, and this tracker verifies the naming. It fails the run when:
//
//	1. a declared prose key is ASSERTED — comparing a paragraph, or a tally
//	   derived from one, to an English string pins wording, not behaviour;
//	2. a declared prose key is EXCUSED with free text — an unfalsifiable reason
//	   is indistinguishable from the undocumented default this removes;
//	3. a key the block does NOT declare prose is discharged;
//	4. the discharged set differs from `assertions.prose` — the comparison that
//	   consumes `prose` itself, and what makes a forgotten key fail rather than
//	   vanish;
//	5. a discharge names NO keys;
//	6. a discharge names a key this fixture's run did not assert;
//	7. a discharge names a key that is itself prose.
//
// Rule 6 is the whole convention: the excuse becomes falsifiable, because the
// tracker can check it. "`epoch_disambiguation` is discharged by `frame_epoch`
// and `blob_epoch`" is a claim about the run; "`epoch_disambiguation` is prose"
// is not.
//
// The ledger is FIXTURE-scoped, not block-scoped: `epoch_disambiguation` is
// stated in `assertions` and discharged by `expect.frame_epoch` /
// `expect.blob_epoch`, asserted long after that block is finished. Verification
// therefore runs at fixture end, through `verifyProse(t, fixture)`, and a run
// that never verifies fails from the ledger's own teardown — an unverified
// discharge claim is as bad as an unconsumed key.

// ---------------------------------------------------------------------------
// Rule 8: a declaring fixture that never reaches verification
// ---------------------------------------------------------------------------
//
// Rules 1-7 are all satisfied over an EMPTY population: a fixture that is opened
// and then never replayed passes every one of them while proving nothing. That
// is the vacuity the corpus's own `anti_vacuity` keys exist to name, reappearing
// inside the guard meant to enforce them.
//
// The required set is therefore DERIVED FROM THE CORPUS — every fixture on disk
// whose `assertions` block declares a non-empty `prose` array — and never from a
// hand-kept list here. A count in this file would be a claim, and a claim rots;
// worse, a new prose fixture upstream would land silently instead of reddening
// this binding, which is the whole point of the rollout being self-enforcing.
// The required set is scoped to the fixtures this run actually OPENED, which is
// what makes the check compose rather than duplicate: `check-conformance-coverage.sh`
// already proves the corpus was opened, against its own explicit
// KNOWN_UNCOVERED list, so rule 8 only has to prove that an opened DECLARING
// fixture reached verification. It also keeps `go test -run` honest — a filtered
// run legitimately opens nothing else.
var (
	proseVerifiedMu sync.Mutex
	proseVerified   = map[string]bool{}
	proseOpened     = map[string]bool{}
)

func recordProseVerified(fixture string) {
	proseVerifiedMu.Lock()
	proseVerified[fixture] = true
	proseVerifiedMu.Unlock()
}

// recordProseOpened books a fixture read. Unlike the conformance manifest it
// also counts the VENDORED mirror, because a fixture replayed from the offline
// fallback owes its paragraphs a discharge exactly as the canonical one does.
// It attributes against every RESOLVED corpus root (#lzoverrideallrunners), so
// a run redirected by LAZILY_SPEC_CONFORMANCE_DIR keys prose the same way as one
// against the canonical checkout instead of quietly booking nothing.
func recordProseOpened(path string) {
	id, ok := specAnyRootRelative(path)
	if !ok {
		return
	}
	proseVerifiedMu.Lock()
	proseOpened[id] = true
	proseVerifiedMu.Unlock()
}

// proseDeclaringFixtures walks the conformance corpus and returns every fixture
// whose top-level `assertions` block declares prose, keyed the same way the
// runners name them (`codec/nodeid_exact_range.json`).
func proseDeclaringFixtures() []string {
	seen := map[string]bool{}
	for _, root := range specCorpusRoots() {
		_ = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".json") {
				return nil //nolint:nilerr // a corpus root that is absent contributes nothing
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return nil //nolint:nilerr
			}
			var fixture map[string]any
			if err := json.Unmarshal(data, &fixture); err != nil {
				return nil //nolint:nilerr
			}
			block, ok := fixture["assertions"].(map[string]any)
			if !ok || len(proseDeclaration(block)) == 0 {
				return nil
			}
			id, err := filepath.Rel(root, path)
			if err != nil {
				return nil //nolint:nilerr
			}
			seen[filepath.ToSlash(id)] = true
			return nil
		})
	}
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// unverifiedProseFixtures is the decision rule 8 reports, split out so it can be
// mutation-checked without the process-exit path TestMain takes.
func unverifiedProseFixtures() []string {
	proseVerifiedMu.Lock()
	verified := make(map[string]bool, len(proseVerified))
	for id := range proseVerified {
		verified[id] = true
	}
	opened := make(map[string]bool, len(proseOpened))
	for id := range proseOpened {
		opened[id] = true
	}
	proseVerifiedMu.Unlock()
	var missing []string
	for _, id := range proseDeclaringFixtures() {
		if opened[id] && !verified[id] {
			missing = append(missing, id)
		}
	}
	return missing
}

// checkProseVerificationCoverage is rule 8, run from TestMain after the suite
// finishes: only then is the union of verifications complete, and no ordering
// between test functions has to be assumed.
func checkProseVerificationCoverage() bool {
	missing := unverifiedProseFixtures()
	if len(missing) == 0 {
		return true
	}
	fmt.Fprintf(os.Stderr,
		"FAIL: %d conformance fixture(s) declare `assertions.prose` and never reached verifyProse: %v\n"+
			"  Rules 1-7 are all satisfied over an empty population, so a declaring fixture that is opened and\n"+
			"  never replayed — or replayed by a runner that forgot verifyProse — passes every one of them while\n"+
			"  proving nothing. Replay it and discharge its paragraphs with proseKey (#lzprosekeyconvention).\n",
		len(missing), missing)
	return false
}

// proseDischarge is one claim: this paragraph's obligation is carried by these
// executable keys, and this run asserted them.
type proseDischarge struct {
	label string
	block map[string]any
	key   string
	by    []string
}

// proseBlock is a block that declared `prose`, held alongside its rung-3 ledger
// so rules 1 and 2 can read what the run did with each declared key.
type proseBlock struct {
	label string
	obj   map[string]any
	blk   *assertionBlock
}

// proseLedger is the fixture-scoped record: every key name the run ASSERTED,
// every block that DECLARED prose, and every pending discharge CLAIM.
type proseLedger struct {
	mu       sync.Mutex
	fixture  string
	verified bool
	asserted map[string]bool
	// carried is every key name any tracked block of this fixture actually
	// holds. A discharge naming a key the fixture does not carry AT ALL has
	// rotted, exactly as a stale excuse has, and is a distinct failure from
	// naming a key that is carried but never asserted.
	carried    map[string]bool
	blocks     []proseBlock
	discharges []proseDischarge
}

var (
	proseLedgersMu sync.Mutex
	// Keyed by the ROOT test name: one fixture replay is one top-level test,
	// and a subtest's assertions belong to the same fixture as its parent's.
	proseLedgers = map[string]*proseLedger{}
)

func proseLedgerRoot(t *testing.T) string {
	name := t.Name()
	if i := strings.IndexByte(name, '/'); i >= 0 {
		return name[:i]
	}
	return name
}

// openProseLedger returns the replay's ledger, creating and arming it on first
// use. The teardown it registers is what fails a run that records discharge
// claims and never verifies them.
func openProseLedger(t *testing.T) *proseLedger {
	t.Helper()
	root := proseLedgerRoot(t)
	proseLedgersMu.Lock()
	ledger, existing := proseLedgers[root]
	if !existing {
		ledger = &proseLedger{asserted: map[string]bool{}, carried: map[string]bool{}}
		proseLedgers[root] = ledger
	}
	proseLedgersMu.Unlock()
	if !existing {
		t.Cleanup(func() {
			proseLedgersMu.Lock()
			if proseLedgers[root] == ledger {
				delete(proseLedgers, root)
			}
			proseLedgersMu.Unlock()
			ledger.closed(t)
		})
	}
	return ledger
}

func lookupProseLedger(t *testing.T) *proseLedger {
	proseLedgersMu.Lock()
	defer proseLedgersMu.Unlock()
	return proseLedgers[proseLedgerRoot(t)]
}

func (l *proseLedger) markAsserted(key string) {
	l.mu.Lock()
	l.asserted[key] = true
	l.mu.Unlock()
}

func (l *proseLedger) recordCarried(obj map[string]any) {
	l.mu.Lock()
	for key := range obj {
		l.carried[key] = true
	}
	l.mu.Unlock()
}

func (l *proseLedger) declareBlock(t *testing.T, label string, obj map[string]any, blk *assertionBlock) {
	t.Helper()
	if len(proseDeclaration(obj)) == 0 {
		t.Errorf("%s: `prose` is present but names no keys — a block declaring prose declares which keys are prose", label)
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, existing := range l.blocks {
		if sameJSONBlock(existing.obj, obj) {
			return
		}
	}
	l.blocks = append(l.blocks, proseBlock{label: label, obj: obj, blk: blk})
}

func (l *proseLedger) recordDischarge(d proseDischarge) {
	l.mu.Lock()
	l.discharges = append(l.discharges, d)
	l.mu.Unlock()
}

// closed is the ledger's own teardown: a run that recorded prose but never
// verified it fails here, exactly as an unconsumed key does.
func (l *proseLedger) closed(t *testing.T) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.verified || (len(l.blocks) == 0 && len(l.discharges) == 0) {
		return
	}
	t.Errorf("%s: the fixture declares prose keys (or the runner discharged some) and verifyProse was never called "+
		"— an unverified discharge claim is a claim about the run that nothing checked; call verifyProse(t, fixture) "+
		"once the replay is finished", proseLedgerFixtureLabel(l.blocks))
}

func proseLedgerFixtureLabel(blocks []proseBlock) string {
	if len(blocks) > 0 {
		return blocks[0].label
	}
	return "prose ledger"
}

func sameJSONBlock(a, b map[string]any) bool {
	ida, oka := assertionBlockID(a)
	idb, okb := assertionBlockID(b)
	return oka && okb && ida == idb
}

// proseKey discharges a prose key by naming the executable keys that carry its
// obligation. It REPLACES excuseKey for these keys: two paths to satisfy one key
// is the ambiguity this convention removes, so a declared prose key that is
// excused fails (rule 2) rather than being quietly accepted.
func proseKey(t *testing.T, block map[string]any, key string, dischargedBy ...string) {
	t.Helper()
	ledger := lookupProseLedger(t)
	if ledger == nil {
		t.Errorf("%s: proseKey(%q) with no open prose ledger — the block carrying the key must go through "+
			"consumeKeys before its prose can be discharged", assertionLabel(block), key)
		return
	}
	ledger.recordDischarge(proseDischarge{
		label: assertionLabel(block),
		block: block,
		key:   key,
		by:    append([]string{}, dischargedBy...),
	})
}

// verifyProse arms the fixture-end check. It is called once, AFTER the replay.
//
// The NET, however, is not armed here — it is armed by the consumption seam
// (trackAssertions), because every cleanup mechanism runs in REVERSE
// registration order. A net armed by the first proseKey call would fire BEFORE a
// verifyProse the runner registered earlier in its body and report a false
// failure; armed from the seam that already owns the block's consumption check,
// it is structurally guaranteed to run last.
//
// The cleanup registered here runs before the per-block rung-3 teardowns for the
// same LIFO reason, so the `prose` key it consumes is seen as asserted by them.
//
// A "run" is ONE TEST, not one process: the ledger belongs to the *testing.T
// that opened it, and is CLEARED here so a second replay in the same test cannot
// be satisfied by the first one's assertions.
func verifyProse(t *testing.T, fixture string) {
	t.Helper()
	ledger := lookupProseLedger(t)
	if ledger == nil {
		t.Errorf("verifyProse(%s): no prose ledger is open — nothing in this replay went through consumeKeys", fixture)
		return
	}
	// Detach the population NOW, not in the cleanup. verifyProse is called after
	// the replay, so everything it must judge has already happened; taking the
	// snapshot here rather than later is what makes a second verifyProse in the
	// same test judge only its own fixture, and what keeps LIFO cleanup ordering
	// from interleaving two fixtures' populations.
	snapshot := ledger.detach(fixture)
	t.Cleanup(func() {
		for _, problem := range snapshot.problems(fixture) {
			t.Errorf("%s", problem)
		}
		// The comparison above read the block's own `prose` value; marking it
		// asserted is what CONSUMES it, so a fixture that gains a prose key the
		// runner never discharges fails rung 3 rather than passing silently.
		snapshot.consumeDeclarations(t)
		recordProseVerified(fixture)
	})
}

// detach takes the ledger's population and CLEARS it, so "the same fixture's
// run" means one test rather than one process. Unioning asserted keys across
// runs would let a discharge in one be satisfied by an assertion in another —
// the accident-of-collocation the fixture-scoped ledger exists to bound.
func (l *proseLedger) detach(fixture string) *proseLedger {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := &proseLedger{
		fixture:    fixture,
		verified:   true,
		asserted:   l.asserted,
		carried:    l.carried,
		blocks:     l.blocks,
		discharges: l.discharges,
	}
	l.fixture = fixture
	l.verified = true
	l.asserted = map[string]bool{}
	l.carried = map[string]bool{}
	l.blocks = nil
	l.discharges = nil
	return out
}

// consumeDeclarations marks each declaring block's `prose` key asserted. The
// check itself lives in problems, so the message a mismatch produces names the
// missing and the extra keys rather than dumping two lists.
func (l *proseLedger) consumeDeclarations(t *testing.T) {
	l.mu.Lock()
	blocks := append([]proseBlock{}, l.blocks...)
	l.mu.Unlock()
	for _, block := range blocks {
		assertKeyWith(t, block.obj, "prose", func(want any) {
			names, ok := want.([]any)
			if !ok {
				t.Errorf("%s: prose declaration must be an array, got %T", assertionLabel(block.obj), want)
				return
			}
			for _, name := range names {
				if _, ok := name.(string); !ok {
					t.Errorf("%s: prose declaration entries must be strings, got %T", assertionLabel(block.obj), name)
				}
			}
		})
	}
}

// problems is the decision verifyProse reports, split out so every rule can be
// mutation-checked without a t whose Errorf would redden the caller.
func (l *proseLedger) problems(fixture string) []string {
	l.mu.Lock()
	blocks := append([]proseBlock{}, l.blocks...)
	discharges := append([]proseDischarge{}, l.discharges...)
	asserted := make(map[string]bool, len(l.asserted))
	for key := range l.asserted {
		asserted[key] = true
	}
	carried := make(map[string]bool, len(l.carried))
	for key := range l.carried {
		carried[key] = true
	}
	l.mu.Unlock()

	var out []string
	if len(blocks) == 0 {
		return []string{fmt.Sprintf("verifyProse(%s): no block declared `prose` — either the fixture carries no "+
			"prose declaration or the block carrying it never reached consumeKeys", fixture)}
	}

	// Rule 7 reads the fixture-wide set: a discharge naming a paragraph from
	// any block of this fixture is naming prose. SEEDED WITH `prose` ITSELF,
	// which is not redundant: `prose` never lists itself, so without the seed
	// rule 7 misses `proseKey(t, block, k, "prose")` — and rule 4's own
	// comparison marks `prose` asserted, so rule 6 would wave it through. A
	// paragraph discharged by the declaration that it is a paragraph proves
	// nothing.
	isProse := map[string]bool{"prose": true}
	for _, block := range blocks {
		for _, name := range proseDeclaration(block.obj) {
			isProse[name] = true
		}
	}

	for _, discharge := range discharges {
		known := false
		for _, block := range blocks {
			if sameJSONBlock(block.obj, discharge.block) {
				known = true
				break
			}
		}
		if !known {
			// Rule 3, in its strongest form: the block does not declare `prose`
			// at all, so nothing in the corpus says this key is a paragraph.
			out = append(out, fmt.Sprintf("%s: key %q is discharged, but its block declares no `prose` — "+
				"only the corpus decides which keys are prose", discharge.label, discharge.key))
		}
		// Rule 5.
		if len(discharge.by) == 0 {
			out = append(out, fmt.Sprintf("%s: the discharge of %q names no keys — a discharge that names "+
				"nothing is the free-text excuse this convention replaces", discharge.label, discharge.key))
		}
		for _, name := range discharge.by {
			switch {
			case isProse[name]:
				// Rule 7, including the `prose` seed.
				out = append(out, fmt.Sprintf("%s: the discharge of %q names %q, which is itself prose — "+
					"a paragraph cannot carry another paragraph's obligation, and `prose` least of all: "+
					"being declared a paragraph is not evidence about the run", discharge.label, discharge.key, name))
			case !carried[name]:
				// Rule 6, rotted form: the fixture does not carry this key at
				// all, so no run could ever have asserted it.
				out = append(out, fmt.Sprintf("%s: the discharge of %q names %q, which this fixture carries in no "+
					"block — the discharge has rotted, exactly as a stale excuse has", discharge.label, discharge.key, name))
			case !asserted[name]:
				// Rule 6 — the whole convention. ASSERTED, not merely
				// dispositioned: an excused key discharges nothing, because an
				// excuse is precisely the absence of a comparison.
				out = append(out, fmt.Sprintf("%s: the discharge of %q names %q, which this fixture's run never "+
					"asserted — the claim is false, or the assertion it names was dropped", discharge.label, discharge.key, name))
			}
		}
	}

	for _, block := range blocks {
		declared := proseDeclaration(block.obj)
		declaredSet := map[string]bool{}
		for _, name := range declared {
			declaredSet[name] = true
		}

		discharged := map[string]bool{}
		for _, discharge := range discharges {
			if !sameJSONBlock(block.obj, discharge.block) {
				continue
			}
			// Rule 3.
			if !declaredSet[discharge.key] {
				out = append(out, fmt.Sprintf("%s: key %q is discharged but `assertions.prose` does not list it — "+
					"a key with a value a runner can compare must be ASSERTED, not discharged", block.label, discharge.key))
			}
			discharged[discharge.key] = true
		}

		if block.blk != nil {
			block.blk.mu.Lock()
			for _, name := range declared {
				// Rule 1.
				if block.blk.asserted[name] {
					out = append(out, fmt.Sprintf("%s: prose key %q is ASSERTED — comparing a paragraph, or a tally "+
						"derived from one, to an English string pins wording rather than behaviour; discharge it "+
						"with proseKey instead", block.label, name))
				}
				// Rule 2.
				if reason, excused := block.blk.excused[name]; excused {
					out = append(out, fmt.Sprintf("%s: prose key %q is EXCUSED with free text (%q) — an unfalsifiable "+
						"reason is indistinguishable from no reason; name the executable keys that carry the "+
						"obligation with proseKey", block.label, name, reason))
				}
			}
			block.blk.mu.Unlock()
		}

		// Rule 4: the comparison that consumes `prose` itself.
		var missing, extra []string
		for _, name := range declared {
			if !discharged[name] {
				missing = append(missing, name)
			}
		}
		for name := range discharged {
			if !declaredSet[name] {
				extra = append(extra, name)
			}
		}
		sort.Strings(missing)
		sort.Strings(extra)
		if len(missing) > 0 || len(extra) > 0 {
			out = append(out, fmt.Sprintf("%s: the discharged set does not match `assertions.prose` — undischarged %v, "+
				"discharged-but-not-declared %v; every declared paragraph gets a discharge naming the executable "+
				"keys that carry it", block.label, missing, extra))
		}
	}
	return out
}

func assertionLabel(block map[string]any) string {
	if blk := lookupAssertionBlock(block); blk != nil {
		return blk.label
	}
	return "untracked block"
}

// jsonValueEqual compares a decoded fixture value against a live Go value by
// projecting the live value through JSON, so an `int` from the library and a
// `float64` from the fixture compare equal. A nil slice/map and an empty one are
// treated as equal: the distinction is a Go representation detail, not a claim
// the corpus makes.
func jsonValueEqual(want, actual any) bool {
	round, err := json.Marshal(actual)
	if err != nil {
		return false
	}
	var got any
	if err := json.Unmarshal(round, &got); err != nil {
		return false
	}
	return jsonEquivalent(want, got)
}

func jsonEquivalent(a, b any) bool {
	if jsonEmpty(a) && jsonEmpty(b) {
		return true
	}
	switch av := a.(type) {
	case []any:
		bv, ok := b.([]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !jsonEquivalent(av[i], bv[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, v := range av {
			other, present := bv[k]
			if !present || !jsonEquivalent(v, other) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(a, b)
	}
}

// jsonEmpty reports the values that mean "nothing here" across the Go/JSON
// boundary: null, and an empty list or object.
func jsonEmpty(v any) bool {
	switch t := v.(type) {
	case nil:
		return true
	case []any:
		return len(t) == 0
	case map[string]any:
		return len(t) == 0
	}
	return false
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

// ---------------------------------------------------------------------------
// Rung 3, struct-shaped: a decoded field that nothing ever reads
// ---------------------------------------------------------------------------
//
// The map-shaped tracker above cannot see the strictJSON runners: they decode a
// fixture into a struct, and a field is "declared read" by existing. Go will
// refuse to compile an unused *local*, but an unused struct FIELD is silent —
// which is exactly shape 2 of this defect, and exactly the promise the
// #lzassertunknownkeys header makes ("every struct field declared here is a
// promise that the runner reads it"). Nothing was checking that promise.
//
// This is the static counterpart: parse the package's own test sources, collect
// every field carrying a `json:"..."` tag, and fail when the field's name never
// appears as a selector anywhere in the package. Its limit is honest and worth
// stating — a field mentioned only in a log line counts as read — but the shape
// it does catch is the one that was actually present here: a field decoded to
// satisfy DisallowUnknownFields and then never mentioned again.
//
// conformanceStructFieldExcuses is the both-directions allowlist, same rule as
// everywhere else: a name here that IS read fails as a stale excuse.
var conformanceStructFieldExcuses = map[string]string{
	"conformance_strict_json_test.go:ProseDescription": "corpus prose: free-form fixture documentation with no machine-checkable claim, modelled so the strict decode does not mistake it for an unchecked expectation",
	"conformance_strict_json_test.go:ProseNotes":       "corpus prose: free-form fixture documentation with no machine-checkable claim",
	"conformance_strict_json_test.go:ProseNote":        "corpus prose: free-form fixture documentation with no machine-checkable claim",
	"conformance_strict_json_test.go:ProseComment":     "corpus prose: free-form fixture documentation with no machine-checkable claim",
	"conformance_strict_json_test.go:MetaKind":         "corpus taxonomy: `kind` picks which runner replays the fixture rather than stating anything the replay must observe; the runners that gate on it declare their own field (crdtTreeFixture.Kind)",
	"reliable_sync_conformance_test.go:OutboxAck":      "wire-union discriminator: assertRootDelta requires the Delta arm and fails on any other, so the OutboxAck arm selects a code path rather than carrying a value to check",
	"reliable_sync_conformance_test.go:ResyncRequest":  "wire-union discriminator: assertRootDelta requires the Delta arm and fails on any other, so the ResyncRequest arm selects a code path rather than carrying a value to check",
}

type conformanceStructField struct {
	file string
	name string
	tag  string
	pos  string
}

// conformanceTaggedFields returns every json-tagged struct field declared in the
// package's test sources, plus the set of field names read as a selector.
func conformanceTaggedFields(t *testing.T) ([]conformanceStructField, map[string]bool) {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading package dir: %v", err)
	}
	fset := token.NewFileSet()
	var fields []conformanceStructField
	read := map[string]bool{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(".", name), nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.SelectorExpr:
				read[node.Sel.Name] = true
			case *ast.StructType:
				for _, field := range node.Fields.List {
					if field.Tag == nil || len(field.Names) == 0 {
						continue
					}
					raw, err := strconv.Unquote(field.Tag.Value)
					if err != nil {
						continue
					}
					tag := reflect.StructTag(raw).Get("json")
					if tag == "" || tag == "-" {
						continue
					}
					tag = strings.Split(tag, ",")[0]
					for _, ident := range field.Names {
						if !ident.IsExported() {
							continue
						}
						fields = append(fields, conformanceStructField{
							file: name,
							name: ident.Name,
							tag:  tag,
							pos:  fset.Position(ident.Pos()).String(),
						})
					}
				}
			}
			return true
		})
	}
	if len(fields) == 0 {
		t.Fatal("no json-tagged conformance struct fields found — the scan is asserting nothing")
	}
	return fields, read
}

// TestConformanceStructFieldsAreRead is rung 3 for the strictJSON runners.
func TestConformanceStructFieldsAreRead(t *testing.T) {
	fields, read := conformanceTaggedFields(t)
	excusedAndRead := map[string]bool{}
	for _, field := range fields {
		key := field.file + ":" + field.name
		reason, excused := conformanceStructFieldExcuses[key]
		switch {
		case read[field.name] && excused:
			excusedAndRead[key] = true
			t.Errorf("%s: field %s (json %q) is both read and excused (%q) — the excuse has gone stale; delete it",
				field.pos, field.name, field.tag, reason)
		case read[field.name]:
		case excused:
			if strings.TrimSpace(reason) == "" {
				t.Errorf("%s: field %s is excused with an empty reason", field.pos, field.name)
			}
		default:
			t.Errorf("%s: field %s decodes fixture key %q and nothing in the package ever reads it "+
				"— a field that exists only to satisfy DisallowUnknownFields is a silently skipped assertion; "+
				"implement it, or record it in conformanceStructFieldExcuses with a reason",
				field.pos, field.name, field.tag)
		}
	}
	declared := map[string]bool{}
	for _, field := range fields {
		declared[field.file+":"+field.name] = true
	}
	for key := range conformanceStructFieldExcuses {
		if !declared[key] && !excusedAndRead[key] {
			t.Errorf("conformanceStructFieldExcuses names %q, which declares no json-tagged field — the excuse is stale", key)
		}
	}
}

// TestAssertionBlockVerifiesDisposition is the mutation check for the rung-3
// tracker itself: without each arm, the shape it names goes green.
func TestAssertionBlockVerifiesDisposition(t *testing.T) {
	newBlock := func(declared ...string) *assertionBlock {
		return &assertionBlock{
			label:    "fake.json expected",
			obj:      map[string]any{"order": []any{"a"}, "held": true, "description": "prose"},
			declared: declared,
			asserted: map[string]bool{},
			excused:  map[string]string{},
		}
	}

	// Read but never asserted.
	blk := newBlock("order", "held")
	blk.asserted["order"] = true
	problems := blk.problems()
	if len(problems) != 1 || !strings.Contains(problems[0], `"held"`) ||
		!strings.Contains(problems[0], "never reaches a comparison") {
		t.Fatalf("read-but-not-asserted not reported: %v", problems)
	}

	// A reasoned excuse satisfies it; an empty reason does not.
	blk = newBlock("order", "held")
	blk.asserted["order"] = true
	blk.excused["held"] = "replay input: selects the branch, states no value"
	if problems := blk.problems(); len(problems) != 0 {
		t.Fatalf("reasoned excuse rejected: %v", problems)
	}
	blk = newBlock("held")
	blk.excused["held"] = "   "
	if problems := blk.problems(); len(problems) != 1 || !strings.Contains(problems[0], "empty reason") {
		t.Fatalf("empty reason accepted: %v", problems)
	}

	// Stale excuse: excused AND asserted in the same run.
	blk = newBlock("order", "held")
	blk.asserted["order"] = true
	blk.asserted["held"] = true
	blk.excused["held"] = "asserted somewhere else, allegedly"
	problems = blk.problems()
	if len(problems) != 1 || !strings.Contains(problems[0], "gone stale") {
		t.Fatalf("stale excuse not reported: %v", problems)
	}

	// Prose is exempt, and a declared key the fixture does not carry is not a
	// claim about this fixture.
	blk = newBlock("order", "held", "description", "absent")
	blk.asserted["order"] = true
	blk.asserted["held"] = true
	if problems := blk.problems(); len(problems) != 0 {
		t.Fatalf("prose or absent key reported: %v", problems)
	}
}

// TestAssertionBlockRejectsUncheckedObjectValuedKey is the mutation check for
// rung 6 (#lzsubblockkeyset). Without the arm it names, an assertion key whose
// value is a JSON object satisfies the tracker by being asserted at all, and the
// sub-keys beneath it are compared by nothing.
func TestAssertionBlockRejectsUncheckedObjectValuedKey(t *testing.T) {
	newBlock := func() *assertionBlock {
		return &assertionBlock{
			label: "fake.json assertions",
			obj: map[string]any{
				"descriptor": map[string]any{"offset": 0.0, "len": 8.0},
				"held":       true,
			},
			declared:      []string{"descriptor", "held"},
			asserted:      map[string]bool{"held": true},
			excused:       map[string]string{},
			keySetChecked: map[string]bool{},
		}
	}

	// Asserted through a plain callback: the object's key set reached nothing.
	blk := newBlock()
	blk.asserted["descriptor"] = true
	problems := blk.problems()
	if len(problems) != 1 || !strings.Contains(problems[0], `"descriptor"`) ||
		!strings.Contains(problems[0], "without a key-set check") {
		t.Fatalf("unchecked object-valued key not reported: %v", problems)
	}

	// A key-set entry point discharges it.
	blk = newBlock()
	blk.asserted["descriptor"] = true
	blk.keySetChecked["descriptor"] = true
	if problems := blk.problems(); len(problems) != 0 {
		t.Fatalf("key-set-checked object key reported: %v", problems)
	}

	// The excuse channel stays open — with a reason, as it already demands.
	blk = newBlock()
	blk.excused["descriptor"] = "container: asserted key-by-key by the loop below"
	if problems := blk.problems(); len(problems) != 0 {
		t.Fatalf("reasoned excuse on an object-valued key reported: %v", problems)
	}

	// A SCALAR key asserted the plain way is untouched by the rung: this arm is
	// what keeps the guard from degenerating into "every key needs a key set".
	blk = newBlock()
	blk.asserted["descriptor"] = true
	blk.keySetChecked["descriptor"] = true
	blk.obj["held"] = "scalar"
	if problems := blk.problems(); len(problems) != 0 {
		t.Fatalf("scalar key reported by the object-valued guard: %v", problems)
	}
}

// TestAssertKeyOnObjectComparesKeySet pins the fourth discharge path: assertKey
// compares an object WHOLE, so a sub-key the live value lacks is a failure and
// the rung-6 obligation is met by construction. Without this, the allowance
// assertKey makes for object values would be an untested hole.
func TestAssertKeyOnObjectComparesKeySet(t *testing.T) {
	fixture := map[string]any{"descriptor": map[string]any{"offset": 1.0, "len": 8.0}}
	consumeKeys(t, "fake.json assertions", fixture, "descriptor")

	// A live value missing one of the fixture's sub-keys must NOT compare equal.
	if jsonValueEqual(fixture["descriptor"], map[string]int{"offset": 1}) {
		t.Fatal("a partial object compared equal — assertKey would not see an added sub-key")
	}
	if !jsonValueEqual(fixture["descriptor"], map[string]int{"offset": 1, "len": 8}) {
		t.Fatal("the whole object did not compare equal")
	}

	assertKey(t, fixture, "descriptor", map[string]int{"offset": 1, "len": 8})
	blk := lookupAssertionBlock(fixture)
	if blk == nil || !blk.keySetChecked["descriptor"] {
		t.Fatal("assertKey on an object value did not record the key-set check")
	}
}

// TestNoProseWordedExcuses is the STATIC half of rule 2
// (#lzprosekeyconvention). The runtime tracker catches a free-text excuse only
// for a key the corpus already declares prose; this catches the habit itself —
// an `excuseKey` whose reason opens with "prose" is a runner deciding for itself
// that a paragraph needs no discharge, which is the treatment this convention
// replaced. If a key really is a paragraph, `assertions.prose` is where that is
// declared, and proseKey is how it is discharged.
func TestNoProseWordedExcuses(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading package dir: %v", err)
	}
	fset := token.NewFileSet()
	scanned := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(".", name), nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		scanned++
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			fn, ok := call.Fun.(*ast.Ident)
			if !ok || (fn.Name != "excuseKey" && fn.Name != "excuseKeys") {
				return true
			}
			for _, arg := range call.Args {
				ast.Inspect(arg, func(inner ast.Node) bool {
					lit, ok := inner.(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						return true
					}
					text, err := strconv.Unquote(lit.Value)
					if err != nil {
						return true
					}
					if strings.HasPrefix(strings.ToLower(strings.TrimSpace(text)), "prose") {
						t.Errorf("%s: excuse reason opens with %q — a paragraph is DISCHARGED, never excused: "+
							"declare it in the fixture's `assertions.prose` upstream and name the executable keys "+
							"that carry its obligation with proseKey (#lzprosekeyconvention)",
							fset.Position(lit.Pos()), text)
					}
					return true
				})
			}
			return true
		})
	}
	if scanned == 0 {
		t.Fatal("no test sources scanned — the guard is asserting nothing")
	}
}

// TestProseLedgerVerifiesDischarges is the mutation check for the rung-5 tracker
// itself: without each arm, the shape it names goes green.
func TestProseLedgerVerifiesDischarges(t *testing.T) {
	// A block declaring two paragraphs and carrying two executable keys.
	newLedger := func(asserted ...string) (*proseLedger, map[string]any, *assertionBlock) {
		obj := map[string]any{
			"prose":        []any{"clause", "anti_vacuity"},
			"clause":       "a decoder MUST reject rather than round",
			"backends":     []any{"shm"},
			"codecs":       []any{"json"},
			"anti_vacuity": "the exact scenarios are the control",
		}
		blk := &assertionBlock{
			label:    "fake.json assertions",
			obj:      obj,
			declared: []string{"prose", "clause", "backends", "codecs", "anti_vacuity"},
			asserted: map[string]bool{},
			excused:  map[string]string{},
		}
		ledger := &proseLedger{asserted: map[string]bool{}, carried: map[string]bool{}}
		for _, key := range asserted {
			ledger.asserted[key] = true
		}
		for key := range obj {
			ledger.carried[key] = true
		}
		ledger.blocks = []proseBlock{{label: blk.label, obj: obj, blk: blk}}
		return ledger, obj, blk
	}
	only := func(t *testing.T, problems []string, want string) {
		t.Helper()
		if len(problems) != 1 || !strings.Contains(problems[0], want) {
			t.Fatalf("want exactly one problem containing %q, got %v", want, problems)
		}
	}

	// The conforming shape: both paragraphs discharged by asserted keys.
	ledger, obj, _ := newLedger("backends", "codecs")
	ledger.discharges = []proseDischarge{
		{label: "fake.json assertions", block: obj, key: "clause", by: []string{"backends"}},
		{label: "fake.json assertions", block: obj, key: "anti_vacuity", by: []string{"codecs"}},
	}
	if problems := ledger.problems("fake.json"); len(problems) != 0 {
		t.Fatalf("conforming discharge rejected: %v", problems)
	}

	// Rule 1: a declared prose key that is ASSERTED.
	ledger, obj, blk := newLedger("backends", "codecs")
	ledger.discharges = []proseDischarge{
		{block: obj, key: "clause", by: []string{"backends"}},
		{block: obj, key: "anti_vacuity", by: []string{"codecs"}},
	}
	blk.asserted["clause"] = true
	only(t, ledger.problems("fake.json"), "is ASSERTED")

	// Rule 2: a declared prose key EXCUSED with free text.
	ledger, obj, blk = newLedger("backends", "codecs")
	ledger.discharges = []proseDischarge{
		{block: obj, key: "clause", by: []string{"backends"}},
		{block: obj, key: "anti_vacuity", by: []string{"codecs"}},
	}
	blk.excused["anti_vacuity"] = "prose: it explains the fixture"
	only(t, ledger.problems("fake.json"), "is EXCUSED with free text")

	// Rule 3: a key the corpus does not declare prose, discharged. It trips the
	// set comparison too — a discharged-but-not-declared key is exactly what
	// rule 4 reports as `extra`.
	ledger, obj, _ = newLedger("backends", "codecs")
	ledger.discharges = []proseDischarge{
		{block: obj, key: "clause", by: []string{"backends"}},
		{block: obj, key: "anti_vacuity", by: []string{"codecs"}},
		{block: obj, key: "codecs", by: []string{"backends"}},
	}
	problems := ledger.problems("fake.json")
	if len(problems) != 2 || !strings.Contains(problems[0], "does not list it") {
		t.Fatalf("undeclared discharge not reported: %v", problems)
	}

	// Rule 4: a declared paragraph nobody discharged.
	ledger, obj, _ = newLedger("backends", "codecs")
	ledger.discharges = []proseDischarge{{block: obj, key: "clause", by: []string{"backends"}}}
	only(t, ledger.problems("fake.json"), "undischarged [anti_vacuity]")

	// Rule 5: a discharge naming nothing.
	ledger, obj, _ = newLedger("backends", "codecs")
	ledger.discharges = []proseDischarge{
		{block: obj, key: "clause", by: nil},
		{block: obj, key: "anti_vacuity", by: []string{"codecs"}},
	}
	only(t, ledger.problems("fake.json"), "names no keys")

	// Rule 6: a discharge naming a key the run never asserted.
	ledger, obj, _ = newLedger("codecs")
	ledger.discharges = []proseDischarge{
		{block: obj, key: "clause", by: []string{"backends"}},
		{block: obj, key: "anti_vacuity", by: []string{"codecs"}},
	}
	only(t, ledger.problems("fake.json"), "never asserted")

	// Rule 6, rotted form: a discharge naming a key the fixture carries nowhere.
	// A distinct failure from naming a carried key nothing asserted — the
	// discharge has gone stale rather than being false.
	ledger, obj, _ = newLedger("backends", "codecs")
	ledger.discharges = []proseDischarge{
		{block: obj, key: "clause", by: []string{"backend_forms"}},
		{block: obj, key: "anti_vacuity", by: []string{"codecs"}},
	}
	only(t, ledger.problems("fake.json"), "carries in no block")

	// Rule 7: a discharge naming another paragraph.
	ledger, obj, _ = newLedger("backends", "codecs")
	ledger.discharges = []proseDischarge{
		{block: obj, key: "clause", by: []string{"anti_vacuity"}},
		{block: obj, key: "anti_vacuity", by: []string{"codecs"}},
	}
	only(t, ledger.problems("fake.json"), "which is itself prose")

	// Rule 7, the `prose` seed. `prose` never lists itself, so without the seed
	// this slips past rule 7 — and rule 4's own comparison marks `prose`
	// asserted, so rule 6 waves it through too.
	ledger, obj, _ = newLedger("backends", "codecs", "prose")
	ledger.discharges = []proseDischarge{
		{block: obj, key: "clause", by: []string{"prose"}},
		{block: obj, key: "anti_vacuity", by: []string{"codecs"}},
	}
	only(t, ledger.problems("fake.json"), "which is itself prose")

	// A fixture whose prose declaration never reached the tracker.
	empty := &proseLedger{asserted: map[string]bool{}, carried: map[string]bool{}}
	only(t, empty.problems("fake.json"), "no block declared `prose`")
}

// TestProseDeclarationOverridesAnnotationExemption is the mutation check for the
// hazard two of the nine bindings hit independently: `note` is a reserved
// annotation name AND a declared prose key in both frame-codec fixtures. A
// tracker that subtracts its reserved-name set before consulting
// `assertions.prose` exempts the key from every guard and never discharges it,
// and the fixture skips the convention while the binding reports conforming.
func TestProseDeclarationOverridesAnnotationExemption(t *testing.T) {
	// `note` is declared prose, so it is NOT dispositioned by the rung-3 tracker
	// (the rung-5 tracker owns it) — the exemption must not be what causes that.
	declaring := &assertionBlock{
		label:    "fake.json assertions",
		obj:      map[string]any{"prose": []any{"note"}, "note": "a paragraph stating an obligation", "role": "reference"},
		declared: []string{"prose", "note", "role"},
		asserted: map[string]bool{},
		excused:  map[string]string{},
	}
	if !declaring.declaresProse("note") {
		t.Fatal("a declared `note` is not seen as prose — the declaration is being read after the exemption")
	}
	// Inside a declaring block the name exemption is off ENTIRELY: `description`
	// is a reserved annotation name, absent from the array, and still owes a
	// disposition.
	declaring.obj["description"] = "narration"
	declaring.declared = append(declaring.declared, "description")
	problems := declaring.problems()
	var sawDescription, sawNote bool
	for _, problem := range problems {
		if strings.Contains(problem, `"description"`) {
			sawDescription = true
		}
		if strings.Contains(problem, `"note"`) {
			sawNote = true
		}
	}
	if !sawDescription {
		t.Fatalf("the name exemption still applies inside a declaring block: %v", problems)
	}
	if sawNote {
		t.Fatalf("a declared prose key was dispositioned by rung 3 rather than rung 5: %v", problems)
	}

	// Outside a declaring block the exemption stands as-is.
	plain := &assertionBlock{
		label:    "fake.json step",
		obj:      map[string]any{"note": "narration", "value": float64(1)},
		declared: []string{"note", "value"},
		asserted: map[string]bool{"value": true},
		excused:  map[string]string{},
	}
	if problems := plain.problems(); len(problems) != 0 {
		t.Fatalf("reserved annotation name reported outside a declaring block: %v", problems)
	}
}

// TestAssertKeyMarksAndCompares pins the two properties assertKey is relied on
// for: it compares against the fixture's own value across the Go/JSON numeric
// boundary, and it is the only way a key becomes asserted.
func TestAssertKeyMarksAndCompares(t *testing.T) {
	block := map[string]any{"count": float64(3), "order": []any{"a", "b"}, "empty": []any{}}
	trackAssertions(t, "fake.json expected", block, []string{"count", "order", "empty"})
	blk := lookupAssertionBlock(block)
	if blk == nil {
		t.Fatal("block not tracked")
	}

	assertKey(t, block, "count", 3) // int vs fixture float64
	assertKey(t, block, "order", []string{"a", "b"})
	assertKey(t, block, "empty", []string(nil)) // nil slice vs []
	for _, key := range []string{"count", "order", "empty"} {
		if !blk.asserted[key] {
			t.Fatalf("%s not marked asserted", key)
		}
	}
	if problems := blk.problems(); len(problems) != 0 {
		t.Fatalf("fully asserted block reported: %v", problems)
	}

	// A comparison written against a literal marks nothing.
	other := map[string]any{"count": float64(3)}
	trackAssertions(t, "fake.json literal", other, []string{"count"})
	if other["count"] == float64(3) { //nolint:staticcheck // this is the defect shape, on purpose
		_ = 1
	}
	if problems := lookupAssertionBlock(other).problems(); len(problems) != 1 ||
		!strings.Contains(problems[0], "never reaches a comparison") {
		t.Fatalf("literal comparison counted as an assertion: %v", problems)
	}
	excuseKey(t, other, "count", "asserted by the arm above; excused here to keep this probe's teardown green")

	if jsonValueEqual(float64(3), 4) {
		t.Fatal("jsonValueEqual accepted a mismatch")
	}
	if !jsonValueEqual(map[string]any{"a": float64(1)}, map[string]int{"a": 1}) {
		t.Fatal("jsonValueEqual rejected an equivalent map")
	}
}
