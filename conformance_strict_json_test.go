package lazily

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
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
}

var (
	assertionBlocksMu sync.Mutex
	// Keyed by the block map's runtime pointer: the runners hand the raw map
	// around (`expected["order"]`), so identity is the only handle available.
	// The record holds the map, which keeps it alive until the cleanup that
	// removes the record, so an address is never reused under a live entry.
	assertionBlocks = map[uintptr][]*assertionBlock{}
)

// proseKeys are exempt from all three conditions: they document the fixture
// rather than stating anything the replay must observe.
var proseKeys = map[string]bool{
	"description": true, "notes": true, "note": true, "comment": true,
	"why": true, "kind": true, "model": true,
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
		label:    label,
		obj:      obj,
		declared: append([]string{}, declared...),
		asserted: map[string]bool{},
		excused:  map[string]string{},
	}
	assertionBlocksMu.Lock()
	assertionBlocks[id] = append(assertionBlocks[id], blk)
	assertionBlocksMu.Unlock()
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
	for _, key := range b.declared {
		if proseKeys[key] {
			continue
		}
		if _, present := b.obj[key]; !present {
			// A key the fixture does not carry cannot be asserted; the runner
			// declaring it read-if-present is not a claim about this fixture.
			continue
		}
		reason, excused := b.excused[key]
		switch {
		case b.asserted[key] && excused:
			out = append(out, fmt.Sprintf("%s: key %q is both asserted and excused (%q) — the excuse has gone stale and now hides nothing; delete it",
				b.label, key, reason))
		case excused && strings.TrimSpace(reason) == "":
			out = append(out, fmt.Sprintf("%s: key %q is excused with an empty reason — an excuse without a reason is a silent skip", b.label, key))
		case b.asserted[key] || excused:
			// dispositioned
		default:
			out = append(out, fmt.Sprintf("%s: key %q is read by the runner but never reaches a comparison against the fixture's own value "+
				"— assert it with assertKey/assertKeyWith, or say why it cannot be asserted here with excuseKey", b.label, key))
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
	})
}

// assertKeyWith marks key asserted and hands the fixture's own value to the
// caller's check. It exists for the comparisons that are not equality — a
// tolerance, a set containment, an invariant derived from the value. The point
// is that the fixture's value reaches the comparison, not that the comparison is
// `==`.
func assertKeyWith(t *testing.T, block map[string]any, key string, check func(want any)) {
	t.Helper()
	want, present := block[key]
	if !present {
		t.Errorf("%s: key %q is asserted but the fixture block does not carry it", assertionLabel(block), key)
		return
	}
	if blk := lookupAssertionBlock(block); blk != nil {
		blk.markAsserted(key)
	}
	check(want)
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
