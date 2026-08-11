package lazily

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
)

// ---------------------------------------------------------------------------
// Rung 0: a block NO runner ever BINDS (#lzunboundblockguard)
// ---------------------------------------------------------------------------
//
// Every rung above this one reasons about a block the runner already HAS. Rung 2
// (consumeKeys) fails on a key the runner does not name; rung 3 fails on a key
// that is named and never compared; rungs 5 and 6 refine what "compared" means.
// All four are conditioned on the block having reached a runner at all. A block
// no runner ever opens is in none of their populations, so it cannot be reported
// as unconsumed — it is not "unasserted", it is INVISIBLE, and the suite is green
// over it in the strongest possible way: nothing knows it exists.
//
// This is not hypothetical. In lazily-dart every per-frame `assertions` block in
// `signaling/frames.json` — 17 frames, 9 distinct keys — was dead for exactly
// this reason. The unconsumed-key guard there was working correctly and reported
// nothing, because the runner bound the frame's `wire` and never looked at its
// `assertions` sibling. It was found by flipping fixture values (#lzperturbaudit),
// not by any guard.
//
// So this rung asks the question the others structurally cannot: OF THE FIXTURES
// THIS RUN ACTUALLY OPENED, is there an assertion-bearing block that no runner
// bound?
//
// Three properties make the answer trustworthy:
//
//   - The population comes from the RUNTIME MANIFEST, not from a source scan. A
//     grep for fixture names proves a filename is mentioned; only the recorded
//     read proves the bytes were opened (see conformance_manifest_test.go).
//   - The walk descends through ARRAYS. dart's dead blocks were one level down,
//     inside the elements of `frames`, which is where per-step and per-frame
//     assertions live in most of this corpus. A top-level-only walk would have
//     reported the dart hole as clean.
//   - Binding is matched by CONTENT DIGEST, not by pointer identity. Each runner
//     decodes the fixture independently — into a struct here, into a
//     `map[string]any` there — so no handle survives from the file on disk to the
//     value the runner holds. The digest is computed over the JSON tree in both
//     places, so the two sides can be compared at all.
//
// What "bound" means, precisely. A block is bound when a runner took
// responsibility for its KEY SET, by one of the two seams this package has:
//
//	consumeKeys/trackAssertions — the map-shaped seam. The runner declares the
//	   keys it reads and rung 3 then demands a disposition for each.
//	strictJSON — the struct-shaped seam. json.Decoder.DisallowUnknownFields makes
//	   an unmodelled key a hard error, and TestConformanceStructFieldsAreRead
//	   proves the modelled fields are read. Binding is recorded only where the
//	   destination type is a STRUCT: a `json.RawMessage`, an `any`, or a
//	   `map[string]json.RawMessage` field checks no keys, so descending through
//	   one records nothing and the sub-block still owes its own binding — which is
//	   what a re-decode through strictJSON, or a consumeKeys on the same object,
//	   then supplies.
//
// Known limit, stated rather than hidden: the digest is content-addressed and
// GLOBAL, so two byte-identical blocks in different fixtures credit each other.
// A `{"value": 1}` bound in one fixture marks an identical one elsewhere bound.
// Narrowing it would need a handle that survives the decode, which is the thing
// that does not exist. The digest is salted with the block's own KEY NAME where
// the binding seam knows it, so at least an `expected` never credits an
// `assertions`.

// assertionBearingBlockNames is the canonical set of keys whose object value is
// a block of assertions. `assertions` is the corpus-wide name; `expect` and
// `expected` are the per-step and per-scenario names the runners in this package
// hand to consumeKeys. Taxonomy and payload objects (`wire`, `input`, `seed`,
// `op`) are deliberately absent: they are replay INPUT, and a guard that demanded
// they be bound would be demanding assertions about the fixture's own stimulus.
var assertionBearingBlockNames = map[string]bool{
	"assertions": true,
	"expect":     true,
	"expected":   true,
}

// anyBlockName is the digest salt used by a seam that binds a block without
// knowing which key carried it. consumeKeys call sites pass a freeform label
// ("arena_blob.json assertions"), not a key, so their digests must match a block
// found under any of the names above.
const anyBlockName = "*"

var (
	boundBlocksMu sync.Mutex
	boundBlocks   = map[string]bool{}
	// openedFixturePaths maps the corpus-relative id of every fixture this run
	// opened to the path it was read from, so the guard re-reads the same bytes
	// the runner saw.
	openedFixturePaths = map[string]string{}
)

// blockDigest is the content address of a decoded JSON block, salted with the
// key that carried it.
func blockDigest(name string, obj map[string]any) (string, bool) {
	canonical, err := json.Marshal(obj)
	if err != nil {
		return "", false
	}
	sum := sha256.Sum256(append([]byte(name+"\x00"), canonical...))
	return hex.EncodeToString(sum[:]), true
}

// recordBoundBlock books a block a runner took responsibility for. name is the
// fixture key that carried it, or anyBlockName when the seam does not know.
func recordBoundBlock(name string, obj map[string]any) {
	if obj == nil {
		return
	}
	digest, ok := blockDigest(name, obj)
	if !ok {
		return
	}
	boundBlocksMu.Lock()
	boundBlocks[digest] = true
	boundBlocksMu.Unlock()
}

// blockIsBound reports whether a block found on disk under key `name` was bound
// by either seam.
func blockIsBound(bound map[string]bool, name string, obj map[string]any) bool {
	for _, salt := range []string{name, anyBlockName} {
		if digest, ok := blockDigest(salt, obj); ok && bound[digest] {
			return true
		}
	}
	return false
}

// bindBlock is the THIRD binding seam, for the runners that predate consumeKeys
// and take responsibility for a block's key set in their own loop: they iterate
// the block's keys with a fail-closed `default: t.Fatalf("unknown assertion key")`
// arm, or compare the block WHOLE against what the run produced. The header of
// conformance_strict_json_test.go already names the first shape and leaves those
// runners alone, so this rung has to be able to SEE them or it would report a
// live, fail-closed replay as dead.
//
// It is a DECLARATION, and deliberately weaker than the other two seams: it
// proves only that the block reached a runner, which is exactly what rung 0
// claims and no more. It is not a substitute for consumeKeys — what each key of
// the block then owes is rung 3's business — so a call site here should say in
// one line WHICH of the two shapes it is, and that claim is reviewable against
// the loop it sits next to.
func bindBlock(name string, block map[string]any) {
	recordBoundBlock(name, block)
}

// bindBlockFields is bindBlock for the runners that hold a block as
// `map[string]json.RawMessage` — the fail-closed shape's usual Go spelling. The
// raw values are re-decoded so the digest is taken over the same tree the corpus
// walk sees.
func bindBlockFields(name string, fields map[string]json.RawMessage) {
	if fields == nil {
		return
	}
	encoded, err := json.Marshal(fields)
	if err != nil {
		return
	}
	bindBlockBytes(name, encoded)
}

// bindBlockBytes is bindBlock for a block still held as raw JSON.
func bindBlockBytes(name string, raw []byte) {
	var block map[string]any
	if err := json.Unmarshal(raw, &block); err != nil {
		return
	}
	recordBoundBlock(name, block)
}

// recordOpenedFixture books the path a conformance fixture was read from. Like
// the prose ledger and unlike the coverage manifest it attributes against EVERY
// resolved root, mirror included: a block replayed from the offline fallback owes
// a binding exactly as the canonical one does.
func recordOpenedFixture(path string) {
	id, ok := specAnyRootRelative(path)
	if !ok || !strings.HasSuffix(path, ".json") {
		return
	}
	boundBlocksMu.Lock()
	if _, seen := openedFixturePaths[id]; !seen {
		openedFixturePaths[id] = path
	}
	boundBlocksMu.Unlock()
}

// ---------------------------------------------------------------------------
// The struct-shaped seam
// ---------------------------------------------------------------------------

var rawMessageType = reflect.TypeOf(json.RawMessage{})

// recordStrictBind books every block a strictJSON decode took responsibility
// for. It re-walks the raw document alongside the destination TYPE, because that
// is what decides where DisallowUnknownFields actually bites: a field typed
// `json.RawMessage` or `any` swallows an entire sub-tree without checking one
// key of it, and crediting those would make this rung report bindings that never
// happened.
func recordStrictBind(data []byte, v any) {
	bindStructuralInto(recordBoundBlock, data, v)
}

// bindStructuralInto is recordStrictBind with the sink injected, so a test can
// observe what a decode would book without publishing digests into the global
// ledger the real corpus is judged against.
func bindStructuralInto(record func(string, map[string]any), data []byte, v any) {
	var tree any
	if err := json.Unmarshal(data, &tree); err != nil {
		return
	}
	bindStructural(record, anyBlockName, tree, reflect.TypeOf(v))
}

func bindStructural(record func(string, map[string]any), name string, node any, typ reflect.Type) {
	if typ == nil {
		return
	}
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ == rawMessageType {
		return
	}
	// A custom UnmarshalJSON decides for itself what to accept, so the strict
	// decoder's key check does not apply beneath it.
	if reflect.PointerTo(typ).Implements(reflect.TypeOf((*json.Unmarshaler)(nil)).Elem()) {
		return
	}
	switch typ.Kind() {
	case reflect.Struct:
		obj, ok := node.(map[string]any)
		if !ok {
			return
		}
		record(name, obj)
		fields := jsonFieldTypes(typ)
		for key, child := range obj {
			if fieldType, modelled := fields[key]; modelled {
				bindStructural(record, key, child, fieldType)
			}
		}
	case reflect.Slice, reflect.Array:
		list, ok := node.([]any)
		if !ok {
			return
		}
		for _, child := range list {
			bindStructural(record, name, child, typ.Elem())
		}
	case reflect.Map:
		// The map's own key set is unchecked — any key decodes — so the map node
		// is NOT recorded as bound. Its values still are, when they are structs.
		obj, ok := node.(map[string]any)
		if !ok {
			return
		}
		for key, child := range obj {
			bindStructural(record, key, child, typ.Elem())
		}
	}
}

// jsonFieldTypes maps a struct's JSON key names to the types they decode into,
// flattening embedded structs the way encoding/json does (conformanceDoc is
// embedded throughout this package).
func jsonFieldTypes(typ reflect.Type) map[string]reflect.Type {
	out := map[string]reflect.Type{}
	var walk func(reflect.Type)
	walk = func(t reflect.Type) {
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
			if name == "-" {
				continue
			}
			if field.Anonymous && name == "" {
				embedded := field.Type
				for embedded.Kind() == reflect.Pointer {
					embedded = embedded.Elem()
				}
				if embedded.Kind() == reflect.Struct {
					walk(embedded)
					continue
				}
			}
			if !field.IsExported() {
				continue
			}
			if name == "" {
				name = field.Name
			}
			out[name] = field.Type
		}
	}
	walk(typ)
	return out
}

// ---------------------------------------------------------------------------
// The escape hatch
// ---------------------------------------------------------------------------
//
// A block that legitimately cannot be bound says so here, with a reason, and the
// entry is checked in BOTH directions like every other ledger in this family: an
// excuse for a block that IS bound fails as stale, and an excuse naming a path an
// opened fixture does not carry fails as rotted. Silence is not a disposition.
//
// The key is `<corpus-relative fixture id> <JSON path>`, exactly as the guard
// prints it.
var unboundBlockExcuses = map[string]string{}

// ---------------------------------------------------------------------------
// The walk and the decision
// ---------------------------------------------------------------------------

// fixtureBlock is one assertion-bearing block found on disk.
type fixtureBlock struct {
	fixture string
	path    string
	name    string
	obj     map[string]any
}

// walkAssertionBlocks collects every assertion-bearing block in a decoded
// fixture, descending through arrays as well as objects.
func walkAssertionBlocks(fixture string, tree any) []fixtureBlock {
	var out []fixtureBlock
	var walk func(node any, path string)
	walk = func(node any, path string) {
		switch value := node.(type) {
		case map[string]any:
			keys := make([]string, 0, len(value))
			for key := range value {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				child := value[key]
				childPath := path + "." + key
				if obj, isObject := child.(map[string]any); isObject && assertionBearingBlockNames[key] {
					out = append(out, fixtureBlock{fixture: fixture, path: childPath, name: key, obj: obj})
				}
				walk(child, childPath)
			}
		case []any:
			for i, child := range value {
				walk(child, fmt.Sprintf("%s[%d]", path, i))
			}
		}
	}
	walk(tree, "")
	return out
}

// unboundBlockReport is the decision this rung makes, split out from the process
// exit path so every arm can be mutation-checked with an ordinary test.
//
// opened maps a fixture id to the path it was read from; bound is the digest set
// the two seams recorded; excuses is the table above.
func unboundBlockReport(opened map[string]string, bound map[string]bool, excuses map[string]string) (problems []string, fixtures, blocks int) {
	ids := make([]string, 0, len(opened))
	for id := range opened {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	seenPaths := map[string]bool{}
	boundPaths := map[string]bool{}
	for _, id := range ids {
		data, err := os.ReadFile(opened[id])
		if err != nil {
			continue
		}
		var tree any
		if err := json.Unmarshal(data, &tree); err != nil {
			continue
		}
		fixtures++
		for _, block := range walkAssertionBlocks(id, tree) {
			blocks++
			key := block.fixture + " " + block.path
			seenPaths[key] = true
			isBound := blockIsBound(bound, block.name, block.obj)
			if isBound {
				boundPaths[key] = true
			}
			if _, excused := excuses[key]; excused {
				continue
			}
			if isBound {
				continue
			}
			problems = append(problems, fmt.Sprintf(
				"%s: block %s is assertion-bearing and NO runner bound it — it is not merely unasserted, "+
					"it is invisible to every other guard here, because a block nothing opens cannot be reported "+
					"unconsumed. Bind it with consumeKeys, or decode it through a strictJSON struct, or say why it "+
					"cannot be bound in unboundBlockExcuses (#lzunboundblockguard)", block.fixture, block.path))
		}
	}

	excuseKeys := make([]string, 0, len(excuses))
	for key := range excuses {
		excuseKeys = append(excuseKeys, key)
	}
	sort.Strings(excuseKeys)
	for _, key := range excuseKeys {
		reason := excuses[key]
		switch {
		case strings.TrimSpace(reason) == "":
			problems = append(problems, fmt.Sprintf(
				"unboundBlockExcuses[%q] has an empty reason — an excuse without a reason is a silent skip", key))
		case boundPaths[key]:
			problems = append(problems, fmt.Sprintf(
				"unboundBlockExcuses[%q] excuses a block that IS bound (%q) — the excuse has gone stale and now "+
					"hides nothing; delete it", key, reason))
		case !seenPaths[key] && openedFixtureOfExcuse(opened, key):
			problems = append(problems, fmt.Sprintf(
				"unboundBlockExcuses[%q] names a path its fixture no longer carries (%q) — the excuse has rotted; "+
					"delete it or point it at the block it meant", key, reason))
		}
	}
	sort.Strings(problems)
	return problems, fixtures, blocks
}

// openedFixtureOfExcuse reports whether the fixture an excuse names was opened
// by this run. A filtered `go test -run` legitimately opens nothing else, so an
// excuse for an unopened fixture is not judged either way.
func openedFixtureOfExcuse(opened map[string]string, key string) bool {
	fixture, _, ok := strings.Cut(key, " ")
	if !ok {
		return false
	}
	_, wasOpened := opened[fixture]
	return wasOpened
}

// snapshotUnboundInputs copies the two runtime populations under the lock.
func snapshotUnboundInputs() (map[string]string, map[string]bool) {
	boundBlocksMu.Lock()
	defer boundBlocksMu.Unlock()
	opened := make(map[string]string, len(openedFixturePaths))
	for id, path := range openedFixturePaths {
		opened[id] = path
	}
	bound := make(map[string]bool, len(boundBlocks))
	for digest := range boundBlocks {
		bound[digest] = true
	}
	return opened, bound
}

// checkUnboundAssertionBlocks is this rung run from TestMain, after the suite
// finishes: only then is the union of bindings complete, and no ordering between
// test functions has to be assumed.
//
// A run that opened NO fixture (`go test -bench` with `-run '^$'`, or a narrow
// `-run` filter) replayed no conformance at all and is not judged. A run that
// opened fixtures and found no assertion-bearing block in any of them IS judged,
// and fails: a walk that examined nothing is indistinguishable from a walk that
// found nothing, and the second reads as green.
func checkUnboundAssertionBlocks() bool {
	// A FILTERED run cannot judge boundness, and this is not a convenience —
	// it is the one place where a false RED is manufacturable. A fixture is
	// routinely opened by one test and bound by another (a shared loader reads
	// the file; the runner that binds its blocks lives in a different Test
	// function), so `-run` can select the opener while filtering out the binder
	// and every block in that fixture then looks dead. `make check` runs the
	// suite unfiltered, which is where this rung is meant to bite; `make
	// conformance` (-run Conformance) and any ad-hoc filter are not judged.
	if filter := flag.Lookup("test.run"); filter != nil && filter.Value.String() != "" {
		return true
	}
	opened, bound := snapshotUnboundInputs()
	if len(opened) == 0 {
		return true
	}
	problems, fixtures, blocks := unboundBlockReport(opened, bound, unboundBlockExcuses)
	if fixtures == 0 || blocks == 0 {
		fmt.Fprintf(os.Stderr,
			"FAIL: the unbound-block guard examined %d fixture(s) and %d assertion-bearing block(s) after a run "+
				"that opened %d conformance fixture(s) — a guard that examined nothing reports the same green as a "+
				"guard that found nothing (#lzunboundblockguard)\n", fixtures, blocks, len(opened))
		return false
	}
	if len(problems) == 0 {
		return true
	}
	fmt.Fprintf(os.Stderr, "FAIL: %d assertion-bearing block(s) the run opened were bound by no runner:\n", len(problems))
	for _, problem := range problems {
		fmt.Fprintf(os.Stderr, "  %s\n", problem)
	}
	return false
}

// ---------------------------------------------------------------------------
// Self-enforcement
// ---------------------------------------------------------------------------

// TestUnboundBlockWalkIsNotVacuous is the positive evidence for the walk itself,
// independent of which fixtures a filtered run happens to open. It reads the
// corpus directly, so a walk that stopped recognising assertion blocks — an
// emptied name set, a descent that no longer enters arrays — fails here rather
// than reporting a clean corpus.
func TestUnboundBlockWalkIsNotVacuous(t *testing.T) {
	path := specPath("signaling", "frames.json")
	data, err := os.ReadFile(path)
	if err != nil {
		specFixtureMissing(t, "signaling/frames.json unreadable: %v", err)
		return
	}
	var tree any
	if err := json.Unmarshal(data, &tree); err != nil {
		t.Fatalf("decode signaling/frames.json: %v", err)
	}
	blocks := walkAssertionBlocks("signaling/frames.json", tree)
	if len(blocks) == 0 {
		t.Fatalf("the walk found 0 assertion-bearing blocks in signaling/frames.json, which carries one per frame — "+
			"this guard would report every corpus clean (%d block names recognised)", len(assertionBearingBlockNames))
	}
	// The dart hole was NESTED, inside the elements of `frames`. A walk that
	// only looked at the fixture root would have called that corpus clean.
	nested := 0
	for _, block := range blocks {
		if strings.Contains(block.path, "[") {
			nested++
		}
	}
	if nested == 0 {
		t.Fatal("the walk found only top-level blocks in signaling/frames.json — its per-frame `assertions` live " +
			"inside an array, which is exactly the shape that was dead in lazily-dart (#lzunboundblockguard)")
	}
	t.Logf("signaling/frames.json: %d assertion-bearing blocks (%d nested in arrays)", len(blocks), nested)
}

// TestUnboundBlockReportDecides is the mutation check for the four arms of the
// decision: an unbound block fails, a bound one does not, a stale excuse fails,
// and a rotted excuse fails.
func TestUnboundBlockReportDecides(t *testing.T) {
	dir := t.TempDir()
	fixture := dir + "/probe.json"
	const doc = `{"steps":[{"expected":{"count":41}},{"expected":{"count":42}}]}`
	if err := os.WriteFile(fixture, []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	opened := map[string]string{"probe/probe.json": fixture}

	unbound := map[string]bool{}
	problems, fixtures, blocks := unboundBlockReport(opened, unbound, nil)
	if fixtures != 1 || blocks != 2 {
		t.Fatalf("walk examined %d fixture(s) / %d block(s), want 1 / 2", fixtures, blocks)
	}
	if len(problems) != 2 {
		t.Fatalf("two unbound blocks reported %d problem(s): %v", len(problems), problems)
	}
	if !strings.Contains(problems[0], "probe/probe.json") || !strings.Contains(problems[0], ".steps[0].expected") {
		t.Fatalf("the failure does not name the fixture and the block path: %q", problems[0])
	}

	// Binding both blocks clears them.
	bound := map[string]bool{}
	for _, count := range []float64{41, 42} {
		digest, ok := blockDigest("expected", map[string]any{"count": count})
		if !ok {
			t.Fatal("digest failed")
		}
		bound[digest] = true
	}
	if problems, _, _ := unboundBlockReport(opened, bound, nil); len(problems) != 0 {
		t.Fatalf("bound blocks still reported: %v", problems)
	}

	// A stale excuse — for a block that IS bound — fails.
	stale := map[string]string{"probe/probe.json .steps[0].expected": "cannot be bound"}
	problems, _, _ = unboundBlockReport(opened, bound, stale)
	if len(problems) != 1 || !strings.Contains(problems[0], "IS bound") {
		t.Fatalf("a stale excuse was accepted: %v", problems)
	}

	// An excuse naming a path the fixture does not carry fails as rotted.
	rotted := map[string]string{"probe/probe.json .steps[7].expected": "cannot be bound"}
	problems, _, _ = unboundBlockReport(opened, bound, rotted)
	if len(problems) != 1 || !strings.Contains(problems[0], "rotted") {
		t.Fatalf("a rotted excuse was accepted: %v", problems)
	}

	// An excuse with no reason fails, exactly as excuseKey's does.
	empty := map[string]string{"probe/probe.json .steps[0].expected": "  "}
	problems, _, _ = unboundBlockReport(opened, bound, empty)
	if len(problems) == 0 || !strings.Contains(problems[0], "empty reason") {
		t.Fatalf("an excuse without a reason was accepted: %v", problems)
	}
}

// TestStrictBindRecordsOnlyStructurallyCheckedBlocks pins the distinction the
// struct seam turns on: a `json.RawMessage` field checks not one key of the
// sub-tree it swallows, so crediting it would let this rung report a binding
// that never happened.
func TestStrictBindRecordsOnlyStructurallyCheckedBlocks(t *testing.T) {
	type inner struct {
		Count int `json:"count"`
	}
	type element struct {
		Expected inner           `json:"expected"`
		Opaque   json.RawMessage `json:"opaque"`
	}
	type doc struct {
		Steps []element `json:"steps"`
	}
	const raw = `{"steps":[{"expected":{"count":1},"opaque":{"expected":{"count":2}}}]}`
	var out doc
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatal(err)
	}
	// Into a LOCAL sink: publishing a probe's digests into the global ledger
	// would credit any byte-identical block the real corpus carries.
	bound := map[string]bool{}
	bindStructuralInto(func(name string, obj map[string]any) {
		if digest, ok := blockDigest(name, obj); ok {
			bound[digest] = true
		}
	}, []byte(raw), &out)
	if len(bound) == 0 {
		t.Fatal("a strict decode recorded no binding at all")
	}
	if !blockIsBound(bound, "expected", map[string]any{"count": float64(1)}) {
		t.Fatal("a struct-modelled block was not recorded as bound")
	}
	if blockIsBound(bound, "expected", map[string]any{"count": float64(2)}) {
		t.Fatal("a block beneath a json.RawMessage was credited as bound — that field checks no key of it")
	}
	// The probe's raw field carries the sub-tree the assertion above is about, so
	// it is READ here rather than declared and ignored — the shape
	// TestConformanceStructFieldsAreRead exists to reject.
	if len(out.Steps[0].Opaque) == 0 {
		t.Fatal("the probe's json.RawMessage field decoded nothing, so the assertion above proves nothing")
	}
}
