package lazily

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
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
// Four properties make the answer trustworthy:
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
//   - The MAGNITUDE of the inventory is asserted, not just its cleanliness. "No
//     unbound block" over an inventory of zero is OK reported having compared
//     nothing, and the number it is held to is DERIVED from the canonical corpus
//     minus this binding's own committed ledger — never typed. See
//     deriveBlockMagnitude (#lzblocksitepin).
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
// The ledger's EXACT SIZE, pinned (#lzledgerceiling / #lzledgerratchet)
// ---------------------------------------------------------------------------
//
// The ledger above is an EQUALITY against the run, failing in BOTH directions:
// an unbound block nobody excused fails, and an excuse the run outlived fails as
// stale or rotted. That is stronger than a one-directional allowlist and it is
// still satisfied by ANY CONSISTENT PAIR. A commit that detaches a bind AND
// writes the matching entry passes both directions, because the two sides agree
// about the new state — which is all an equality against the RUN can ask.
//
// Nothing else here sees that either. The magnitude rung (#lzblocksitepin) counts
// DECLARED sites and distinct digests; a detached bind removes neither, so 725 /
// 616 hold with the block no longer bound by anything. The coverage guard counts
// fixtures OPENED, and the fixture is still opened. This was demonstrated against
// this binding rather than argued: dropping the bind for
// `signaling/frames.json .frames[0].assertions` and adding its ledger entry left
// every other rung green.
//
// What closes it is a second comparison whose other side does NOT move with the
// run: the ledger's SIZE against a COMMITTED CONSTANT. That independence is the
// whole value. The set equality compares the ledger against the run, and under
// the attack both of its sides move together; a constant stays where the last
// reviewer left it. Ledger size is also derivable from nothing, which is what
// separates this line from a floor that merely restates a derivation.
//
// The pin is an EXACT EQUALITY, in both directions (#lzledgerratchet). It was
// first written as a CEILING (`len > pin` fails), and a ceiling SELF-DISABLES.
// It refuses the attack only while its slack is zero: after one legitimate
// migration the ledger shrinks, the constant stays put, slack becomes >= 1, and
// the same detach-plus-excuse commit passes again. Slack accumulates with every
// migration and converges on exactly the retired hand-typed block floor — 30
// against an actual 725, a number so far above the population that it never
// fired and so was never updated. That is the defect this family of rungs exists
// to replace, and a ceiling walks back into it one migration at a time.
//
// An equality has no slack by construction, and cannot drift silently, because a
// STALE VALUE FAILS. Growth means an excuse was added; shrink means sites were
// migrated and this line was not lowered in the same commit. Both are things a
// person must see, and a number that FAILS when stale is a ratchet rather than
// drift. Raising it stays legitimate — a corpus that gains a genuinely
// unreachable fixture is the real case — but it must be deliberate and visible
// in the diff, which is exactly what an equality forces and a ceiling does not.
//
// Raise it ONLY for a block that genuinely cannot be bound by any seam this
// package has, with the reason spelled in the entry, and expect to be asked why
// the capability cannot exist. Never to park a block someone means to bind later:
// that is the laundering this rung exists to refuse.
const expectedLedgeredBlocksEnv = "EXPECTED_LEDGERED_BLOCKS"

// defaultExpectedLedgeredBlocks is where this binding sits TODAY.
// unboundBlockExcuses is EMPTY — lazily-go binds every assertion-bearing block in
// every fixture it opens — so the pin is zero, landing it changes no verdict, and
// the first entry anyone adds fails until this line is raised in the same commit.
const defaultExpectedLedgeredBlocks = 0

// expectedLedgeredBlocks reads the pin. An unreadable override is a hard error
// and not a fallback to the default: a pin that cannot be read must not be
// assumed away, which is the same rule every other missing-evidence path in this
// file follows.
//
// ONE parse for the whole family (#lzpinparsestrict): a NON-EMPTY run of bare
// ASCII digits '0'-'9', and nothing else, checked BEFORE strconv runs. Ten
// bindings wrote ten readers for this one constant, so the family's real contract
// became whichever was loosest, and this reader had two of the loose parts.
// strconv.Atoi honours a sign, so "+1" was 1 (and "-1" only failed one step
// later, on the negative check). And os.Getenv cannot tell an UNSET variable
// from an explicitly EMPTY one, so with TrimSpace ahead of it both "" and " "
// silently became the committed default — `export EXPECTED_LEDGERED_BLOCKS=` and
// a typo that expanded to nothing were indistinguishable from no override at all
// to anyone reading a green run. os.LookupEnv makes that distinction, so only a
// genuinely absent variable takes the default now.
//
// Refused: empty, whitespace around or inside, a leading '+' or '-', separators,
// a radix prefix, a float or an exponent, and any non-ASCII digit. Leading zeros
// are fine and "0" stays valid — this binding pins at zero.
func expectedLedgeredBlocks() (int, error) {
	raw, present := os.LookupEnv(expectedLedgeredBlocksEnv)
	if !present {
		return defaultExpectedLedgeredBlocks, nil
	}
	if !isBareASCIIDigits(raw) {
		return 0, fmt.Errorf("%s=%q is not a non-negative integer in bare ASCII digits, so the ledger size "+
			"pin cannot be read. Falling back to the default here would let a typo silently relax a policy "+
			"line — an EMPTY value included, which os.Getenv could not have told apart from no override at "+
			"all (#lzpinparsestrict, #lzledgerratchet)",
			expectedLedgeredBlocksEnv, raw)
	}
	expected, err := strconv.Atoi(raw)
	if err != nil {
		// Unreachable for bare digits short of an overflowing value, and still
		// reported rather than assumed away.
		return 0, fmt.Errorf("%s=%q is bare digits that strconv could not read: %w (#lzpinparsestrict)",
			expectedLedgeredBlocksEnv, raw, err)
	}
	return expected, nil
}

// isBareASCIIDigits is the family's pin rule (#lzpinparsestrict): at least one
// byte, every byte in '0'-'9'. Deliberately NOT unicode.IsDigit, which is true
// for the Arabic-Indic three and every other Unicode decimal digit, and not a
// regexp \d either — Go's \d is ASCII-only but Python's is not, and lazily-dart
// had a reader built on the wrong half of that. Byte-wise, so a multi-byte rune
// cannot pass by having a digit byte in it.
func isBareASCIIDigits(raw string) bool {
	if raw == "" {
		return false
	}
	for index := 0; index < len(raw); index++ {
		if raw[index] < '0' || raw[index] > '9' {
			return false
		}
	}
	return true
}

// ledgeredSiteListCap bounds how many entries a refusal prints. The list is what
// makes the diff that moved the line legible, but a long one buries the sentence
// that says what to do, so the tail is pointed at `git diff` instead.
const ledgeredSiteListCap = 10

// ledgeredSiteList renders the ledger for a refusal, in sorted order and capped.
func ledgeredSiteList(excuses map[string]string) string {
	keys := make([]string, 0, len(excuses))
	for key := range excuses {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var detail strings.Builder
	for i, key := range keys {
		if i == ledgeredSiteListCap {
			fmt.Fprintf(&detail,
				"\n        ... and %d more — run `git diff` on unboundBlockExcuses for the rest", len(keys)-i)
			break
		}
		fmt.Fprintf(&detail, "\n        %s — %s", key, excuses[key])
	}
	return detail.String()
}

// ledgerSizeProblems is the decision, split out from the exit path so both
// directions can be mutation-checked with an ordinary test. It is an EXACT
// equality, and the refusal names WHICH DIRECTION moved, because the remedy is
// not the same one: growth means bind the block, shrink means lower the pin.
func ledgerSizeProblems(excuses map[string]string, expected int) []string {
	switch {
	case len(excuses) == expected:
		return nil
	case len(excuses) > expected:
		return []string{fmt.Sprintf(
			"the rung-0 ledger GREW: %d assertion-block site(s) are ledgered as unbound in unboundBlockExcuses, "+
				"and the pin (defaultExpectedLedgeredBlocks / %s) is %d. The set equality above cannot see this "+
				"on its own — it only checks that the ledger and the RUN agree, which any consistent pair "+
				"satisfies, because a detached bind's site is still DECLARED: the magnitude rung keeps counting "+
				"it and the coverage guard keeps opening its fixture. BIND THE BLOCK. Raise the pin to %d in "+
				"THIS commit only for a block that genuinely cannot be bound by consumeKeys or a strictJSON "+
				"struct, with the reason in the entry (#lzledgerratchet).%s",
			len(excuses), expectedLedgeredBlocksEnv, expected, len(excuses), ledgeredSiteList(excuses))}
	default:
		return []string{fmt.Sprintf(
			"the rung-0 ledger SHRANK to %d assertion-block site(s) in unboundBlockExcuses, and the pin "+
				"(defaultExpectedLedgeredBlocks / %s) is still %d. Sites were migrated and the pin was not "+
				"lowered in the same commit. LOWER THE PIN TO %d IN THIS COMMIT. A pin left above the population "+
				"is slack, and slack is exactly what let the retired `<=` ceiling admit a detach-plus-excuse "+
				"commit one migration later (#lzledgerratchet).%s",
			len(excuses), expectedLedgeredBlocksEnv, expected, len(excuses), ledgeredSiteList(excuses))}
	}
}

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
func unboundBlockReport(opened map[string]string, bound map[string]bool, excuses map[string]string) (problems []string, fixtures int, inventory blockMagnitude) {
	ids := make([]string, 0, len(opened))
	for id := range opened {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	seenPaths := map[string]bool{}
	boundPaths := map[string]bool{}
	// The digest dimension of the inventory. Sites are counted as they are
	// walked; digests dedupe by content, which is the whole reason both are
	// carried (#lzblocksitepin).
	inventoryDigests := map[string]bool{}
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
			digest, digestible := blockDigest(block.name, block.obj)
			if !digestible {
				// Booked on NEITHER dimension, exactly as deriveBlockMagnitude
				// skips it, so the two sides stay comparable.
				continue
			}
			inventory.sites++
			inventoryDigests[digest] = true
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
	inventory.digests = len(inventoryDigests)
	return problems, fixtures, inventory
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
//
// It also asserts the MAGNITUDE of what it examined, in both dimensions, against
// a number derived from the canonical corpus rather than typed here
// (#lzblocksitepin). The zero-check above is only the floor of that argument: it
// cannot tell an inventory of 725 sites from one of 12, and every rung in this
// file is scoped to the blocks the inventory holds.
func checkUnboundAssertionBlocks() bool {
	// The SIZE PIN first (#lzledgerratchet). It is a property of COMMITTED SOURCE
	// rather than of this run, so it is judged before the two early returns
	// below: a filtered run and a run that opened nothing both leave the ledger
	// exactly as large as the tree says it is, and neither is a reason for the
	// ledger to be allowed to move away from its pin unremarked.
	pinned, pinErr := expectedLedgeredBlocks()
	if pinErr != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", pinErr)
		return false
	}
	if pinProblems := ledgerSizeProblems(unboundBlockExcuses, pinned); len(pinProblems) > 0 {
		fmt.Fprintf(os.Stderr, "FAIL: %d assertion-block problem(s) before any fixture was examined:\n",
			len(pinProblems))
		for _, problem := range pinProblems {
			fmt.Fprintf(os.Stderr, "  %s\n", problem)
		}
		return false
	}

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
	problems, fixtures, inventory := unboundBlockReport(opened, bound, unboundBlockExcuses)
	if fixtures == 0 || inventory.sites == 0 || inventory.digests == 0 {
		fmt.Fprintf(os.Stderr,
			"FAIL: the unbound-block guard examined %d fixture(s) and %d assertion-bearing block(s) (%d distinct "+
				"digest(s)) after a run that opened %d conformance fixture(s) — a guard that examined nothing "+
				"reports the same green as a guard that found nothing (#lzunboundblockguard)\n",
			fixtures, inventory.sites, inventory.digests, len(opened))
		return false
	}

	// The derived magnitude (#lzblocksitepin). The check above rejects only an
	// inventory of ZERO; this one pins how big it is, in both dimensions, against
	// a number computed from the canonical corpus rather than typed here.
	expected, root, deriveErr := deriveBlockMagnitude()
	switch {
	case errors.Is(deriveErr, errNoCanonicalCorpus):
		// A contributor without the sibling checkout replays the vendored mirror
		// and is not making a false claim. Under CI the same state is missing
		// EVIDENCE and fails, exactly as an absent corpus does in
		// scripts/check-conformance-coverage.sh.
		if os.Getenv("CI") != "" {
			fmt.Fprintf(os.Stderr,
				"FAIL: the assertion-block magnitude cannot be derived and CI is set: %v. Under CI this is a "+
					"wrong checkout, not an absent corpus, and reporting the inventory OK here would be OK over "+
					"an unmeasured magnitude (#lzvacuousrun)\n", deriveErr)
			return false
		}
		fmt.Fprintf(os.Stderr,
			"NOTE: %d assertion-block site(s) / %d distinct digest(s) inventoried, magnitude NOT compared: %v. "+
				"Local checkout only — this is a hard failure under CI (#lzblocksitepin)\n",
			inventory.sites, inventory.digests, deriveErr)
	case deriveErr != nil:
		// Missing evidence, reported the way a gap is. Deriving is the only way
		// this rung knows a magnitude at all, so "could not derive" must not read
		// as "nothing to compare".
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", deriveErr)
		return false
	default:
		problems = append(problems, blockMagnitudeProblems(inventory, expected, root)...)
	}

	if len(problems) == 0 {
		// Positive evidence. Every rung above is a negative check that says
		// nothing about magnitude, so the magnitude is printed — both the live
		// inventory and the number DERIVED from the corpus listing minus
		// KNOWN_UNCOVERED. Nothing is re-pinned from this line; it is here so a
		// reader can see the two agree, and see which side moved when they do not.
		if deriveErr == nil {
			fmt.Fprintf(os.Stderr,
				"assertion-block inventory OK: %d site(s) / %d distinct digest(s) inventoried from %d opened "+
					"fixture(s) (derived from the canonical corpus: %d site(s) / %d digest(s)); %d ledgered as "+
					"unbound, pinned at exactly %d\n",
				inventory.sites, inventory.digests, fixtures, expected.sites, expected.digests,
				len(unboundBlockExcuses), pinned)
		}
		return true
	}
	fmt.Fprintf(os.Stderr, "FAIL: %d assertion-block problem(s) after a run that opened %d fixture(s):\n",
		len(problems), len(opened))
	for _, problem := range problems {
		fmt.Fprintf(os.Stderr, "  %s\n", problem)
	}
	return false
}

// ---------------------------------------------------------------------------
// Positive-evidence magnitude (#lzblocksitepin / #lzvacuousrun)
// ---------------------------------------------------------------------------
//
// Everything above this point is a NEGATIVE check: of the blocks the run
// inventoried, none was unbound. It says nothing about HOW MANY there were, and
// zero inventoried blocks means zero unbound blocks — OK reported having
// compared nothing. The `fixtures == 0 || sites == 0` guard in
// checkUnboundAssertionBlocks rejects only the floor of that: it cannot tell an
// inventory of 725 from one of 12.
//
// So the magnitude is asserted, and it is DERIVED and an EQUALITY. Two things
// make each of those non-negotiable.
//
// Derived, not typed. A `MIN_BLOCKS` literal re-pinned by hand after reading a
// log lags the corpus by however long nobody reads the log. lazily-py's pin sat
// at 578 from 2026-08-11 while the real inventory was 620: 42 blocks could have
// stopped being inventoried with the rung still green. The two inputs here both
// move on their own — the canonical corpus listing, and this binding's own
// committed KNOWN_UNCOVERED ledger — so corpus MINUS ledger is computed, never
// transcribed. A fixture landing upstream moves this number with no edit here; a
// fixture this binding stops opening moves it only through a committed ledger
// line.
//
// EQUALITY, not `>=`. A floor cannot see slack, and slack is what rots a pin.
//
// TWO dimensions, from ONE walk, because neither subsumes the other:
//
//   - SITES — one per "<fixture> <json path>", so it counts what the corpus
//     CARRIES. A block whose content digest recurs elsewhere can be deleted
//     outright and the digest count does not move; this is the number that sees
//     it.
//   - DISTINCT DIGESTS — what the corpus SAYS, deduplicated. A content edit that
//     collapses two distinct claims into one spelling leaves every site in place
//     and this is the number that sees it.
//
// The walk is walkAssertionBlocks — the SAME function the inventory side uses —
// so the two sides cannot disagree about what counts as a block, and both
// dimensions come out of one traversal. That is also why this binding derives a
// different number from a sibling over an almost identical opened set: this walk
// reads three block names and object-valued blocks only.
//
// What it deliberately does NOT read: openedFixturePaths, boundBlocks, or
// anything else this run produced. An expectation derived from what the run read
// goes to zero alongside the count it is compared against the moment the
// recorder detaches, and the rung is vacuously green again.

// blockMagnitude is the two-dimensional size of an assertion-block population.
type blockMagnitude struct {
	sites   int
	digests int
}

// errNoCanonicalCorpus reports an absent lazily-spec sibling checkout. Split out
// from every other derivation failure because it is the one that is legitimate
// locally: a contributor without the sibling is not making a false claim. Under
// CI it is missing EVIDENCE and fails, exactly as an absent corpus does in
// scripts/check-conformance-coverage.sh.
var errNoCanonicalCorpus = errors.New("no canonical lazily-spec conformance corpus")

// coverageScriptCandidates spells the path to this binding's committed ledger.
func coverageScriptCandidates() []string {
	out := []string{filepath.Join("scripts", "check-conformance-coverage.sh")}
	if _, file, _, ok := runtime.Caller(0); ok {
		out = append(out, filepath.Join(filepath.Dir(file), "scripts", "check-conformance-coverage.sh"))
	}
	return out
}

// knownUncoveredFixtures reads the corpus-relative paths of the fixtures this
// binding does not open, out of the bash `KNOWN_UNCOVERED=( ... )` array in
// scripts/check-conformance-coverage.sh.
//
// Parsed rather than restated in Go on purpose. A second copy would be one more
// thing to re-pin by hand, which is the defect this whole seam removes, and the
// array has to stay in the script anyway: lazily-spec's check-corpus-floors.mjs
// classifies the ledger arrays declared there and fails on an unclassified one.
//
// Every failure here is HARD. Treating an unreadable or unmatched array as empty
// would derive a LARGER expectation, from a set the suite never opens, and
// report the guard's own blindness as a corpus problem.
func knownUncoveredFixtures() (map[string]bool, error) {
	var (
		text  string
		found string
	)
	for _, candidate := range coverageScriptCandidates() {
		data, err := os.ReadFile(candidate)
		if err == nil {
			text, found = string(data), candidate
			break
		}
	}
	if found == "" {
		return nil, fmt.Errorf(
			"cannot read %s, which holds the KNOWN_UNCOVERED ledger the assertion-block "+
				"expectation is derived from", coverageScriptCandidates()[0])
	}
	const marker = "\nKNOWN_UNCOVERED=(\n"
	start := strings.Index(text, marker)
	if start < 0 {
		return nil, fmt.Errorf(
			"%s no longer declares a KNOWN_UNCOVERED=( array. The assertion-block "+
				"expectation is derived from it, so a rename has to be mirrored here rather "+
				"than quietly deriving over a different set", found)
	}
	start += len(marker)
	end := strings.Index(text[start:], "\n)\n")
	if end < 0 {
		return nil, fmt.Errorf(
			"%s: the KNOWN_UNCOVERED=( array is never closed by a line holding only ')'", found)
	}
	entries := map[string]bool{}
	for _, line := range strings.Split(text[start:start+end], "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		for {
			open := strings.Index(trimmed, `"`)
			if open < 0 {
				break
			}
			rest := trimmed[open+1:]
			close := strings.Index(rest, `"`)
			if close < 0 {
				break
			}
			entries[rest[:close]] = true
			trimmed = rest[close+1:]
		}
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf(
			"%s: KNOWN_UNCOVERED parsed as EMPTY. Shrinking that list to nothing is the "+
				"goal state, but so is a parser that has stopped matching its entries, and "+
				"the two are indistinguishable from here. If the list is genuinely empty, "+
				"relax this check deliberately", found)
	}
	return entries, nil
}

// Derived expectations, cached by resolved canonical root.
var (
	derivedMagnitudeMu    sync.Mutex
	derivedMagnitudeCache = map[string]blockMagnitude{}
)

// deriveBlockMagnitude returns the (sites, digests) the fixtures this binding
// opens really carry, and the canonical root it was derived from.
//
// CANONICAL corpus minus knownUncoveredFixtures, walked with
// walkAssertionBlocks. The fixtures are read with os.ReadFile and NOT
// specReadFile: specReadFile books every read into openedFixturePaths and the
// prose ledger, so deriving through it would inventory the whole corpus as
// "opened" and the expectation would then be compared against an inventory it
// had just populated itself.
//
// Either dimension deriving as ZERO is a hard failure. Zero compares equal to an
// inventory of zero, which is the vacuous green this rung exists to reject,
// reached from the expectation side instead of the inventory side.
func deriveBlockMagnitude() (blockMagnitude, string, error) {
	root, ok := canonicalSpecRoot()
	if !ok {
		return blockMagnitude{}, "", fmt.Errorf(
			"%w at %s: pointing %s somewhere else does not substitute for it — these "+
				"numbers are what the run is judged AGAINST, not what it replayed",
			errNoCanonicalCorpus, canonicalSpecRootCandidates()[0], specDirEnv)
	}
	derivedMagnitudeMu.Lock()
	cached, hit := derivedMagnitudeCache[root]
	derivedMagnitudeMu.Unlock()
	if hit {
		return cached, root, nil
	}

	excused, err := knownUncoveredFixtures()
	if err != nil {
		return blockMagnitude{}, root, err
	}

	var relatives []string
	walkErr := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			return nil
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		relatives = append(relatives, filepath.ToSlash(rel))
		return nil
	})
	if walkErr != nil {
		return blockMagnitude{}, root, fmt.Errorf(
			"cannot derive the assertion-block expectation: listing the canonical corpus at "+
				"%s failed: %w. A corpus we cannot list is missing evidence, not a corpus "+
				"carrying no blocks", root, walkErr)
	}
	sort.Strings(relatives)

	sites := 0
	digests := map[string]bool{}
	for _, rel := range relatives {
		if excused[rel] {
			continue
		}
		data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			return blockMagnitude{}, root, fmt.Errorf(
				"cannot derive the assertion-block expectation: %s under %s is unreadable "+
					"(%w). A fixture we cannot read is missing evidence, not a fixture carrying "+
					"no blocks", rel, root, err)
		}
		var tree any
		if err := json.Unmarshal(data, &tree); err != nil {
			return blockMagnitude{}, root, fmt.Errorf(
				"cannot derive the assertion-block expectation: %s under %s is not JSON (%w)",
				rel, root, err)
		}
		for _, block := range walkAssertionBlocks(rel, tree) {
			digest, ok := blockDigest(block.name, block.obj)
			if !ok {
				// Booked on NEITHER side. unboundBlockReport skips an
				// undigestible block the same way, so the two dimensions stay
				// comparable.
				continue
			}
			sites++
			digests[digest] = true
		}
	}
	if sites == 0 || len(digests) == 0 {
		return blockMagnitude{}, root, fmt.Errorf(
			"the assertion-block expectation derived as %d site(s) / %d digest(s) from the "+
				"canonical corpus at %s minus the KNOWN_UNCOVERED ledger. Zero of either is "+
				"not a corpus with nothing to check: zero compares EQUAL to an inventory of "+
				"zero, which is exactly the vacuous green this rung exists to reject, reached "+
				"from the expectation side. Either the corpus is empty, the ledger now excuses "+
				"every fixture, or walkAssertionBlocks has stopped matching the fixtures it "+
				"walks", sites, len(digests), root)
	}

	total := blockMagnitude{sites: sites, digests: len(digests)}
	derivedMagnitudeMu.Lock()
	derivedMagnitudeCache[root] = total
	derivedMagnitudeMu.Unlock()
	return total, root, nil
}

// blockMagnitudeProblems compares the inventory this run booked against the
// expectation derived from the canonical corpus, in BOTH dimensions, for
// EQUALITY. Split out from the process exit path so every arm is
// mutation-checkable with an ordinary test.
func blockMagnitudeProblems(inventory, expected blockMagnitude, root string) []string {
	var problems []string
	direction := func(short int) string {
		if short > 0 {
			return fmt.Sprintf("%d FEWER than the canonical corpus owes", short)
		}
		return fmt.Sprintf("%d MORE than the canonical corpus owes", -short)
	}
	if inventory.sites != expected.sites {
		problems = append(problems, fmt.Sprintf(
			"%d assertion-block SITE(s) were inventoried, expected exactly %d — %s. A site is "+
				"one '<fixture> <json path>', so this dimension counts what the corpus CARRIES "+
				"rather than what it distinctly SAYS: a block whose content digest recurs "+
				"elsewhere can be deleted with the digest count unmoved, and this is the number "+
				"that sees it. The expectation is derived from the canonical corpus at %s minus "+
				"the KNOWN_UNCOVERED ledger in scripts/check-conformance-coverage.sh, by the "+
				"same walk the inventory uses. Two plain causes: either the CORPUS MOVED — "+
				"re-pull the lazily-spec sibling, and say so in KNOWN_UNCOVERED if this binding "+
				"now opens a different set; a doctored or older copy reached through %s shows up "+
				"here, which is the point — or the READ-TIME RECORDER DETACHED. There is no "+
				"number to re-pin (#lzblocksitepin)",
			inventory.sites, expected.sites, direction(expected.sites-inventory.sites), root, specDirEnv))
	}
	if inventory.digests != expected.digests {
		problems = append(problems, fmt.Sprintf(
			"%d distinct assertion-block DIGEST(s) were inventoried, expected exactly %d — %s. "+
				"This dimension counts what the corpus SAYS, deduplicated by content, so it "+
				"moves when a fixture changes what a block claims without moving any site — a "+
				"content edit that collapses two distinct claims into one spelling is invisible "+
				"to the site count above. The expectation is derived from the canonical corpus "+
				"at %s minus the KNOWN_UNCOVERED ledger in "+
				"scripts/check-conformance-coverage.sh, by the same walk the inventory uses. "+
				"Same two causes as the site count: the CORPUS MOVED, or the READ-TIME RECORDER "+
				"DETACHED. There is no number to re-pin (#lzblocksitepin)",
			inventory.digests, expected.digests, direction(expected.digests-inventory.digests), root))
	}
	return problems
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
	problems, fixtures, inventory := unboundBlockReport(opened, unbound, nil)
	if fixtures != 1 || inventory.sites != 2 {
		t.Fatalf("walk examined %d fixture(s) / %d site(s), want 1 / 2", fixtures, inventory.sites)
	}
	// The two blocks say DIFFERENT things ({"count":41} and {"count":42}), so the
	// digest dimension sees two as well. The probe below with identical content
	// is what separates the dimensions.
	if inventory.digests != 2 {
		t.Fatalf("walk inventoried %d distinct digest(s), want 2", inventory.digests)
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

// TestKnownUncoveredLedgerParses is the positive evidence for the ledger half of
// the derivation. A parser that silently stopped matching the bash array would
// derive a LARGER expectation from a set the suite never opens, and the failure
// would read as a corpus problem rather than as the guard's own blindness.
func TestKnownUncoveredLedgerParses(t *testing.T) {
	excused, err := knownUncoveredFixtures()
	if err != nil {
		t.Fatalf("parsing the KNOWN_UNCOVERED ledger: %v", err)
	}
	if len(excused) == 0 {
		t.Fatal("the ledger parsed as empty, which knownUncoveredFixtures is supposed to refuse")
	}
	// Every entry is a corpus-relative fixture path, not a scenario excuse
	// ("fixture|id|reason") and not a stray quoted word from a neighbouring
	// array: the parser is bounded to one array, and this is what proves it.
	for entry := range excused {
		if !strings.HasSuffix(entry, ".json") || strings.Contains(entry, "|") {
			t.Errorf("KNOWN_UNCOVERED entry %q is not a corpus-relative fixture path — the parser has "+
				"widened past the array it is bounded to", entry)
		}
	}
	root, ok := canonicalSpecRoot()
	if !ok {
		t.Skip("no canonical lazily-spec sibling checkout")
	}
	// A ledger entry naming a fixture the corpus no longer carries excuses
	// nothing, so it INFLATES the derived expectation by exactly the blocks it
	// fails to exclude. Named here rather than left to surface as a magnitude
	// mismatch nobody can attribute.
	for entry := range excused {
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(entry))); err != nil {
			t.Errorf("KNOWN_UNCOVERED names %q, which the canonical corpus at %s does not carry (%v) — "+
				"the entry has rotted and now excuses nothing", entry, root, err)
		}
	}
	t.Logf("KNOWN_UNCOVERED: %d fixture(s) this binding does not open", len(excused))
}

// TestDerivedBlockMagnitudeIsNotVacuous is the positive evidence for the
// expectation side itself, and for the one property that makes the comparison
// worth making: the expectation must come from somewhere OTHER than the run.
func TestDerivedBlockMagnitudeIsNotVacuous(t *testing.T) {
	if _, ok := canonicalSpecRoot(); !ok {
		t.Skip("no canonical lazily-spec sibling checkout")
	}
	// Derive from cold, so the side-effect assertion below is not answered by a
	// cached value another test warmed.
	root, _ := canonicalSpecRoot()
	derivedMagnitudeMu.Lock()
	delete(derivedMagnitudeCache, root)
	derivedMagnitudeMu.Unlock()

	boundBlocksMu.Lock()
	openedBefore := len(openedFixturePaths)
	boundBefore := len(boundBlocks)
	boundBlocksMu.Unlock()

	expected, derivedRoot, err := deriveBlockMagnitude()
	if err != nil {
		t.Fatalf("deriving the assertion-block magnitude: %v", err)
	}
	if expected.sites == 0 || expected.digests == 0 {
		t.Fatalf("the expectation derived as %d site(s) / %d digest(s) — zero compares EQUAL to an "+
			"inventory of zero", expected.sites, expected.digests)
	}
	// The derivation must not BOOK what it reads. specReadFile records every
	// conformance read into openedFixturePaths (and the prose ledger), so a
	// derivation routed through it would inventory the whole canonical corpus as
	// opened and then compare the expectation against an inventory it had just
	// populated itself — agreeing with itself whatever the suite replayed.
	boundBlocksMu.Lock()
	openedAfter := len(openedFixturePaths)
	boundAfter := len(boundBlocks)
	boundBlocksMu.Unlock()
	if openedAfter != openedBefore {
		t.Errorf("deriving the expectation booked %d fixture(s) as OPENED (%d -> %d) — the expectation is "+
			"now derived from what the run read, which goes to zero alongside the inventory the moment "+
			"the recorder detaches", openedAfter-openedBefore, openedBefore, openedAfter)
	}
	if boundAfter != boundBefore {
		t.Errorf("deriving the expectation recorded %d BINDING(s) (%d -> %d) — the expectation would then "+
			"clear the very blocks it is supposed to demand a binding for", boundAfter-boundBefore, boundBefore, boundAfter)
	}
	// The two dimensions must be genuinely different numbers on this corpus. If
	// every site carried a unique digest the site count would be observationally
	// identical to the digest count, and #lzblocksitepin's whole argument — that
	// deleting a RECURRING block moves one and not the other — would have no
	// witness here.
	if expected.digests >= expected.sites {
		t.Fatalf("derived %d site(s) and %d distinct digest(s): the digest count does not dedupe anything "+
			"on this corpus, so the two dimensions cannot be shown to be independent from here",
			expected.sites, expected.digests)
	}
	t.Logf("derived from %s: %d site(s) / %d distinct digest(s) (%d site(s) carry a recurring shape)",
		derivedRoot, expected.sites, expected.digests, expected.sites-expected.digests)
}

// TestBlockMagnitudeProblemsDecides is the mutation check for the comparison:
// each dimension must fail on its own, in both directions, and an exact match
// must report nothing.
func TestBlockMagnitudeProblemsDecides(t *testing.T) {
	expected := blockMagnitude{sites: 725, digests: 616}
	if problems := blockMagnitudeProblems(expected, expected, "/corpus"); len(problems) != 0 {
		t.Fatalf("an exact match reported %d problem(s): %v", len(problems), problems)
	}
	for _, testCase := range []struct {
		name      string
		inventory blockMagnitude
		wants     []string
	}{
		// A block whose digest RECURS was lost: the site count drops and the
		// digest count cannot see it. This is the arm a digest-only equality
		// misses entirely.
		{"site short", blockMagnitude{sites: 724, digests: 616}, []string{"SITE(s) were inventoried, expected exactly 725", "1 FEWER"}},
		{"site over", blockMagnitude{sites: 726, digests: 616}, []string{"SITE(s) were inventoried, expected exactly 725", "1 MORE"}},
		// A unique-digest block was collapsed into another's spelling: every
		// site is still there and the digest count drops. This is the arm a
		// site-only equality misses entirely.
		{"digest short", blockMagnitude{sites: 725, digests: 615}, []string{"DIGEST(s) were inventoried, expected exactly 616", "1 FEWER"}},
		{"digest over", blockMagnitude{sites: 725, digests: 617}, []string{"DIGEST(s) were inventoried, expected exactly 616", "1 MORE"}},
	} {
		problems := blockMagnitudeProblems(testCase.inventory, expected, "/corpus")
		if len(problems) != 1 {
			t.Fatalf("%s: reported %d problem(s), want exactly 1 (the OTHER dimension must stay silent, "+
				"which is what makes it an independent signal): %v", testCase.name, len(problems), problems)
		}
		for _, want := range testCase.wants {
			if !strings.Contains(problems[0], want) {
				t.Errorf("%s: report does not mention %q: %s", testCase.name, want, problems[0])
			}
		}
	}
	// Both dimensions moving reports both, so a run cannot fix one and infer the
	// other was fine.
	if problems := blockMagnitudeProblems(blockMagnitude{sites: 700, digests: 600}, expected, "/corpus"); len(problems) != 2 {
		t.Fatalf("both dimensions moved and %d problem(s) were reported, want 2: %v", len(problems), problems)
	}
}

// TestLedgerSizeEqualityDecides is the mutation check for the size pin
// (#lzledgerratchet): it stays silent only at the pin, fires one entry OVER it,
// fires one entry UNDER it, and names the direction plus both numbers so a reader
// does not have to count the map to learn which side moved.
//
// The UNDER case is the point of this test. Under the retired `<=` ceiling a
// ledger below the line exited 0, which is how the ceiling accumulated slack and
// re-admitted the very commit it was written to refuse.
func TestLedgerSizeEqualityDecides(t *testing.T) {
	one := map[string]string{"probe/probe.json .steps[0].expected": "no seam can reach it"}
	two := map[string]string{
		"probe/probe.json .steps[0].expected": "no seam can reach it",
		"probe/probe.json .steps[1].expected": "nor this one",
	}

	if problems := ledgerSizeProblems(nil, 0); len(problems) != 0 {
		t.Fatalf("an empty ledger tripped a pin of 0: %v", problems)
	}
	if problems := ledgerSizeProblems(one, 1); len(problems) != 0 {
		t.Fatalf("a ledger AT its pin was refused: %v", problems)
	}
	if problems := ledgerSizeProblems(two, 2); len(problems) != 0 {
		t.Fatalf("a deliberately raised pin still refused its own population: %v", problems)
	}

	// GROWTH: an excuse was added. This is the direction the ceiling also caught.
	problems := ledgerSizeProblems(one, 0)
	if len(problems) != 1 {
		t.Fatalf("one entry over a pin of 0 reported %d problem(s), want 1: %v", len(problems), problems)
	}
	for _, want := range []string{"GREW", "1 assertion-block site(s) are ledgered", "is 0", "BIND THE BLOCK",
		"still DECLARED", "probe/probe.json .steps[0].expected", "no seam can reach it",
		expectedLedgeredBlocksEnv} {
		if !strings.Contains(problems[0], want) {
			t.Errorf("the growth refusal does not mention %q: %s", want, problems[0])
		}
	}
	if strings.Contains(problems[0], "SHRANK") {
		t.Errorf("the growth refusal reported the wrong direction: %s", problems[0])
	}

	// SHRINK: sites were migrated and the pin was not lowered with them. An
	// EQUALITY refuses this; a ceiling cannot, and that is the whole change.
	problems = ledgerSizeProblems(nil, 1)
	if len(problems) != 1 {
		t.Fatalf("an empty ledger under a pin of 1 reported %d problem(s), want 1: %v", len(problems), problems)
	}
	for _, want := range []string{"SHRANK to 0", "still 1", "LOWER THE PIN TO 0 IN THIS COMMIT",
		expectedLedgeredBlocksEnv} {
		if !strings.Contains(problems[0], want) {
			t.Errorf("the shrink refusal does not mention %q: %s", want, problems[0])
		}
	}
	if strings.Contains(problems[0], "GREW") {
		t.Errorf("the shrink refusal reported the wrong direction: %s", problems[0])
	}

	// A non-empty ledger below its pin is the same refusal, and it still lists
	// what SURVIVED, so the reviewer can see which entry the migration removed.
	problems = ledgerSizeProblems(one, 2)
	if len(problems) != 1 || !strings.Contains(problems[0], "SHRANK to 1") ||
		!strings.Contains(problems[0], "LOWER THE PIN TO 1") ||
		!strings.Contains(problems[0], "probe/probe.json .steps[0].expected") {
		t.Fatalf("one entry under a pin of 2 was not refused with both numbers and the survivor: %v", problems)
	}

	// It is the SIZE that is judged, not the identity of any entry: a second
	// entry under the same pin is refused the same way, and every entry is
	// listed so the diff that moved the line is legible.
	problems = ledgerSizeProblems(two, 1)
	if len(problems) != 1 || !strings.Contains(problems[0], "2 assertion-block site(s) are ledgered") ||
		!strings.Contains(problems[0], "is 1") {
		t.Fatalf("two entries over a pin of 1 were not refused with both numbers: %v", problems)
	}
	if !strings.Contains(problems[0], ".steps[0].expected") || !strings.Contains(problems[0], ".steps[1].expected") {
		t.Fatalf("the refusal does not list every ledgered site: %s", problems[0])
	}
}

// TestLedgerSiteListIsCapped pins the tail of a large refusal. The site list is
// what makes a moved pin legible, and an uncapped one buries the sentence that
// says what to do, so past the cap the reader is pointed at `git diff` instead.
func TestLedgerSiteListIsCapped(t *testing.T) {
	big := map[string]string{}
	for i := 0; i < ledgeredSiteListCap+3; i++ {
		big[fmt.Sprintf("probe/probe.json .steps[%02d].expected", i)] = "no seam can reach it"
	}
	problems := ledgerSizeProblems(big, 0)
	if len(problems) != 1 {
		t.Fatalf("a ledger of %d over a pin of 0 reported %d problem(s), want 1", len(big), len(problems))
	}
	listed := strings.Count(problems[0], " — no seam can reach it")
	if listed != ledgeredSiteListCap {
		t.Fatalf("the refusal listed %d site(s), want the cap of %d: %s", listed, ledgeredSiteListCap, problems[0])
	}
	if !strings.Contains(problems[0], "... and 3 more") || !strings.Contains(problems[0], "git diff") {
		t.Fatalf("the capped refusal does not point at the rest: %s", problems[0])
	}
	// The cap is a display bound, never a decision bound: the COUNT is still the
	// full population, or a ledger past the cap could hide entries from the pin.
	if !strings.Contains(problems[0], fmt.Sprintf("%d assertion-block site(s) are ledgered", len(big))) {
		t.Fatalf("the capped refusal does not report the full population: %s", problems[0])
	}
}

// TestExpectedLedgeredBlocksReadsThePin pins the override seam. A pin that
// silently fell back to the default on a malformed value would let a typo relax
// a policy line, which is the failure mode the hard error exists for.
//
// Only an UNSET variable takes the default. An explicitly EMPTY one is in the
// rejection set below, not here: this test used to call t.Setenv(env, "") "unset"
// and assert the default, which is exactly the conflation os.Getenv forced and
// os.LookupEnv removes (#lzpinparsestrict).
func TestExpectedLedgeredBlocksReadsThePin(t *testing.T) {
	// t.Setenv first so the cleanup restores whatever the caller had, then
	// actually remove it — t.Setenv cannot express absence.
	t.Setenv(expectedLedgeredBlocksEnv, "0")
	if err := os.Unsetenv(expectedLedgeredBlocksEnv); err != nil {
		t.Fatalf("unsetting %s: %v", expectedLedgeredBlocksEnv, err)
	}
	pinned, err := expectedLedgeredBlocks()
	if err != nil || pinned != defaultExpectedLedgeredBlocks {
		t.Fatalf("unset: pin=%d err=%v, want %d / nil", pinned, err, defaultExpectedLedgeredBlocks)
	}

	// Leading zeros are fine and "0" is valid: five bindings in this family pin
	// at zero, so a rule that refused it would be unusable there.
	for raw, want := range map[string]int{"3": 3, "007": 7, "0": 0} {
		t.Setenv(expectedLedgeredBlocksEnv, raw)
		if pinned, err := expectedLedgeredBlocks(); err != nil || pinned != want {
			t.Fatalf("override %q: pin=%d err=%v, want %d / nil", raw, pinned, err, want)
		}
	}

	// The family's rejection set. " 3 " and "+1" were ACCEPTED here before
	// (TrimSpace, then strconv.Atoi honouring the sign) and "" fell through to
	// the default — three pins nobody typed. None was a hole, since a wrong pin
	// fails the equality loudly; the point is that ten bindings disagreed about
	// which of these were pins at all.
	for _, bad := range []string{"lots", "1.5", "-1", "", " ", "1_0", " 3 ", "+1", "1.0", "0x19", "\u0663"} {
		t.Setenv(expectedLedgeredBlocksEnv, bad)
		_, err := expectedLedgeredBlocks()
		if err == nil {
			t.Fatalf("%s=%q was accepted; an unreadable pin must not fall back to the default",
				expectedLedgeredBlocksEnv, bad)
		}
		if !strings.Contains(err.Error(), fmt.Sprintf("%q", bad)) {
			t.Fatalf("%s=%q was refused without being NAMED: %v. A reader who cannot see what was "+
				"rejected cannot tell a typo from a policy change", expectedLedgeredBlocksEnv, bad, err)
		}
	}
}

// TestThisBindingsLedgerMatchesItsPin asserts the committed state, so the pin is
// not merely a function nothing calls with the real ledger. It runs under any
// filter, because the committed ledger does not depend on which tests a run
// selects.
func TestThisBindingsLedgerMatchesItsPin(t *testing.T) {
	pinned, err := expectedLedgeredBlocks()
	if err != nil {
		t.Fatalf("reading the pin: %v", err)
	}
	if problems := ledgerSizeProblems(unboundBlockExcuses, pinned); len(problems) != 0 {
		t.Fatalf("this binding's ledger does not match its own pin: %v", problems)
	}
	t.Logf("unboundBlockExcuses: %d entry(ies), pinned at exactly %d", len(unboundBlockExcuses), pinned)
}
