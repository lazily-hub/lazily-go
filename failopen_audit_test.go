package lazily

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// Fail-open dispatch audit of the LIBRARY sources (not the conformance
// runners). Every site the audit called out gets one of two tests here:
//
//   - a REJECTION test, for a site converted to fail closed; and
//   - a PINNING test, for a site whose leniency IS the wire contract.
//
// The second kind is the load-bearing one. An undocumented default and a
// deliberate one are indistinguishable from outside the package; the pin is
// what makes them distinguishable, and what makes a later "tidy-up" that
// removes the leniency show up as a red test rather than a silent wire break.

// ---------------------------------------------------------------------------
// FAIL CLOSED — text-CRDT delta-sync wire decode (text_crdt.go)
// ---------------------------------------------------------------------------

// TestOpIdFromWireRejectsNonIntegerFields proves the decode path no longer
// coerces a malformed `counter`/`peer` to 0.
//
// Zero is not a neutral default here: nextId mints counters from 1, so
// OpId{0,0} is exactly textRootKey — the byOrigin bucket for elements whose
// origin is the document start. Before the conversion, `{"counter":"x"}` and
// `{}` both produced the document-root sentinel and merged into visible text.
func TestOpIdFromWireRejectsNonIntegerFields(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want string
	}{
		{"counter is a string", map[string]any{"counter": "7", "peer": 1.0}, "OpId.counter"},
		{"counter is absent", map[string]any{"peer": 1.0}, "OpId.counter"},
		{"counter is null", map[string]any{"counter": nil, "peer": 1.0}, "OpId.counter"},
		{"counter is a bool", map[string]any{"counter": true, "peer": 1.0}, "OpId.counter"},
		{"counter is fractional", map[string]any{"counter": 1.5, "peer": 1.0}, "OpId.counter"},
		{"peer is a string", map[string]any{"counter": 1.0, "peer": "me"}, "OpId.peer"},
		{"peer is absent", map[string]any{"counter": 1.0}, "OpId.peer"},
		{"not an object", []any{1, 2}, "must be a JSON object"},
		{"json.Number is fractional", map[string]any{
			"counter": json.Number("1.5"), "peer": json.Number("1"),
		}, "OpId.counter"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := OpIdFromWire(c.in)
			if err == nil {
				t.Fatalf("OpIdFromWire(%v) accepted, returning %+v; want a rejection", c.in, got)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %q does not name %q", err.Error(), c.want)
			}
			// The rejection must not hand back the root sentinel either.
			if got != (OpId{}) {
				t.Fatalf("rejected decode returned %+v, want the zero value", got)
			}
			if got == textRootKey && err == nil {
				t.Fatal("a malformed OpId resolved to textRootKey")
			}
		})
	}
}

// TestOpIdFromWireAcceptsEveryNumericEncoding pins the ACCEPTING half: the
// three shapes a JSON decoder can hand us for an integer all still decode.
// Without this, "reject everything" would pass the rejection test above.
func TestOpIdFromWireAcceptsEveryNumericEncoding(t *testing.T) {
	cases := []struct {
		name string
		in   any
	}{
		{"float64 (encoding/json default)", map[string]any{"counter": 42.0, "peer": 7.0}},
		{"json.Number (UseNumber)", map[string]any{"counter": json.Number("42"), "peer": json.Number("7")}},
		{"native int64", map[string]any{"counter": int64(42), "peer": int64(7)}},
		{"native int", map[string]any{"counter": 42, "peer": 7}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := OpIdFromWire(c.in)
			if err != nil {
				t.Fatalf("OpIdFromWire(%v): %v", c.in, err)
			}
			if got != (OpId{Counter: 42, Peer: 7}) {
				t.Fatalf("got %+v, want {42 7}", got)
			}
		})
	}
}

// TestTextOpFromWireRejectsMalformedOps covers the op envelope around OpId:
// a non-object op, a missing/non-string `ch`, and a malformed `origin` or
// `deleted`. `origin`/`deleted` stay lenient for an explicit null, which is
// the wire spelling of "document start" / "live".
func TestTextOpFromWireRejectsMalformedOps(t *testing.T) {
	goodID := map[string]any{"counter": 1.0, "peer": 2.0}
	bad := map[string]any{"counter": "nope", "peer": 2.0}

	cases := []struct {
		name string
		in   any
		want string
	}{
		{"not an object", "op", "must be a JSON object"},
		{"id is malformed", map[string]any{"id": bad, "ch": "a"}, "TextOp.id"},
		{"ch is absent", map[string]any{"id": goodID}, "TextOp.ch"},
		{"ch is not a string", map[string]any{"id": goodID, "ch": 3.0}, "TextOp.ch"},
		{"origin is malformed", map[string]any{"id": goodID, "ch": "a", "origin": bad}, "TextOp.origin"},
		{"deleted is malformed", map[string]any{"id": goodID, "ch": "a", "deleted": bad}, "TextOp.deleted"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if _, err := TextOpFromWire(c.in); err == nil {
				t.Fatalf("TextOpFromWire(%v) accepted; want a rejection", c.in)
			} else if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %q does not name %q", err.Error(), c.want)
			}
		})
	}
}

// TestTextOpWireRoundTripSurvives is the accepting counterpart: a well-formed
// op still round-trips through ToWire/FromWire, including the null origin and
// deleted spellings.
func TestTextOpWireRoundTripSurvives(t *testing.T) {
	origin := OpId{Counter: 3, Peer: 9}
	deleted := OpId{Counter: 11, Peer: 4}
	for _, op := range []TextOp{
		{Id: OpId{Counter: 1, Peer: 2}, Ch: "a"},
		{Id: OpId{Counter: 5, Peer: 6}, Ch: "b", Origin: &origin},
		{Id: OpId{Counter: 7, Peer: 8}, Ch: "c", Origin: &origin, Deleted: &deleted},
	} {
		wire := op.ToWire()
		// Force the op through a real JSON round trip so the numeric fields
		// arrive as float64 the way a peer's frame would.
		encoded, err := json.Marshal(wire)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var decoded any
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		got, err := TextOpFromWire(decoded)
		if err != nil {
			t.Fatalf("TextOpFromWire(%s): %v", encoded, err)
		}
		if got.Id != op.Id || got.Ch != op.Ch {
			t.Fatalf("round trip of %s lost the id/ch: %+v", encoded, got)
		}
		if (got.Origin == nil) != (op.Origin == nil) ||
			(got.Origin != nil && *got.Origin != *op.Origin) {
			t.Fatalf("round trip of %s lost origin: %+v", encoded, got.Origin)
		}
		if (got.Deleted == nil) != (op.Deleted == nil) ||
			(got.Deleted != nil && *got.Deleted != *op.Deleted) {
			t.Fatalf("round trip of %s lost deleted: %+v", encoded, got.Deleted)
		}
	}
}

// ---------------------------------------------------------------------------
// FAIL CLOSED — state-chart `kind` derivation (state_chart.go parseState)
// ---------------------------------------------------------------------------

// TestStateChartKindStrictness covers both halves of the `kind` contract after
// the leniency here was overturned (#failclosedsweep).
//
// This site used to be pinned as INTENTIONAL on the argument that `kind` is an
// open annotation slot and that lazily-rs ends the same ladder in
// `Kind::Atomic`. Both premises are gone: `schemas/statechart.json` closes
// `kind` to {atomic, compound, parallel, history, final} under
// `additionalProperties: false`, and lazily-rs, -dart, -js, -kt and -cs now
// reject an unrecognised value by name. A chart is never serialized over
// IPC/FFI, so there was never a wire forward-compat argument to make.
//
// The OMITTED case is unchanged and is what the schema documents as inferred —
// that half still has to work, or "reject everything" would pass the rejections.
func TestStateChartKindStrictness(t *testing.T) {
	// --- the rejections -----------------------------------------------------

	rejects := []struct {
		name string
		doc  string
		want string
	}{
		{
			"unknown kind annotation",
			`{"initial": "root", "states": {
				"root": {"initial": "leaf"},
				"leaf": {"parent": "root", "kind": "vendor-annotation-from-a-newer-spec"}
			}}`,
			`unknown kind "vendor-annotation-from-a-newer-spec"`,
		},
		{
			"a typo of a legal kind",
			`{"initial": "root", "states": {"root": {"kind": "finall"}}}`,
			`unknown kind "finall"`,
		},
		{
			"unknown kind on a compound state",
			`{"initial": "root", "states": {
				"root": {"kind": "machine", "initial": "leaf"},
				"leaf": {"parent": "root"}
			}}`,
			`unknown kind "machine"`,
		},
		{
			"unknown kind does not hide behind parallel",
			`{"initial": "root", "states": {
				"root": {"parallel": true, "kind": "unrecognised"},
				"a":    {"parent": "root"},
				"b":    {"parent": "root"}
			}}`,
			`unknown kind "unrecognised"`,
		},
		{
			"non-string kind",
			`{"initial": "root", "states": {"root": {"kind": 7}}}`,
			"`kind` must be a string",
		},
		// `history` is closed the same way. The unknown-STRING case was already
		// strict; the non-string case was not, and dropping the pseudo-state
		// entirely is worse than guessing its mode.
		{
			"unknown string history",
			`{"initial": "root", "states": {
				"root": {"initial": "leaf"},
				"leaf": {"parent": "root", "history": "medium"}
			}}`,
			"unknown history kind medium",
		},
		{
			"non-string history",
			`{"initial": "root", "states": {
				"root": {"initial": "leaf"},
				"leaf": {"parent": "root", "history": 7}
			}}`,
			"`history` must be the string",
		},
	}
	for _, c := range rejects {
		c := c
		t.Run(c.name, func(t *testing.T) {
			_, err := ChartDefFromJSON([]byte(c.doc))
			if err == nil {
				t.Fatalf("%s was accepted; want a rejection", c.doc)
			}
			// Naming the offending value is the assertion. A rejection for an
			// unrelated reason implements none of the clause.
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %q does not name %q", err.Error(), c.want)
			}
		})
	}

	// --- the inference that must survive ------------------------------------
	//
	// An OMITTED `kind` is the schema's documented inference rule and stays
	// exactly as it was. Without these, rejecting every chart would pass above.

	t.Run("omitted kind still infers structurally", func(t *testing.T) {
		def := mustChart(t, `{
			"initial": "root",
			"states": {
				"root": {"initial": "mid"},
				"mid":  {"parent": "root", "initial": "leaf"},
				"leaf": {"parent": "mid"}
			}
		}`)
		if got := def.kind("root"); got != kindCompound {
			t.Fatalf("root derived %v, want kindCompound", got)
		}
		if got := def.kind("mid"); got != kindCompound {
			t.Fatalf("mid derived %v, want kindCompound", got)
		}
		if got := def.kind("leaf"); got != kindAtomic {
			t.Fatalf("leaf derived %v, want kindAtomic", got)
		}
		chart := NewStateChart(NewContext(), def)
		if leaves := chart.ActiveLeaves(); len(leaves) != 1 || leaves[0] != "leaf" {
			t.Fatalf("active leaves %v, want [leaf]", leaves)
		}
	})

	t.Run("omitted kind infers parallel", func(t *testing.T) {
		def := mustChart(t, `{
			"initial": "root",
			"states": {
				"root": {"parallel": true},
				"a":    {"parent": "root"},
				"b":    {"parent": "root"}
			}
		}`)
		if got := def.kind("root"); got != kindParallel {
			t.Fatalf("root derived %v, want kindParallel", got)
		}
	})

	// Every legal enum value must still parse, or "reject any present `kind`"
	// would pass the rejections above.
	t.Run("every enum value is accepted", func(t *testing.T) {
		for _, k := range []string{"atomic", "compound", "parallel", "history", "final"} {
			doc := `{"initial": "root", "states": {
				"root": {"initial": "leaf"},
				"leaf": {"parent": "root", "kind": "` + k + `"}
			}}`
			if _, err := ChartDefFromJSON([]byte(doc)); err != nil {
				t.Fatalf("kind %q was rejected: %v", k, err)
			}
		}
	})

	t.Run("kind final is still honoured", func(t *testing.T) {
		def := mustChart(t, `{
			"initial": "root",
			"states": {
				"root": {"initial": "leaf"},
				"leaf": {"parent": "root", "kind": "final"}
			}
		}`)
		if got := def.kind("leaf"); got != kindFinal {
			t.Fatalf("leaf derived %v, want kindFinal", got)
		}
	})

	t.Run("a string history still parses", func(t *testing.T) {
		def := mustChart(t, `{
			"initial": "root",
			"states": {
				"root": {"initial": "leaf"},
				"leaf": {"parent": "root", "default": "leaf", "history": "deep"}
			}
		}`)
		if got := def.kind("leaf"); got != kindHistoryDeep {
			t.Fatalf("leaf derived %v, want kindHistoryDeep", got)
		}
	})
}

func mustChart(t *testing.T, doc string) *ChartDef {
	t.Helper()
	def, err := ChartDefFromJSON([]byte(doc))
	if err != nil {
		t.Fatalf("ChartDefFromJSON: %v", err)
	}
	return def
}

// ---------------------------------------------------------------------------
// INTENTIONAL — FFI message-kind discriminant (ffi.go)
// ---------------------------------------------------------------------------

// TestFFIMessageKindLeniencyIsPinned records the wire reason for
// LazilyFfiMessageKindFromCode's unknown-code default, alongside the existing
// discriminant-stability assertion in capability_ffi_test.go.
//
// Wire reason: the code crosses a C ABI as a plain `int`. A C caller built
// against an older header has no `LazilyFfiMessageKind` member for a frame
// kind added later, and the C enum's own zero value IS "unknown", so mapping
// an out-of-range code to Unknown is what keeps an older host loadable against
// a newer library. The consequence is bounded: Unknown is not a routable kind,
// so a caller that switches on it falls into its own unhandled branch rather
// than misrouting the frame as a Snapshot.
func TestFFIMessageKindLeniencyIsPinned(t *testing.T) {
	for _, code := range []int{-1, 6, 99, 1 << 30} {
		if got := LazilyFfiMessageKindFromCode(code); got != LazilyFfiMessageKindUnknown {
			t.Fatalf("FromCode(%d) = %v, want Unknown", code, got)
		}
	}
	// Known codes must still decode, or "always Unknown" would pass the above.
	for code, want := range map[int]LazilyFfiMessageKind{
		1: LazilyFfiMessageKindSnapshot,
		2: LazilyFfiMessageKindDelta,
		3: LazilyFfiMessageKindCrdtSync,
		4: LazilyFfiMessageKindResyncRequest,
		5: LazilyFfiMessageKindOutboxAck,
	} {
		if got := LazilyFfiMessageKindFromCode(code); got != want {
			t.Fatalf("FromCode(%d) = %v, want %v", code, got, want)
		}
	}
	// The paired status decoder is the fail-closed counterpart: it reports the
	// unknown code through `ok` rather than defaulting.
	if _, ok := LazilyFfiStatusFromCode(99); ok {
		t.Fatal("LazilyFfiStatusFromCode(99) reported ok; the status decoder is strict by contract")
	}
}

// ---------------------------------------------------------------------------
// FAIL CLOSED — blob backend discriminator (ipc.go)
// ---------------------------------------------------------------------------

// TestBlobBackendStrictness covers both halves of the `backend` discriminator
// contract (#lzblobbackendstrict).
//
// This site used to be pinned as INTENTIONAL: an unknown token was normalized to
// Shm on the argument that the router then declines it, so nothing misroutes.
// That inverts the resolve_wrong_backend theorem
// (docs/zero-copy-transport.md), which says a descriptor of one kind never
// resolves against a different backend's table BECAUSE receivers route by kind.
// Reading an unknown kind as `shm` IS routing a non-shm descriptor into the shm
// table — a table this build genuinely resolves — leaving the header
// verification to catch it probabilistically with a 64-bit checksum instead of
// the routing rule catching it structurally.
//
// The forward-compat channel is the field's ABSENCE, and it is the only one: a
// new backend enters the protocol by adding an enum value, which is a spec
// change with a fixture, so an unknown token is a corrupt or non-conforming
// producer rather than a newer peer.
func TestBlobBackendStrictness(t *testing.T) {
	// --- the rejection ------------------------------------------------------

	for _, unknown := range []string{"rdma", "cuda-ipc", "Shm", "ARROW", ""} {
		if _, err := ParseBlobBackendKind(unknown); err == nil {
			t.Fatalf("ParseBlobBackendKind(%q) accepted; want a rejection", unknown)
		} else if !strings.Contains(err.Error(), unknown) && unknown != "" {
			t.Fatalf("error %q does not name the offending token %q", err.Error(), unknown)
		}
		if BlobBackendKind(unknown).IsKnown() && unknown != "" {
			t.Fatalf("BlobBackendKind(%q).IsKnown() = true", unknown)
		}
	}

	// A present-but-unknown `backend` fails the DECODE, naming the token. It is
	// not silently rewritten to a legal value.
	wire := []byte(`{"offset":40,"len":17,"generation":2,"epoch":9,` +
		`"checksum":987654321,"backend":"rdma"}`)
	var ref ShmBlobRef
	err := json.Unmarshal(wire, &ref)
	if err == nil {
		t.Fatalf("a descriptor with backend \"rdma\" decoded to %+v; want a rejection", ref)
	}
	if !strings.Contains(err.Error(), "rdma") {
		t.Fatalf("error %q does not name the offending token", err.Error())
	}
	if ref.Backend != "" {
		t.Fatalf("a rejected decode still populated the descriptor: %+v", ref)
	}

	// --- the leniency that must survive -------------------------------------
	//
	// An ABSENT backend is the forward-compat channel and still decodes as Shm.

	var legacy ShmBlobRef
	if err := json.Unmarshal([]byte(
		`{"offset":40,"len":17,"generation":2,"epoch":9,"checksum":987654321}`), &legacy); err != nil {
		t.Fatalf("a descriptor with no `backend` must decode: %v", err)
	}
	if legacy.Backend.Normalized() != BackendShm {
		t.Fatalf("absent backend decoded as %q, want %q", legacy.Backend, BackendShm)
	}
	// ...and re-encodes without the field, so a pre-field descriptor round-trips
	// byte-identically.
	out, err := json.Marshal(legacy)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if bytes.Contains(out, []byte(`"backend"`)) {
		t.Fatalf("a shm descriptor emitted the `backend` field: %s", out)
	}

	// Known kinds must still parse and route, or "reject everything" would pass
	// the rejections above.
	for kind, wantIndex := range map[BlobBackendKind]int{
		BackendShm: 0, BackendArrow: 1, BackendInProcess: 2,
	} {
		parsed, err := ParseBlobBackendKind(string(kind))
		if err != nil {
			t.Fatalf("ParseBlobBackendKind(%q): %v", kind, err)
		}
		if parsed != kind {
			t.Fatalf("ParseBlobBackendKind(%q) = %q", kind, parsed)
		}
		got, ok := kind.routerIndex()
		if !ok || got != wantIndex {
			t.Fatalf("%q.routerIndex() = %d (ok=%v), want %d", kind, got, ok, wantIndex)
		}
	}
	// The absent discriminator routes to the shm slot, which is what makes the
	// pre-field descriptor resolvable at all.
	if got, ok := BlobBackendKind("").routerIndex(); !ok || got != 0 {
		t.Fatalf("absent backend routerIndex() = %d (ok=%v), want 0", got, ok)
	}

	// --- the in-process counterpart -----------------------------------------
	//
	// A descriptor built in Go rather than decoded cannot reach the strict
	// decoder, so the router refuses to route it too. It resolves to NOTHING
	// rather than into slot 0.
	if _, ok := BlobBackendKind("rdma").routerIndex(); ok {
		t.Fatal("an unknown backend claimed a router slot")
	}
	backend := NewInProcessBackend()
	router := NewBlobRouter().Register(backend)
	desc, err := backend.Write([]byte("abcd"))
	if err != nil {
		t.Fatalf("in-process write: %v", err)
	}
	// Positive control first, so the refusal below is known to be about the
	// discriminator rather than about an empty router.
	if _, ok := router.ReadView(desc); !ok {
		t.Fatal("a real descriptor did not resolve; the negative case below would prove nothing")
	}
	// Same descriptor, same registered backend, only the discriminator changed.
	if _, ok := router.ReadView(desc.WithBackend("rdma")); ok {
		t.Fatal("an unknown backend resolved against a populated router")
	}
}
