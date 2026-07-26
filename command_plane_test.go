package lazily

// Conformance replay of the shared lazily-spec command-plane fixtures
// (conformance/message-passing/*.json).
//
// Mirrors the JS replay harness lazily-js/test/message-passing.test.js and the
// Kotlin lazily-kt/src/test/.../CommandConformanceTest.kt:
//   - each fixture is either a top-level `frames`+`expect` or a `scenarios`
//     array of {name, frames, expect};
//   - frames are folded in order into a fresh CommandProjection, dispatching on
//     `schema` ("message-passing" → ApplyMessage; "receipts" → ObserveReceipt);
//   - `expect` is variable-shaped and asserts the folded projection image,
//     terminal-after-frame-index, ignored-frame-indices (StaleGeneration),
//     RPC resolution indices, and terminal-conflict fail-closed.
//
// Fixtures are resolved via a relative-path helper; a test skips (never fails)
// when the spec checkout is absent.

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

// ---------------------------------------------------------------------------
// Fixture resolution
// ---------------------------------------------------------------------------

func loadMessagePassingFixture(t *testing.T, name string) []byte {
	t.Helper()
	rel := filepath.Join("message-passing", name)
	candidates := []string{
		filepath.Join("..", "lazily-spec", "conformance", rel),
		filepath.Join("test", "conformance", rel),
	}
	if _, file, _, ok := runtime.Caller(0); ok {
		dir := filepath.Dir(file)
		candidates = append(candidates,
			filepath.Join(dir, "..", "lazily-spec", "conformance", rel),
		)
	}
	for _, path := range candidates {
		if b, err := specReadFile(path); err == nil {
			return b
		}
	}
	t.Skipf("conformance fixture not found: %s", rel)
	return nil
}

// ---------------------------------------------------------------------------
// Fixture JSON shapes
// ---------------------------------------------------------------------------

type mpFrame struct {
	Schema string          `json:"schema"`
	Wire   json.RawMessage `json:"wire"`
}

type mpExpect struct {
	Projection               json.RawMessage `json:"projection,omitempty"`
	TerminalAfterFrameIndex  *int            `json:"terminal_after_frame_index,omitempty"`
	IgnoredFrameIndices      []int           `json:"ignored_frame_indices,omitempty"`
	Rpc                      *mpRpcExpect    `json:"rpc,omitempty"`
	Conflict                 bool            `json:"conflict,omitempty"`
	ConflictCommandId        string          `json:"conflict_command_id,omitempty"`
	ConflictAfterFrameIndex  *int            `json:"conflict_after_frame_index,omitempty"`
	ProjectionBeforeConflict json.RawMessage `json:"projection_before_conflict,omitempty"`
}

type mpRpcExpect struct {
	CommandId                   string `json:"command_id"`
	ResolvesAfterFrameIndex     int    `json:"resolves_after_frame_index"`
	UnresolvedAfterFrameIndices []int  `json:"unresolved_after_frame_indices"`
	TerminalStatus              string `json:"terminal_status"`
}

type mpScenario struct {
	Name   string    `json:"name"`
	Frames []mpFrame `json:"frames"`
	Expect mpExpect  `json:"expect"`
}

type mpFixture struct {
	ProtocolVersion int          `json:"protocol_version"`
	Kind            string       `json:"kind"`
	Model           string       `json:"model"`
	Description     string       `json:"description"`
	Frames          []mpFrame    `json:"frames"`
	Expect          mpExpect     `json:"expect"`
	Scenarios       []mpScenario `json:"scenarios"`
}

// ---------------------------------------------------------------------------
// Frame folding
// ---------------------------------------------------------------------------

// foldFrame dispatches a frame into the projection and returns the apply status
// of the last folded item.
func foldFrame(projection *CommandProjection, frame mpFrame) CommandApplyStatus {
	switch frame.Schema {
	case "message-passing":
		msg, err := CommandMessageFromWire(frame.Wire)
		if err != nil {
			panic("decode CommandMessage: " + err.Error())
		}
		return projection.ApplyMessage(msg)
	case "receipts":
		// The wire body is {"CausalReceipts": {...}}.
		tag, body, err := splitTagged(frame.Wire, "receipts wire")
		if err != nil {
			panic("decode receipts wire: " + err.Error())
		}
		if tag != "CausalReceipts" {
			panic("unexpected receipts wire tag: " + tag)
		}
		batch, err := CausalReceiptsFromWire(body)
		if err != nil {
			panic("decode CausalReceipts: " + err.Error())
		}
		var last CommandApplyStatus = CommandStatusUnknown{}
		for _, r := range batch.Receipts {
			last = projection.ObserveReceipt(r)
		}
		return last
	}
	panic("unknown frame schema: " + frame.Schema)
}

// ---------------------------------------------------------------------------
// Assertions
// ---------------------------------------------------------------------------

// assertProjectionImage compares the projection's folded image to the expected
// wire image by decoding both to canonical JSON trees (so nullable fields and
// ordering compare structurally).
func assertProjectionImage(t *testing.T, projection *CommandProjection, expected json.RawMessage, label string) {
	t.Helper()
	got := projection.ToImage()
	gotBytes, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("%s: marshal projection image: %v", label, err)
	}
	var gotTree, wantTree any
	if err := json.Unmarshal(gotBytes, &gotTree); err != nil {
		t.Fatalf("%s: unmarshal got image: %v", label, err)
	}
	if err := json.Unmarshal(expected, &wantTree); err != nil {
		t.Fatalf("%s: unmarshal expected image: %v", label, err)
	}
	if !reflect.DeepEqual(gotTree, wantTree) {
		gotNorm, _ := json.MarshalIndent(gotTree, "", "  ")
		wantNorm, _ := json.MarshalIndent(wantTree, "", "  ")
		t.Errorf("%s: projection image mismatch:\n got:  %s\n want: %s", label, gotNorm, wantNorm)
	}
}

func isTerminalConflict(s CommandApplyStatus) bool {
	_, ok := s.(CommandStatusTerminalConflict)
	return ok
}

// containsInt reports whether xs contains v.
func containsInt(xs []int, v int) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}

// assertImageEqual compares two projection images by canonical JSON trees.
func assertImageEqual(t *testing.T, got, want CommandProjectionImage, label string, frameIdx int) {
	t.Helper()
	gotBytes, _ := json.Marshal(got)
	wantBytes, _ := json.Marshal(want)
	var gotTree, wantTree any
	json.Unmarshal(gotBytes, &gotTree)
	json.Unmarshal(wantBytes, &wantTree)
	if !reflect.DeepEqual(gotTree, wantTree) {
		t.Errorf("%s: frame %d was marked ignored but mutated the projection:\n got:  %s\n want: %s",
			label, frameIdx, gotBytes, wantBytes)
	}
}

// runMessagePassingScenario replays one scenario's frames and asserts expect.
func runMessagePassingScenario(t *testing.T, name string, frames []mpFrame, expect mpExpect) {
	t.Helper()
	label := name
	projection := NewCommandProjection()
	var rpcCommandId string
	if expect.Rpc != nil {
		rpcCommandId = expect.Rpc.CommandId
	}

	for i, frame := range frames {
		// Snapshot the projection image before folding, so an "ignored" frame
		// can be asserted as a no-op regardless of its returned status kind.
		var snapshot *CommandProjectionImage
		ignored := containsInt(expect.IgnoredFrameIndices, i)
		if ignored {
			img := projection.ToImage()
			snapshot = &img
		}

		status := foldFrame(projection, frame)

		// Ignored frames must not mutate the projection image (they may return
		// StaleGeneration for a stale-generation frame, or Recorded for a
		// cancel-after-terminal — either way the image is unchanged).
		if ignored {
			if snapshot != nil {
				assertImageEqual(t, projection.ToImage(), *snapshot, label, i)
			}
		}

		// Terminal-after-frame-index: before this index the command is
		// non-terminal; at/after it is terminal.
		if expect.TerminalAfterFrameIndex != nil {
			// Find the command id from the first submit frame.
			cid := firstCommandId(frames)
			if cid != "" {
				_, terminal := projection.TerminalFor(cid)
				if i < *expect.TerminalAfterFrameIndex && terminal {
					t.Errorf("%s: command %q became terminal before frame %d (at frame %d)", label, cid, *expect.TerminalAfterFrameIndex, i)
				}
				if i == *expect.TerminalAfterFrameIndex && !terminal {
					t.Errorf("%s: command %q not terminal at frame %d", label, cid, i)
				}
			}
		}

		// RPC resolution poll.
		if expect.Rpc != nil {
			state := projection // RPC client's projection IS the reducer here
			call := pollCallOf(state, rpcCommandId)
			for _, idx := range expect.Rpc.UnresolvedAfterFrameIndices {
				if i == idx && call.Kind != CallStateKindPending {
					t.Errorf("%s: rpc call resolved before frame %d (at frame %d): %s", label, idx, i, call.Kind)
				}
			}
			if i == expect.Rpc.ResolvesAfterFrameIndex {
				if call.Kind != CallStateKindResolved {
					t.Errorf("%s: rpc call not resolved at frame %d: %s", label, i, call.Kind)
				} else if string(call.Entry.Status) != expect.Rpc.TerminalStatus {
					t.Errorf("%s: rpc terminal status = %q, want %q", label, call.Entry.Status, expect.Rpc.TerminalStatus)
				}
			}
		}

		// Terminal conflict fail-closed: at conflict_after_frame_index, the
		// fold returns TerminalConflict and the projection image equals the
		// pre-conflict snapshot.
		if expect.Conflict && expect.ConflictAfterFrameIndex != nil && i == *expect.ConflictAfterFrameIndex {
			if !isTerminalConflict(status) {
				t.Errorf("%s: frame %d expected TerminalConflict, got %T", label, i, status)
			}
			if !projection.HasConflict(expect.ConflictCommandId) {
				t.Errorf("%s: HasConflict(%q) = false after frame %d", label, expect.ConflictCommandId, i)
			}
			if len(expect.ProjectionBeforeConflict) > 0 {
				assertProjectionImage(t, projection, expect.ProjectionBeforeConflict, label+" [pre-conflict]")
			}
		}
	}

	// Final projection image.
	if len(expect.Projection) > 0 {
		assertProjectionImage(t, projection, expect.Projection, label)
	}

	// Conflict flag (without an explicit after-index) must hold at the end.
	if expect.Conflict && expect.ConflictAfterFrameIndex == nil {
		if !projection.HasConflict(expect.ConflictCommandId) {
			t.Errorf("%s: HasConflict(%q) = false at end", label, expect.ConflictCommandId)
		}
	}
}

// pollCallOf mirrors CommandRpcClient.PollCall against a bare projection (the
// RPC client's projection is the reducer; the transport is a no-op for the
// replay since frames are folded directly).
func pollCallOf(projection *CommandProjection, commandId string) CallState {
	if projection.HasConflict(commandId) {
		return CallState{Kind: CallStateKindConflict}
	}
	if entry, ok := projection.TerminalFor(commandId); ok {
		return CallState{Kind: CallStateKindResolved, Entry: entry}
	}
	return CallState{Kind: CallStateKindPending}
}

// firstCommandId returns the command_id of the first CommandSubmit frame, or "".
func firstCommandId(frames []mpFrame) string {
	for _, f := range frames {
		if f.Schema != "message-passing" {
			continue
		}
		tag, body, err := splitTagged(f.Wire, "frame")
		if err != nil {
			continue
		}
		if tag != "CommandSubmit" {
			continue
		}
		var s CommandSubmit
		if err := json.Unmarshal(body, &s); err != nil {
			continue
		}
		return s.CommandId
	}
	return ""
}

// runMessagePassingFixture loads a fixture, normalizes it to a scenario list,
// and replays each scenario.
func runMessagePassingFixture(t *testing.T, name string) {
	raw := loadMessagePassingFixture(t, name)
	var fixture mpFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatalf("decode fixture %s: %v", name, err)
	}
	scenarios := fixture.Scenarios
	if len(scenarios) == 0 {
		// Bare frames+expect at the top level: wrap in a single scenario.
		scenarios = []mpScenario{{Name: name, Frames: fixture.Frames, Expect: fixture.Expect}}
	}
	for _, sc := range scenarios {
		label := name
		if sc.Name != "" {
			label = name + "[" + sc.Name + "]"
		}
		t.Run(label, func(t *testing.T) {
			runMessagePassingScenario(t, label, sc.Frames, sc.Expect)
		})
	}
}

func TestCommandPlaneConformance(t *testing.T) {
	fixtures := []string{
		"editor_route_submit.json",
		"sync_tmux_layout_submit.json",
		"accepted_then_applied_receipt.json",
		"stale_generation_ignored.json",
		"terminal_conflict_fail_closed.json",
		"cancel_preempts_nonterminal.json",
		"reconnect_command_projection.json",
		"rpc_call_waits_for_terminal.json",
	}
	for _, name := range fixtures {
		name := name
		t.Run(name, func(t *testing.T) {
			runMessagePassingFixture(t, name)
		})
	}
}

// ---------------------------------------------------------------------------
// Unit tests (no fixtures) — mirroring the JS/Kotlin unit suite.
// ---------------------------------------------------------------------------

func TestCommandStatusTerminality(t *testing.T) {
	terminal := []CommandStatus{
		CommandStatusApplied, CommandStatusRejected, CommandStatusCancelled,
		CommandStatusSuperseded, CommandStatusTimedOut,
	}
	nonTerminal := []CommandStatus{CommandStatusSubmitted, CommandStatusAccepted, CommandStatusRunning}
	for _, s := range terminal {
		if !isTerminalCommandStatus(s) {
			t.Errorf("%q should be terminal", s)
		}
	}
	for _, s := range nonTerminal {
		if isTerminalCommandStatus(s) {
			t.Errorf("%q should not be terminal", s)
		}
	}
}

func TestCommandSubmitRoundTrip(t *testing.T) {
	s := CommandSubmit{
		CommandId: "cmd-1", CausationId: "cmd-1", Source: "vscode-plugin",
		Target: "project-controller", Namespace: "agent-doc", Name: "editor_route",
		AuthorityGeneration: 42, IdempotencyKey: "k", DeadlineMs: 1000,
		Policy:      CommandPolicy{Dedupe: DedupePolicySameIdempotencyKey, Supersede: false, CancelOnPreempt: true},
		PayloadType: "agent-doc.editor_route.v1", PayloadHash: "sha256:abc",
		Payload:          IpcValueInline{Bytes: []byte("hi")},
		RequiredFeatures: []string{"causal-receipts"},
	}
	msg := NewCommandMessageSubmit(s)
	encoded, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	decoded, err := CommandMessageFromWire(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Tag != CommandMessageTagSubmit || decoded.Submit == nil {
		t.Fatalf("decoded tag/submit mismatch: %+v", decoded)
	}
	reencoded, _ := json.Marshal(decoded)
	if string(encoded) != string(reencoded) {
		t.Errorf("round-trip unstable:\n first: %s\n second: %s", encoded, reencoded)
	}
}

func TestCommandAcceptedIsNonTerminal(t *testing.T) {
	p := NewCommandProjection()
	p.Submit(CommandSubmit{
		CommandId: "c", CausationId: "c", Source: "s", Target: "t",
		Namespace: "n", Name: "r", AuthorityGeneration: 1, IdempotencyKey: "k",
		DeadlineMs: 0, Policy: CommandPolicy{Dedupe: DedupePolicyNone},
		PayloadType: "p", PayloadHash: "h", Payload: IpcValueInline{Bytes: nil},
	})
	p.Event(CommandEvent{EventId: "e1", CommandId: "c", Kind: CommandEventKindAccepted, Generation: 1})
	if entry, _ := p.Entry("c"); entry.Status != CommandStatusAccepted || entry.Terminal {
		t.Fatalf("accepted should be non-terminal, got %+v", entry)
	}
}

func TestCommandDuplicateSubmitIsIdempotent(t *testing.T) {
	p := NewCommandProjection()
	s := CommandSubmit{
		CommandId: "c", CausationId: "c", Source: "s", Target: "t",
		Namespace: "n", Name: "r", AuthorityGeneration: 42, IdempotencyKey: "k",
		DeadlineMs: 0, Policy: CommandPolicy{Dedupe: DedupePolicySameIdempotencyKey},
		PayloadType: "p", PayloadHash: "h", Payload: IpcValueInline{Bytes: nil},
	}
	p.Submit(s)
	// A second submit at a different generation must NOT bump the generation.
	s2 := s
	s2.AuthorityGeneration = 99
	status := p.Submit(s2)
	if _, ok := status.(CommandStatusDuplicate); !ok {
		t.Fatalf("duplicate submit should return Duplicate, got %T", status)
	}
	if entry, _ := p.Entry("c"); entry.Generation != 42 {
		t.Fatalf("duplicate submit should not bump generation; got %d", entry.Generation)
	}
}

func TestCommandConflictingTerminalsFailClosed(t *testing.T) {
	p := NewCommandProjection()
	p.Submit(CommandSubmit{
		CommandId: "c", CausationId: "c", Source: "s", Target: "t",
		Namespace: "n", Name: "r", AuthorityGeneration: 1, IdempotencyKey: "k",
		DeadlineMs: 0, Policy: CommandPolicy{Dedupe: DedupePolicyNone},
		PayloadType: "p", PayloadHash: "h", Payload: IpcValueInline{Bytes: nil},
	})
	p.ObserveReceipt(AppliedReceipt("r1", "c", "o", 1))
	status := p.ObserveReceipt(RejectedReceipt("r2", "c", "o", 1))
	if _, ok := status.(CommandStatusTerminalConflict); !ok {
		t.Fatalf("conflicting terminal should return TerminalConflict, got %T", status)
	}
	if !p.HasConflict("c") {
		t.Fatalf("HasConflict(c) should be true")
	}
	// The first terminal outcome sticks (fail-closed: existing not overwritten).
	entry, _ := p.Entry("c")
	if entry.Status != CommandStatusApplied || !entry.Terminal {
		t.Fatalf("conflict should not overwrite existing terminal, got %+v", entry)
	}
}

// TestCommandRpcResolvesOnlyOnTerminalReceipt mirrors the JS/Kotlin RPC unit
// test: a call stays Pending through Accepted+Started events and resolves only
// when an applied receipt folds in.
func TestCommandRpcResolvesOnlyOnTerminalReceipt(t *testing.T) {
	var sent []CommandMessage
	client := NewCommandRpcClient(CommandTransportFunc(func(m CommandMessage) {
		sent = append(sent, m)
	}))
	cid := client.Submit(CommandSubmit{
		CommandId: "c", CausationId: "c", Source: "s", Target: "t",
		Namespace: "n", Name: "r", AuthorityGeneration: 1, IdempotencyKey: "k",
		DeadlineMs: 0, Policy: CommandPolicy{Dedupe: DedupePolicySameIdempotencyKey},
		PayloadType: "p", PayloadHash: "h", Payload: IpcValueInline{Bytes: nil},
	})
	if cid != "c" {
		t.Fatalf("command id = %q, want %q", cid, "c")
	}
	if len(sent) != 1 || sent[0].Tag != CommandMessageTagSubmit {
		t.Fatalf("expected one submit sent, got %d messages", len(sent))
	}
	if state := client.PollCall("c"); state.Kind != CallStateKindPending {
		t.Fatalf("before events: state = %s, want pending", state.Kind)
	}
	client.IngestCommand(NewCommandMessageEvents(CommandEvents{Events: []CommandEvent{
		{EventId: "e1", CommandId: "c", Kind: CommandEventKindAccepted, Generation: 1},
		{EventId: "e2", CommandId: "c", Kind: CommandEventKindStarted, Generation: 1},
	}}))
	if state := client.PollCall("c"); state.Kind != CallStateKindPending {
		t.Fatalf("after progress events: state = %s, want pending", state.Kind)
	}
	client.IngestReceipt(AppliedReceipt("r1", "c", "o", 1))
	state := client.PollCall("c")
	if state.Kind != CallStateKindResolved {
		t.Fatalf("after applied receipt: state = %s, want resolved", state.Kind)
	}
	if state.Entry.Status != CommandStatusApplied {
		t.Fatalf("terminal status = %q, want applied", state.Entry.Status)
	}
}
