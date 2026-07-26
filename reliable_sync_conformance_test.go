package lazily

// Conformance replay of the canonical lazily-spec/conformance/reliable-sync/
// fixtures against the native ResyncCoordinator / InMemoryOutbox / OrSet /
// WireLwwRegister, plus a JSON round-trip of the two control frames
// (ResyncRequest / OutboxAck) and SyncDriver loop-shape unit tests.
//
// Cross-language pin with lazily-rs / lazily-kt / lazily-js; correctness backstop
// lazily-formal ReliableSync.lean. Fixtures resolve via loadConformanceFixture
// (sibling ../lazily-spec/conformance/reliable-sync/ or the local committed copy
// ../lazily-spec/conformance/reliable-sync/). The vendored copy under
// test/conformance/ is no longer read by any test — see the note in
// conformance_manifest_test.go about why reading a vendored copy is invisible to
// a source-grep coverage guard.

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

// rsFixture is the reliable-sync fixture envelope (scenarios kept raw so each
// scenario can decode into its own shape).
type rsFixture struct {
	Kind      string            `json:"kind"`
	Scenarios []json.RawMessage `json:"scenarios"`
}

func loadReliableSyncFixture(t *testing.T, name string) rsFixture {
	t.Helper()
	raw := loadConformanceFixture(t, "reliable-sync", name)
	var fx rsFixture
	if err := json.Unmarshal(raw, &fx); err != nil {
		t.Fatalf("decode fixture %s: %v", name, err)
	}
	return fx
}

// scenario returns the raw scenario object with the given name.
func (fx rsFixture) scenario(t *testing.T, name string) json.RawMessage {
	t.Helper()
	for _, sc := range fx.Scenarios {
		var head struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(sc, &head); err != nil {
			t.Fatalf("decode scenario head: %v", err)
		}
		if head.Name == name {
			return sc
		}
	}
	t.Fatalf("scenario %q not found", name)
	return nil
}

func mustMessage(t *testing.T, raw json.RawMessage) IpcMessage {
	t.Helper()
	m, err := IpcMessageFromWire(raw)
	if err != nil {
		t.Fatalf("decode frame %s: %v", raw, err)
	}
	return m
}

// ---------------------------------------------------------------------------
// control-frame serde round-trip
// ---------------------------------------------------------------------------

func TestReliableSyncResyncRequestRoundTripsJSON(t *testing.T) {
	m := IpcMessageResyncRequest{Value: ResyncRequest{FromEpoch: 2}}
	b, err := m.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if got, want := string(b), `{"ResyncRequest":{"from_epoch":2}}`; got != want {
		t.Fatalf("wire = %s, want %s", got, want)
	}
	decoded, err := DecodeIpcMessageJSON(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(decoded, m) {
		t.Fatalf("round-trip = %#v, want %#v", decoded, m)
	}
}

func TestReliableSyncOutboxAckRoundTripsJSON(t *testing.T) {
	m := IpcMessageOutboxAck{Value: OutboxAck{ThroughEpoch: 41}}
	b, err := m.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if got, want := string(b), `{"OutboxAck":{"through_epoch":41}}`; got != want {
		t.Fatalf("wire = %s, want %s", got, want)
	}
	decoded, err := DecodeIpcMessageJSON(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(decoded, m) {
		t.Fatalf("round-trip = %#v, want %#v", decoded, m)
	}
}

func TestReliableSyncControlFramesClassifyAsKinds(t *testing.T) {
	if got := ipcMessageKind(IpcMessageResyncRequest{Value: ResyncRequest{FromEpoch: 1}}); got != LazilyFfiMessageKindResyncRequest {
		t.Fatalf("ResyncRequest kind = %d, want 4", got)
	}
	if got := ipcMessageKind(IpcMessageOutboxAck{Value: OutboxAck{ThroughEpoch: 1}}); got != LazilyFfiMessageKindOutboxAck {
		t.Fatalf("OutboxAck kind = %d, want 5", got)
	}
}

// ---------------------------------------------------------------------------
// multi_epoch_delta.json
// ---------------------------------------------------------------------------

func TestReliableSyncMultiEpochDelta(t *testing.T) {
	fx := loadReliableSyncFixture(t, "multi_epoch_delta.json")
	if fx.Kind != "ReliableSync" {
		t.Fatalf("kind = %q, want ReliableSync", fx.Kind)
	}

	var span struct {
		ReceiverLastEpoch Epoch `json:"receiver_last_epoch"`
		Delta             struct {
			BaseEpoch Epoch `json:"base_epoch"`
			Epoch     Epoch `json:"epoch"`
		} `json:"delta"`
		Expect struct {
			ReceiverLastEpochAfter Epoch `json:"receiver_last_epoch_after"`
		} `json:"expect"`
	}
	if err := json.Unmarshal(fx.scenario(t, "span_3_applies_equal_to_unit_fold"), &span); err != nil {
		t.Fatalf("decode span scenario: %v", err)
	}
	if !(span.Delta.Epoch > span.Delta.BaseEpoch+1) {
		t.Fatalf("fixture must pin a multi-epoch span")
	}
	delta := Delta{BaseEpoch: span.Delta.BaseEpoch, Epoch: span.Delta.Epoch}
	if got, want := delta.Span(), span.Delta.Epoch-span.Delta.BaseEpoch; got != want {
		t.Fatalf("span = %d, want %d", got, want)
	}
	coord := NewResyncCoordinatorWithEpoch(span.ReceiverLastEpoch)
	if action, _ := coord.IngestDelta(delta); action != ResyncActionApply {
		t.Fatalf("action = %v, want Apply", action)
	}
	if coord.LastEpoch() != span.Expect.ReceiverLastEpochAfter {
		t.Fatalf("last_epoch = %d, want %d", coord.LastEpoch(), span.Expect.ReceiverLastEpochAfter)
	}

	var gap struct {
		ReceiverLastEpoch Epoch `json:"receiver_last_epoch"`
		Delta             struct {
			BaseEpoch Epoch `json:"base_epoch"`
			Epoch     Epoch `json:"epoch"`
		} `json:"delta"`
		Expect struct {
			RequestFrom Epoch `json:"request_from"`
		} `json:"expect"`
	}
	if err := json.Unmarshal(fx.scenario(t, "gap_rule_unchanged_under_span"), &gap); err != nil {
		t.Fatalf("decode gap scenario: %v", err)
	}
	gc := NewResyncCoordinatorWithEpoch(gap.ReceiverLastEpoch)
	action, from := gc.IngestDelta(Delta{BaseEpoch: gap.Delta.BaseEpoch, Epoch: gap.Delta.Epoch})
	if action != ResyncActionRequestSnapshot || from != gap.Expect.RequestFrom {
		t.Fatalf("gap ingest = (%v, %d), want (RequestSnapshot, %d)", action, from, gap.Expect.RequestFrom)
	}
	if gc.LastEpoch() != gap.ReceiverLastEpoch {
		t.Fatalf("gap last_epoch = %d, want %d (unchanged)", gc.LastEpoch(), gap.ReceiverLastEpoch)
	}
}

// ---------------------------------------------------------------------------
// resync_gap_converge.json
// ---------------------------------------------------------------------------

type rsInbound struct {
	Dropped        bool            `json:"dropped"`
	Frame          json.RawMessage `json:"frame"`
	ExpectAction   string          `json:"expect_action"`
	RequestFrom    Epoch           `json:"request_from"`
	LastEpochAfter Epoch           `json:"last_epoch_after"`
}

func TestReliableSyncResyncGapConverge(t *testing.T) {
	fx := loadReliableSyncFixture(t, "resync_gap_converge.json")

	var sc struct {
		StartLastEpoch Epoch       `json:"start_last_epoch"`
		Inbound        []rsInbound `json:"inbound"`
		Expect         struct {
			FinalLastEpoch        Epoch `json:"final_last_epoch"`
			ResyncRequestsEmitted int   `json:"resync_requests_emitted"`
		} `json:"expect"`
	}
	if err := json.Unmarshal(fx.scenario(t, "drop_suffix_then_resync_converges"), &sc); err != nil {
		t.Fatalf("decode scenario: %v", err)
	}
	coord := NewResyncCoordinatorWithEpoch(sc.StartLastEpoch)
	requests := 0
	for _, frame := range sc.Inbound {
		if frame.Dropped {
			continue
		}
		action, from := coord.Ingest(mustMessage(t, frame.Frame))
		switch frame.ExpectAction {
		case "Apply":
			if action != ResyncActionApply {
				t.Fatalf("action = %v, want Apply", action)
			}
		case "RequestSnapshot":
			requests++
			if action != ResyncActionRequestSnapshot || from != frame.RequestFrom {
				t.Fatalf("action = (%v,%d), want (RequestSnapshot,%d)", action, from, frame.RequestFrom)
			}
		case "Ignore":
			if action != ResyncActionIgnore {
				t.Fatalf("action = %v, want Ignore", action)
			}
		default:
			t.Fatalf("unknown expect_action %q", frame.ExpectAction)
		}
		if coord.LastEpoch() != frame.LastEpochAfter {
			t.Fatalf("last_epoch = %d, want %d", coord.LastEpoch(), frame.LastEpochAfter)
		}
	}
	if coord.LastEpoch() != sc.Expect.FinalLastEpoch {
		t.Fatalf("final last_epoch = %d, want %d", coord.LastEpoch(), sc.Expect.FinalLastEpoch)
	}
	if requests != sc.Expect.ResyncRequestsEmitted {
		t.Fatalf("requests = %d, want %d", requests, sc.Expect.ResyncRequestsEmitted)
	}

	var single struct {
		StartLastEpoch Epoch       `json:"start_last_epoch"`
		Inbound        []rsInbound `json:"inbound"`
		Expect         struct {
			ResyncRequestsEmitted int `json:"resync_requests_emitted"`
		} `json:"expect"`
	}
	if err := json.Unmarshal(fx.scenario(t, "single_request_per_gap"), &single); err != nil {
		t.Fatalf("decode single scenario: %v", err)
	}
	c2 := NewResyncCoordinatorWithEpoch(single.StartLastEpoch)
	req2 := 0
	for _, frame := range single.Inbound {
		if action, _ := c2.Ingest(mustMessage(t, frame.Frame)); action == ResyncActionRequestSnapshot {
			req2++
		}
	}
	if req2 != single.Expect.ResyncRequestsEmitted {
		t.Fatalf("single-gap requests = %d, want %d", req2, single.Expect.ResyncRequestsEmitted)
	}
}

// ---------------------------------------------------------------------------
// idempotent_redelivery.json
// ---------------------------------------------------------------------------

func TestReliableSyncIdempotentRedelivery(t *testing.T) {
	fx := loadReliableSyncFixture(t, "idempotent_redelivery.json")
	for _, name := range []string{"replayed_delta_is_ignored", "duplicate_current_head_is_ignored"} {
		var sc struct {
			StartLastEpoch Epoch       `json:"start_last_epoch"`
			Inbound        []rsInbound `json:"inbound"`
			Expect         struct {
				FinalLastEpoch Epoch `json:"final_last_epoch"`
			} `json:"expect"`
		}
		if err := json.Unmarshal(fx.scenario(t, name), &sc); err != nil {
			t.Fatalf("decode %s: %v", name, err)
		}
		coord := NewResyncCoordinatorWithEpoch(sc.StartLastEpoch)
		for _, frame := range sc.Inbound {
			if action, _ := coord.Ingest(mustMessage(t, frame.Frame)); action != ResyncActionIgnore {
				t.Fatalf("%s: action = %v, want Ignore", name, action)
			}
			if coord.LastEpoch() != frame.LastEpochAfter {
				t.Fatalf("%s: last_epoch = %d, want %d", name, coord.LastEpoch(), frame.LastEpochAfter)
			}
		}
		if coord.LastEpoch() != sc.Expect.FinalLastEpoch {
			t.Fatalf("%s: final last_epoch = %d, want %d", name, coord.LastEpoch(), sc.Expect.FinalLastEpoch)
		}
	}
}

// ---------------------------------------------------------------------------
// outbox_replay_after_crash.json — with a reference file-backed DurableOutbox
// ---------------------------------------------------------------------------

// fileOutbox is a reference disk-backed DurableOutbox exercising crash replay:
// each frame is a JSON `[epoch, wire]` line, retention rewrites the file.
type fileOutbox struct {
	path         string
	ackedThrough Epoch
}

func newFileOutbox(t *testing.T, path string) *fileOutbox {
	t.Helper()
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		if err := os.WriteFile(path, nil, 0o644); err != nil {
			t.Fatalf("create outbox file: %v", err)
		}
	}
	return &fileOutbox{path: path}
}

func (o *fileOutbox) readAll(t *testing.T) []OutboxEntry {
	t.Helper()
	data, err := specReadFile(o.path)
	if err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	var out []OutboxEntry
	for _, line := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var pair []json.RawMessage
		if err := json.Unmarshal([]byte(line), &pair); err != nil {
			t.Fatalf("decode outbox line: %v", err)
		}
		var epoch Epoch
		if err := json.Unmarshal(pair[0], &epoch); err != nil {
			t.Fatalf("decode outbox epoch: %v", err)
		}
		msg, err := IpcMessageFromWire(pair[1])
		if err != nil {
			t.Fatalf("decode outbox frame: %v", err)
		}
		out = append(out, OutboxEntry{Epoch: epoch, Msg: msg})
	}
	return out
}

func encodeOutboxLine(t *testing.T, e OutboxEntry) string {
	t.Helper()
	wire, err := e.Msg.EncodeJSON()
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}
	return "[" + strconv.FormatInt(int64(e.Epoch), 10) + "," + string(wire) + "]\n"
}

func (o *fileOutbox) append(t *testing.T, epoch Epoch, msg IpcMessage) {
	t.Helper()
	f, err := os.OpenFile(o.path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatalf("open outbox for append: %v", err)
	}
	defer f.Close()
	if _, err := f.WriteString(encodeOutboxLine(t, OutboxEntry{Epoch: epoch, Msg: msg})); err != nil {
		t.Fatalf("append outbox: %v", err)
	}
}

func (o *fileOutbox) ackThrough(t *testing.T, epoch Epoch) {
	t.Helper()
	if epoch > o.ackedThrough {
		o.ackedThrough = epoch
	}
	var buf strings.Builder
	for _, e := range o.readAll(t) {
		if e.Epoch > o.ackedThrough {
			buf.WriteString(encodeOutboxLine(t, e))
		}
	}
	if err := os.WriteFile(o.path, []byte(buf.String()), 0o644); err != nil {
		t.Fatalf("rewrite outbox: %v", err)
	}
}

func (o *fileOutbox) replayFrom(t *testing.T, cursor Epoch) []OutboxEntry {
	t.Helper()
	var out []OutboxEntry
	for _, e := range o.readAll(t) {
		if e.Epoch > cursor {
			out = append(out, e)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Epoch < out[j].Epoch })
	return out
}

func (o *fileOutbox) retainedEpochs(t *testing.T) []Epoch {
	t.Helper()
	var es []Epoch
	for _, e := range o.readAll(t) {
		es = append(es, e.Epoch)
	}
	sort.Slice(es, func(i, j int) bool { return es[i] < es[j] })
	return es
}

type rsAppended struct {
	Epoch Epoch           `json:"epoch"`
	Frame json.RawMessage `json:"frame"`
}

func TestReliableSyncOutboxReplayAfterCrash(t *testing.T) {
	fx := loadReliableSyncFixture(t, "outbox_replay_after_crash.json")

	var sc struct {
		Appended        []rsAppended `json:"appended"`
		AckThrough      Epoch        `json:"ack_through"`
		ReconnectCursor Epoch        `json:"reconnect_cursor"`
		Expect          struct {
			RetainedAfterAck       []Epoch `json:"retained_after_ack"`
			ReplayedFromCursor     []Epoch `json:"replayed_from_cursor"`
			ReceiverApplies        []Epoch `json:"receiver_applies"`
			ReceiverLastEpochAfter Epoch   `json:"receiver_last_epoch_after"`
		} `json:"expect"`
	}
	if err := json.Unmarshal(fx.scenario(t, "crash_between_append_and_ack_replays_on_reconnect"), &sc); err != nil {
		t.Fatalf("decode scenario: %v", err)
	}

	path := filepath.Join(t.TempDir(), "outbox.jsonl")
	mem := NewInMemoryOutbox()
	file := newFileOutbox(t, path)
	for _, a := range sc.Appended {
		msg := mustMessage(t, a.Frame)
		mem.Append(a.Epoch, msg)
		file.append(t, a.Epoch, msg)
	}
	mem.AckThrough(sc.AckThrough)
	file.ackThrough(t, sc.AckThrough)

	if !reflect.DeepEqual(mem.RetainedEpochs(), sc.Expect.RetainedAfterAck) {
		t.Fatalf("mem retained = %v, want %v", mem.RetainedEpochs(), sc.Expect.RetainedAfterAck)
	}
	if !reflect.DeepEqual(file.retainedEpochs(t), sc.Expect.RetainedAfterAck) {
		t.Fatalf("file retained = %v, want %v", file.retainedEpochs(t), sc.Expect.RetainedAfterAck)
	}

	// "crash": reopen the durable file outbox from disk.
	file = newFileOutbox(t, path)
	replay := file.replayFrom(t, sc.ReconnectCursor)
	replayEpochs := make([]Epoch, 0, len(replay))
	for _, e := range replay {
		replayEpochs = append(replayEpochs, e.Epoch)
	}
	if !reflect.DeepEqual(replayEpochs, sc.Expect.ReplayedFromCursor) {
		t.Fatalf("replay = %v, want %v", replayEpochs, sc.Expect.ReplayedFromCursor)
	}

	// Feed the replay to a receiver at the reconnect cursor: applies each once.
	coord := NewResyncCoordinatorWithEpoch(sc.ReconnectCursor)
	var applied []Epoch
	for _, e := range replay {
		if action, _ := coord.Ingest(e.Msg); action == ResyncActionApply {
			applied = append(applied, coord.LastEpoch())
		}
	}
	if !reflect.DeepEqual(applied, sc.Expect.ReceiverApplies) {
		t.Fatalf("applied = %v, want %v", applied, sc.Expect.ReceiverApplies)
	}
	if coord.LastEpoch() != sc.Expect.ReceiverLastEpochAfter {
		t.Fatalf("receiver last_epoch = %d, want %d", coord.LastEpoch(), sc.Expect.ReceiverLastEpochAfter)
	}

	// send_failure_retains_frame_for_next_tick
	var sc2 struct {
		Appended []rsAppended `json:"appended"`
		Expect   struct {
			Retained []Epoch `json:"retained"`
		} `json:"expect"`
	}
	if err := json.Unmarshal(fx.scenario(t, "send_failure_retains_frame_for_next_tick"), &sc2); err != nil {
		t.Fatalf("decode send-failure scenario: %v", err)
	}
	mem2 := NewInMemoryOutbox()
	for _, a := range sc2.Appended {
		mem2.Append(a.Epoch, mustMessage(t, a.Frame))
	}
	if !reflect.DeepEqual(mem2.RetainedEpochs(), sc2.Expect.Retained) {
		t.Fatalf("retained = %v, want %v", mem2.RetainedEpochs(), sc2.Expect.Retained)
	}
	resent := make([]Epoch, 0, len(sc2.Expect.Retained))
	for _, e := range mem2.ReplayFrom(sc2.Expect.Retained[0] - 1) {
		resent = append(resent, e.Epoch)
	}
	if !reflect.DeepEqual(resent, sc2.Expect.Retained) {
		t.Fatalf("resent = %v, want %v", resent, sc2.Expect.Retained)
	}
}

// ---------------------------------------------------------------------------
// liveness_orset_lww.json
// ---------------------------------------------------------------------------

type stampJSON struct {
	WallTime int64  `json:"wall_time"`
	Logical  int64  `json:"logical"`
	Peer     PeerId `json:"peer"`
}

func (s stampJSON) wire() WireStamp {
	return WireStamp{WallTime: s.WallTime, Logical: s.Logical, Peer: s.Peer}
}

// parsePid strips a "pid" prefix from the trailing key segment.
func parsePid(t *testing.T, s string) PeerId {
	t.Helper()
	seg := s
	if i := strings.LastIndex(s, "/"); i >= 0 {
		seg = s[i+1:]
	}
	seg = strings.TrimPrefix(seg, "pid")
	n, err := strconv.ParseInt(seg, 10, 64)
	if err != nil {
		t.Fatalf("parse pid from %q: %v", s, err)
	}
	return PeerId(n)
}

func TestReliableSyncLivenessOrSetLww(t *testing.T) {
	fx := loadReliableSyncFixture(t, "liveness_orset_lww.json")

	// open_set_add_wins_over_stale_remove
	var add struct {
		Ops []struct {
			Op           string   `json:"op"`
			Tag          string   `json:"tag"`
			ObservedTags []string `json:"observed_tags"`
		} `json:"ops"`
		Expect struct {
			Present bool `json:"present"`
		} `json:"expect"`
	}
	if err := json.Unmarshal(fx.scenario(t, "open_set_add_wins_over_stale_remove"), &add); err != nil {
		t.Fatalf("decode orset scenario: %v", err)
	}
	set := NewOrSet()
	for _, op := range add.Ops {
		switch op.Op {
		case "add":
			set.Add(op.Tag)
		case "remove":
			set.RemoveObserved(op.ObservedTags)
		}
	}
	if set.Present() != add.Expect.Present {
		t.Fatalf("present = %v, want %v", set.Present(), add.Expect.Present)
	}

	// lww_alive_highest_stamp_wins
	var lww struct {
		Ops []struct {
			Value bool      `json:"value"`
			Stamp stampJSON `json:"stamp"`
		} `json:"ops"`
		Expect struct {
			Value bool `json:"value"`
		} `json:"expect"`
	}
	if err := json.Unmarshal(fx.scenario(t, "lww_alive_highest_stamp_wins"), &lww); err != nil {
		t.Fatalf("decode lww scenario: %v", err)
	}
	reg := NewWireLwwRegister(lww.Ops[0].Stamp.wire(), lww.Ops[0].Value)
	for _, op := range lww.Ops[1:] {
		reg.Set(op.Stamp.wire(), op.Value)
	}
	if reg.Value() != lww.Expect.Value {
		t.Fatalf("lww value = %v, want %v", reg.Value(), lww.Expect.Value)
	}

	// whole_editor_death_cascades
	var death struct {
		OpenSet []struct {
			Key     string `json:"key"`
			Present bool   `json:"present"`
		} `json:"open_set"`
		AliveBefore map[string]bool `json:"alive_before"`
		Op          struct {
			Key   string    `json:"key"`
			Value bool      `json:"value"`
			Stamp stampJSON `json:"stamp"`
		} `json:"op"`
		Expect struct {
			LiveDocsAfter []string `json:"live_docs_after"`
		} `json:"expect"`
	}
	if err := json.Unmarshal(fx.scenario(t, "whole_editor_death_cascades"), &death); err != nil {
		t.Fatalf("decode death scenario: %v", err)
	}
	type openEntry struct {
		doc string
		pid PeerId
	}
	var open []openEntry
	for _, e := range death.OpenSet {
		if !e.Present {
			continue
		}
		parts := strings.SplitN(e.Key, "/", 2)
		open = append(open, openEntry{doc: parts[0], pid: parsePid(t, e.Key)})
	}
	alive := map[PeerId]*WireLwwRegister[bool]{}
	for pidStr, v := range death.AliveBefore {
		pid, err := strconv.ParseInt(pidStr, 10, 64)
		if err != nil {
			t.Fatalf("parse alive pid %q: %v", pidStr, err)
		}
		alive[PeerId(pid)] = NewWireLwwRegister(WireStamp{WallTime: 1, Logical: 0, Peer: 1}, v)
	}
	deadPid := parsePid(t, death.Op.Key)
	alive[deadPid].Set(death.Op.Stamp.wire(), death.Op.Value)

	liveSet := map[string]struct{}{}
	for _, e := range open {
		if reg, ok := alive[e.pid]; ok && reg.Value() {
			liveSet[e.doc] = struct{}{}
		}
	}
	live := make([]string, 0, len(liveSet))
	for doc := range liveSet {
		live = append(live, doc)
	}
	sort.Strings(live)
	want := append([]string(nil), death.Expect.LiveDocsAfter...)
	sort.Strings(want)
	if !reflect.DeepEqual(live, want) {
		t.Fatalf("live docs = %v, want %v", live, want)
	}
}

// ---------------------------------------------------------------------------
// SyncDriver (#sync-driver) loop-shape unit tests over a scripted seam
// ---------------------------------------------------------------------------
//
// A SimWorld-style deterministic transport pair mirroring lazily-rs / lazily-js:
// the sink records what the driver sends (and can be toggled "down" to model a
// disconnect); the source replays a scripted inbound stream (and can inject one
// read error). No goroutines, no real socket — every tick is a pure step. The
// seam carries no wire form of its own, so it has no conformance fixture; these
// unit tests pin the loop shape the spec § SyncDriver requires.

var errSinkDown = errors.New("scripted sink down")
var errSourceRead = errors.New("scripted source read failure")

type testWire struct {
	sent      []IpcMessage
	inbound   []IpcMessage
	up        bool
	sourceErr bool
}

type testSink struct{ w *testWire }

func (s testSink) Send(m IpcMessage) error {
	if !s.w.up {
		return errSinkDown
	}
	s.w.sent = append(s.w.sent, m)
	return nil
}

type testSource struct{ w *testWire }

func (s testSource) Recv() (IpcMessage, bool, error) {
	if s.w.sourceErr {
		s.w.sourceErr = false
		return nil, false, errSourceRead
	}
	if len(s.w.inbound) == 0 {
		return nil, false, nil
	}
	m := s.w.inbound[0]
	s.w.inbound = s.w.inbound[1:]
	return m, true, nil
}

type zeroClock struct{}

func (zeroClock) NowMillis() int64 { return 0 }

// snapAhead answers a ResyncRequest{from} with a snapshot at from + 5.
type snapAhead struct{}

func (snapAhead) Snapshot(from Epoch) IpcMessage {
	return IpcMessageSnapshot{Value: Snapshot{Epoch: from + 5}}
}

func driverAt(w *testWire, lastEpoch Epoch) *SyncDriver {
	return NewSyncDriverWithEpoch(testSink{w}, testSource{w}, NewInMemoryOutbox(), zeroClock{}, snapAhead{}, lastEpoch)
}

func dframe(base, epoch Epoch) IpcMessage {
	return IpcMessageDelta{Value: Delta{BaseEpoch: base, Epoch: epoch}}
}

func mustTick(t *testing.T, d *SyncDriver) Progress {
	t.Helper()
	p, err := d.Tick()
	if err != nil {
		t.Fatalf("tick: %v", err)
	}
	return p
}

func TestSyncDriverDrainsAppendBeforeSendAndRetainsUntilAcked(t *testing.T) {
	w := &testWire{up: true}
	d := driverAt(w, 0)
	d.Enqueue(1, dframe(0, 1))
	d.Enqueue(2, dframe(1, 2))
	p := mustTick(t, d)
	if p.Sent != 2 {
		t.Fatalf("sent = %d, want 2", p.Sent)
	}
	if len(w.sent) != 2 {
		t.Fatalf("wire sent = %d, want 2", len(w.sent))
	}
	if p.Retained != 2 {
		t.Fatalf("retained = %d, want 2", p.Retained)
	}
	if d.IsStalled() {
		t.Fatalf("driver should not be stalled")
	}

	w.inbound = append(w.inbound, IpcMessageOutboxAck{Value: OutboxAck{ThroughEpoch: 2}})
	p = mustTick(t, d)
	if p.PeerAckedThrough != 2 {
		t.Fatalf("peer_acked_through = %d, want 2", p.PeerAckedThrough)
	}
	if p.Retained != 0 {
		t.Fatalf("retained = %d, want 0 (acked pruned)", p.Retained)
	}
}

func TestSyncDriverRetainsOnSendFailureAndReplaysOnReconnect(t *testing.T) {
	w := &testWire{up: false} // sink down before the first send
	d := driverAt(w, 0)
	d.Enqueue(1, dframe(0, 1))
	p := mustTick(t, d)
	if p.Sent != 0 {
		t.Fatalf("sent = %d, want 0", p.Sent)
	}
	if !d.IsStalled() {
		t.Fatalf("a failed send should stall the driver")
	}
	if p.Retained != 1 {
		t.Fatalf("retained = %d, want 1", p.Retained)
	}
	if len(w.sent) != 0 {
		t.Fatalf("nothing should have reached the sink")
	}
	if got := d.StalledFor(250); got != 250 {
		t.Fatalf("stalled_for = %d, want 250", got)
	}

	w.up = true
	d.OnReconnect()
	p = mustTick(t, d)
	if d.IsStalled() {
		t.Fatalf("reconnect should clear the stall")
	}
	if p.Sent != 1 {
		t.Fatalf("sent = %d, want 1 (retained frame replayed)", p.Sent)
	}
	found := false
	for _, m := range w.sent {
		if dm, ok := m.(IpcMessageDelta); ok && dm.Value.Epoch == 1 {
			found = true
		}
	}
	if !found {
		t.Fatalf("the replayed delta should reach the sink")
	}
}

func TestSyncDriverAppliesDeltaAndAdvertisesReceiverCursor(t *testing.T) {
	w := &testWire{up: true}
	d := driverAt(w, 0)
	w.inbound = append(w.inbound, dframe(0, 1))
	p := mustTick(t, d)
	if len(p.Applied) != 1 {
		t.Fatalf("applied = %d, want 1", len(p.Applied))
	}
	if d.LastEpoch() != 1 {
		t.Fatalf("last_epoch = %d, want 1", d.LastEpoch())
	}
	found := false
	for _, m := range w.sent {
		if am, ok := m.(IpcMessageOutboxAck); ok && am.Value.ThroughEpoch == 1 {
			found = true
		}
	}
	if !found {
		t.Fatalf("an OutboxAck advertising the new cursor should be sent")
	}
}

func TestSyncDriverRedeliveryIsIdempotentNoOp(t *testing.T) {
	w := &testWire{up: true}
	d := driverAt(w, 0)
	w.inbound = append(w.inbound, dframe(0, 1))
	if p := mustTick(t, d); len(p.Applied) != 1 {
		t.Fatalf("applied = %d, want 1", len(p.Applied))
	}
	w.inbound = append(w.inbound, dframe(0, 1))
	p := mustTick(t, d)
	if len(p.Applied) != 0 {
		t.Fatalf("re-delivery applied = %d, want 0", len(p.Applied))
	}
	if d.LastEpoch() != 1 {
		t.Fatalf("last_epoch = %d, want 1 (no double advance)", d.LastEpoch())
	}
}

func TestSyncDriverRequestsSnapshotOnInboundGap(t *testing.T) {
	w := &testWire{up: true}
	d := driverAt(w, 2)
	w.inbound = append(w.inbound, dframe(3, 4)) // base 3 > last 2 → gap
	p := mustTick(t, d)
	if !p.ResyncRequested {
		t.Fatalf("resync_requested should be true")
	}
	if len(p.Applied) != 0 {
		t.Fatalf("gapped delta should not be applied")
	}
	found := false
	for _, m := range w.sent {
		if rm, ok := m.(IpcMessageResyncRequest); ok && rm.Value.FromEpoch == 2 {
			found = true
		}
	}
	if !found {
		t.Fatalf("a ResyncRequest at the current cursor should be emitted")
	}
}

func TestSyncDriverAnswersResyncRequestWithProviderSnapshot(t *testing.T) {
	w := &testWire{up: true}
	d := driverAt(w, 0)
	w.inbound = append(w.inbound, IpcMessageResyncRequest{Value: ResyncRequest{FromEpoch: 2}})
	p := mustTick(t, d)
	if p.SnapshotsServed != 1 {
		t.Fatalf("snapshots_served = %d, want 1", p.SnapshotsServed)
	}
	found := false
	for _, m := range w.sent {
		if sm, ok := m.(IpcMessageSnapshot); ok && sm.Value.Epoch == 7 {
			found = true
		}
	}
	if !found {
		t.Fatalf("a covering snapshot (from + 5) should be sent")
	}
}

func TestSyncDriverSurfacesSourceReadError(t *testing.T) {
	w := &testWire{up: true, sourceErr: true}
	d := driverAt(w, 0)
	_, err := d.Tick()
	var de *DriverError
	if !errors.As(err, &de) {
		t.Fatalf("err = %v, want *DriverError", err)
	}
	if !errors.Is(de.Source, errSourceRead) {
		t.Fatalf("DriverError.Source = %v, want scripted source read failure", de.Source)
	}
}

func TestSyncDriverGapThenSnapshotConverges(t *testing.T) {
	w := &testWire{up: true}
	d := driverAt(w, 2)
	w.inbound = append(w.inbound, dframe(4, 5)) // gap
	mustTick(t, d)
	if d.LastEpoch() != 2 {
		t.Fatalf("last_epoch = %d, want 2 (stuck at pre-gap cursor)", d.LastEpoch())
	}
	w.inbound = append(w.inbound, IpcMessageSnapshot{Value: Snapshot{Epoch: 5}})
	p := mustTick(t, d)
	if len(p.Applied) != 1 {
		t.Fatalf("applied = %d, want 1", len(p.Applied))
	}
	if d.LastEpoch() != 5 {
		t.Fatalf("last_epoch = %d, want 5 (snapshot restored convergence)", d.LastEpoch())
	}
}
