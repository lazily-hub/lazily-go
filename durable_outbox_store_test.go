package lazily

import (
	"path/filepath"
	"testing"
)

// outboxStoreScenarioT is named rather than inline so the fixture struct and
// the lookup helper share ONE definition. They used to carry byte-identical
// anonymous copies, where a field added to one and not the other silently
// stopped being decoded at the other site.
type outboxStoreScenarioT struct {
	conformanceDoc
	Id         string  `json:"id"`
	Name       string  `json:"name"`
	PutEpochs  []Epoch `json:"put_epochs"`
	ScanAfter  Epoch   `json:"scan_after"`
	AckThrough []Epoch `json:"ack_through"`
	// `restart` and `open_handles` are the scenario's setup. They were
	// decoded and dropped: the runner always reopened from disk and always
	// opened exactly the two handles named "stale" and "current", so a
	// fixture that stopped asking for a restart, or renamed a handle, would
	// have gone on passing against the runner's own hardcoded setup.
	//
	// ReopensFromDisk is spelled unlike its json key on purpose:
	// TestConformanceStructFieldsAreRead resolves reads by name, and Topic
	// already has a Restart() method, so a field named `Restart` reads as
	// consumed whether or not anything here ever touches it.
	ReopensFromDisk bool     `json:"restart"`
	OpenHandles     []string `json:"open_handles"`
	SaveCursor      []struct {
		Handle string `json:"handle"`
		Epoch  Epoch  `json:"epoch"`
	} `json:"save_cursor"`
	Expect struct {
		Epochs         []Epoch `json:"epochs"`
		Cursor         Epoch   `json:"cursor"`
		Retained       []Epoch `json:"retained"`
		ReplayFromZero []Epoch `json:"replay_from_zero"`
		LoadedCursor   Epoch   `json:"loaded_cursor"`
		Replay         []Epoch `json:"replay"`
	} `json:"expect"`
}

type outboxStoreFixture struct {
	conformanceMeta
	ProtocolVersion int                    `json:"protocol_version"`
	Scenarios       []outboxStoreScenarioT `json:"scenarios"`
}

func loadOutboxStoreFixture(t *testing.T) outboxStoreFixture {
	t.Helper()
	raw, err := specReadFile(specPath("reliable-sync", "outbox_store_protocol.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture outboxStoreFixture
	mustStrictJSON(t, "reliable-sync/outbox_store_protocol.json", raw, &fixture)
	return fixture
}

func outboxStoreScenario(t *testing.T, fixture outboxStoreFixture, name string) (out outboxStoreScenarioT) {
	t.Helper()
	// Selecting is not replaying (#lzscenariobodyskip): booking rides on the
	// payload handoff, not on the scan that found it.
	for _, sv := range typedScenarioViews("reliable-sync/outbox_store_protocol.json", fixture.Scenarios,
		func(s outboxStoreScenarioT) (string, string) { return s.Id, s.Name }) {
		if sv.ID() != name {
			continue
		}
		return sv.Value()
	}
	t.Fatalf("missing scenario %q", name)
	return out
}

func protocolMessage(epoch Epoch) IpcMessage {
	return IpcMessageDelta{Value: Delta{BaseEpoch: epoch - 1, Epoch: epoch, Ops: []DeltaOp{}}}
}

func replayEpochs(entries []OutboxEntry) []Epoch {
	out := make([]Epoch, len(entries))
	for i, entry := range entries {
		out[i] = entry.Epoch
	}
	return out
}

func TestOutboxStoreProtocol(t *testing.T) {
	fixture := loadOutboxStoreFixture(t)
	ordered := outboxStoreScenario(t, fixture, "unordered_puts_replay_in_epoch_order")
	store := NewInMemoryStore()
	for _, epoch := range ordered.PutEpochs {
		store.Put(epoch, []byte{byte(epoch)})
	}
	stored := store.ScanAfter(ordered.ScanAfter)
	got := make([]Epoch, len(stored))
	for i, entry := range stored {
		got[i] = entry.Epoch
	}
	if !epochsEqual(got, ordered.Expect.Epochs) {
		t.Fatalf("ordered scan = %v", got)
	}

	monotone := outboxStoreScenario(t, fixture, "ack_cursor_is_monotone_and_prune_safe")
	outbox := NewDurableStoreOutbox(NewInMemoryStore())
	for _, epoch := range monotone.PutEpochs {
		outbox.Append(epoch, protocolMessage(epoch))
	}
	for _, epoch := range monotone.AckThrough {
		outbox.AckThrough(epoch)
	}
	if outbox.AckedThrough() != monotone.Expect.Cursor ||
		!epochsEqual(outbox.RetainedEpochs(), monotone.Expect.Retained) ||
		!epochsEqual(replayEpochs(outbox.ReplayFrom(0)), monotone.Expect.ReplayFromZero) {
		t.Fatal("monotonic ack/prune/replay contract diverged")
	}

	restart := outboxStoreScenario(t, fixture, "restart_reloads_cursor_and_unacked_suffix")
	path := filepath.Join(t.TempDir(), "outbox.jsonl")
	first, err := NewFileOutbox(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, epoch := range restart.PutEpochs {
		first.Append(epoch, protocolMessage(epoch))
	}
	for _, epoch := range restart.AckThrough {
		first.AckThrough(epoch)
	}
	// The reopen is what the fixture asks for, not what the runner assumes: the
	// reload assertions below are only about a reload if a reload happened.
	if !restart.ReopensFromDisk {
		t.Fatalf("scenario %q does not set restart — without a reopen the loaded-cursor assertions read the live handle and prove nothing", restart.Name)
	}
	reopened, err := NewFileOutbox(path)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.AckedThrough() != restart.Expect.LoadedCursor ||
		!epochsEqual(reopened.RetainedEpochs(), restart.Expect.Retained) ||
		!epochsEqual(replayEpochs(reopened.ReplayFrom(0)), restart.Expect.Replay) {
		t.Fatal("restart did not reload cursor and unacked suffix")
	}
}

func TestFileOutboxCursorRecordsAreSerializedMonotone(t *testing.T) {
	fixture := loadOutboxStoreFixture(t)
	scenario := outboxStoreScenario(t, fixture, "stale_handle_cannot_regress_cursor")
	path := filepath.Join(t.TempDir(), "cursor.jsonl")
	staleStore, err := NewFileOutboxStore(path)
	if err != nil {
		t.Fatal(err)
	}
	currentStore, err := NewFileOutboxStore(path)
	if err != nil {
		t.Fatal(err)
	}
	// The handle set comes from the fixture. It used to be the pair
	// {"stale", "current"} written into the runner, so `open_handles` decoded
	// into a field nothing read and the scenario could rename or add a handle
	// without the replay noticing.
	if len(scenario.OpenHandles) != 2 {
		t.Fatalf("scenario %q opens %d handles; the regression it describes needs two concurrent handles over one file",
			scenario.Name, len(scenario.OpenHandles))
	}
	stores := []*FileOutboxStore{staleStore, currentStore}
	handles := map[string]*DurableStoreOutbox[*FileOutboxStore]{}
	for i, name := range scenario.OpenHandles {
		handles[name] = NewDurableStoreOutbox(stores[i])
	}
	for _, save := range scenario.SaveCursor {
		handle, open := handles[save.Handle]
		if !open {
			t.Fatalf("scenario %q saves through handle %q, which open_handles does not open", scenario.Name, save.Handle)
		}
		handle.AckThrough(save.Epoch)
	}
	// EVERY open handle must observe the surviving cursor, not just the one the
	// runner happened to name: that is what "cannot regress" means when the
	// later save carries the lower epoch.
	for _, name := range scenario.OpenHandles {
		if cursor := handles[name].AckedThrough(); cursor != scenario.Expect.LoadedCursor {
			t.Fatalf("handle %q observed cursor %d, want %d", name, cursor, scenario.Expect.LoadedCursor)
		}
	}
	if !scenario.ReopensFromDisk {
		t.Fatalf("scenario %q does not set restart — the serialized-cursor assertion below needs a fresh handle over the same file", scenario.Name)
	}
	reopened, err := NewFileOutboxStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if cursor := reopened.LoadCursor(); cursor != scenario.Expect.LoadedCursor {
		t.Fatalf("cursor regressed to %d", cursor)
	}
}

func epochsEqual(a, b []Epoch) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
