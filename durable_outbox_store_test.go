package lazily

import (
	"path/filepath"
	"testing"
)

type outboxStoreFixture struct {
	conformanceMeta
	ProtocolVersion int `json:"protocol_version"`
	Scenarios       []struct {
		conformanceDoc
		Name       string  `json:"name"`
		PutEpochs  []Epoch `json:"put_epochs"`
		ScanAfter  Epoch   `json:"scan_after"`
		AckThrough []Epoch `json:"ack_through"`
		// `restart` and `open_handles` were the scenario's setup, hard-coded in
		// the runner: it always reopened from disk and always opened exactly the
		// two handles named "stale" and "current".
		Restart     bool     `json:"restart"`
		OpenHandles []string `json:"open_handles"`
		SaveCursor  []struct {
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
	} `json:"scenarios"`
}

func loadOutboxStoreFixture(t *testing.T) outboxStoreFixture {
	t.Helper()
	raw, err := specReadFile("../lazily-spec/conformance/reliable-sync/outbox_store_protocol.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture outboxStoreFixture
	mustStrictJSON(t, "reliable-sync/outbox_store_protocol.json", raw, &fixture)
	return fixture
}

func outboxStoreScenario(t *testing.T, fixture outboxStoreFixture, name string) (out struct {
	conformanceDoc
	Name       string  `json:"name"`
	PutEpochs  []Epoch `json:"put_epochs"`
	ScanAfter  Epoch   `json:"scan_after"`
	AckThrough []Epoch `json:"ack_through"`
	// `restart` and `open_handles` were the scenario's setup, hard-coded in
	// the runner: it always reopened from disk and always opened exactly the
	// two handles named "stale" and "current".
	Restart     bool     `json:"restart"`
	OpenHandles []string `json:"open_handles"`
	SaveCursor  []struct {
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
}) {
	t.Helper()
	for _, scenario := range fixture.Scenarios {
		if scenario.Name == name {
			return scenario
		}
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
	ordered := outboxStoreScenario(t, fixture, "unordered puts replay in ascending epoch order")
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

	monotone := outboxStoreScenario(t, fixture, "ack cursor is monotone and prune-safe")
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

	restart := outboxStoreScenario(t, fixture, "restart reloads cursor and unacked suffix")
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
	scenario := outboxStoreScenario(t, fixture, "stale handle cannot regress serialized cursor")
	path := filepath.Join(t.TempDir(), "cursor.jsonl")
	staleStore, err := NewFileOutboxStore(path)
	if err != nil {
		t.Fatal(err)
	}
	currentStore, err := NewFileOutboxStore(path)
	if err != nil {
		t.Fatal(err)
	}
	handles := map[string]*DurableStoreOutbox[*FileOutboxStore]{
		"stale":   NewDurableStoreOutbox(staleStore),
		"current": NewDurableStoreOutbox(currentStore),
	}
	for _, save := range scenario.SaveCursor {
		handles[save.Handle].AckThrough(save.Epoch)
	}
	if cursor := handles["stale"].AckedThrough(); cursor != scenario.Expect.LoadedCursor {
		t.Fatalf("stale handle observed cursor %d", cursor)
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
