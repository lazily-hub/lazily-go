package lazily

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

type outboxStoreFixture struct {
	Scenarios []struct {
		Name       string  `json:"name"`
		PutEpochs  []Epoch `json:"put_epochs"`
		ScanAfter  Epoch   `json:"scan_after"`
		AckThrough []Epoch `json:"ack_through"`
		Expect     struct {
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
	raw, err := os.ReadFile("test/conformance/reliable-sync/outbox_store_protocol.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture outboxStoreFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func outboxStoreScenario(t *testing.T, fixture outboxStoreFixture, name string) (out struct {
	Name       string  `json:"name"`
	PutEpochs  []Epoch `json:"put_epochs"`
	ScanAfter  Epoch   `json:"scan_after"`
	AckThrough []Epoch `json:"ack_through"`
	Expect     struct {
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
	path := filepath.Join(t.TempDir(), "cursor.jsonl")
	stale, err := NewFileOutboxStore(path)
	if err != nil {
		t.Fatal(err)
	}
	current, err := NewFileOutboxStore(path)
	if err != nil {
		t.Fatal(err)
	}
	current.SaveCursor(9)
	stale.SaveCursor(3)
	reopened, err := NewFileOutboxStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if cursor := reopened.LoadCursor(); cursor != 9 {
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
