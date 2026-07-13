package lazily

import (
	"reflect"
	"testing"
)

func TestTopicBroadcastCursorIsolation(t *testing.T) {
	topic := NewTopicCell[string](NewContext())
	if got := topic.Subscribe("alice", TopicDurable); got != TopicSubscribed {
		t.Fatalf("subscribe = %q", got)
	}
	topic.Subscribe("bob", TopicDurable)
	topic.Publish("a")
	topic.Publish("b")
	topic.Advance("alice", 1)
	alice, _ := topic.ReadStream("alice")
	bob, _ := topic.ReadStream("bob")
	if !reflect.DeepEqual(alice, []string{"b"}) || !reflect.DeepEqual(bob, []string{"a", "b"}) {
		t.Fatalf("independent streams: alice=%v bob=%v", alice, bob)
	}
}

func TestTopicDurableReplayAndGC(t *testing.T) {
	topic := NewTopicCell[string](NewContext())
	topic.Subscribe("fast", TopicDurable)
	topic.Subscribe("slow", TopicDurable)
	topic.Publish("a")
	topic.Publish("b")
	topic.Advance("fast", 2)
	topic.Advance("slow", 1)
	topic.Disconnect("slow")
	topic.Publish("c")
	if removed := topic.GC(); removed != 1 {
		t.Fatalf("gc removed %d", removed)
	}
	topic.Reconnect("slow")
	replay, _ := topic.ReadStream("slow")
	if !reflect.DeepEqual(replay, []string{"b", "c"}) {
		t.Fatalf("replay = %v", replay)
	}
	restored := NewTopicCellFromSnapshot(NewContext(), topic.Snapshot())
	if !reflect.DeepEqual(restored.Elements(), topic.Elements()) || restored.BaseOffset() != topic.BaseOffset() {
		t.Fatal("snapshot did not round-trip")
	}
}

func TestTopicEphemeralLifecycle(t *testing.T) {
	topic := NewTopicCell[string](NewContext())
	topic.Subscribe("durable", TopicDurable)
	topic.Subscribe("viewer", TopicEphemeral)
	topic.Publish("a")
	topic.Advance("durable", 1)
	topic.Disconnect("viewer")
	if _, ok := topic.Subscription("viewer"); ok {
		t.Fatal("ephemeral subscription survived disconnect")
	}
	if topic.GC() != 1 {
		t.Fatal("ephemeral subscription held GC frontier")
	}
	topic.Subscribe("viewer", TopicEphemeral)
	viewer, _ := topic.Subscription("viewer")
	if viewer.Cursor != topic.TailOffset() {
		t.Fatalf("new ephemeral cursor = %d, tail = %d", viewer.Cursor, topic.TailOffset())
	}
}

func TestTopicTailAndOfflineAdvanceAreNoops(t *testing.T) {
	topic := NewTopicCell[string](NewContext())
	topic.Subscribe("worker", TopicDurable)
	topic.Publish("a")
	if got := topic.Advance("worker", 1); got != 1 {
		t.Fatalf("advance = %d", got)
	}
	if got := topic.Advance("worker", 1); got != 1 {
		t.Fatalf("tail advance = %d", got)
	}

	topic.Disconnect("worker")
	topic.Publish("b")
	if stream, exists := topic.ReadStream("worker"); exists || len(stream) != 0 {
		t.Fatalf("offline read = %v, exists = %v", stream, exists)
	}
	if got := topic.Advance("worker", 1); got != 1 {
		t.Fatalf("offline advance = %d", got)
	}
	worker, _ := topic.Subscription("worker")
	if worker.Cursor != 1 {
		t.Fatalf("offline cursor = %d", worker.Cursor)
	}

	topic.Reconnect("worker")
	stream, exists := topic.ReadStream("worker")
	if !exists || !reflect.DeepEqual(stream, []string{"b"}) {
		t.Fatalf("reconnected read = %v, exists = %v", stream, exists)
	}
	if topic.GC() != 1 || topic.BaseOffset() != 1 {
		t.Fatal("safe GC did not advance to the frozen cursor")
	}
	worker, _ = topic.Subscription("worker")
	if worker.Cursor != 1 {
		t.Fatalf("GC moved absolute cursor to %d", worker.Cursor)
	}
}

func TestTopicSnapshotRejectsDisconnectedEphemeral(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected invalid snapshot to panic")
		}
	}()
	_ = NewTopicCellFromSnapshot(NewContext(), TopicSnapshot[string]{
		Subscriptions: []TopicSubscriptionSnapshot{{
			ID: "viewer", Cursor: 0, Durability: TopicEphemeral, Connected: false,
		}},
	})
}
