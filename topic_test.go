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
