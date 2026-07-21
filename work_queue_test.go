package lazily

import "testing"

type workQueueReaders struct {
	pending *Computed[int]
	empty   *Computed[bool]
	flight  *Computed[int]
	dead    *Computed[int]
}

func newWorkQueueReaders(q *WorkQueueCell[string]) workQueueReaders {
	h := q.ReaderHandles()
	for _, read := range []func(){
		func() { q.PendingLen() }, func() { q.IsEmpty() },
		func() { q.InFlightLen() }, func() { q.DeadLetterLen() },
	} {
		read()
	}
	return workQueueReaders{h.PendingLen, h.IsEmpty, h.InFlightLen, h.DeadLetterLen}
}

func assertWorkQueueInvalidation(t *testing.T, q *WorkQueueCell[string], r workQueueReaders, pending, empty, flight, dead bool) {
	t.Helper()
	checks := []struct {
		name string
		got  bool
		want bool
	}{
		{"pending_len", cached(r.pending), !pending},
		{"is_empty", cached(r.empty), !empty},
		{"in_flight_len", cached(r.flight), !flight},
		{"dead_letter_len", cached(r.dead), !dead},
	}
	for _, check := range checks {
		if check.got != check.want {
			t.Fatalf("%s cached=%v want %v", check.name, check.got, check.want)
		}
	}
	q.PendingLen()
	q.IsEmpty()
	q.InFlightLen()
	q.DeadLetterLen()
}

func cached[T any](slot *Computed[T]) bool {
	_, ok := slot.Peek()
	return ok
}

func TestWorkQueueCompetingDelivery(t *testing.T) {
	q := NewWorkQueueCell[string](NewContext(), 10, 3)
	r := newWorkQueueReaders(q)
	if q.Push("a") != 0 {
		t.Fatal("first item id")
	}
	assertWorkQueueInvalidation(t, q, r, true, true, false, false)
	if q.Push("b") != 1 {
		t.Fatal("second item id")
	}
	assertWorkQueueInvalidation(t, q, r, true, false, false, false)
	first, ok := q.Claim("alpha", 100)
	if !ok || first.DeliveryID != 0 || first.ItemID != 0 || first.Attempt != 1 || first.Deadline != 110 {
		t.Fatal("first claim")
	}
	assertWorkQueueInvalidation(t, q, r, true, false, true, false)
	second, ok := q.Claim("beta", 100)
	if !ok || second.DeliveryID != 1 || second.ItemID != 1 {
		t.Fatal("second claim")
	}
	assertWorkQueueInvalidation(t, q, r, true, true, true, false)
	if _, ok := q.Claim("gamma", 100); ok {
		t.Fatal("empty claim")
	}
	assertWorkQueueInvalidation(t, q, r, false, false, false, false)
	if q.Ack("alpha", second.DeliveryID) {
		t.Fatal("wrong-owner ack")
	}
	assertWorkQueueInvalidation(t, q, r, false, false, false, false)
	if !q.Ack("beta", second.DeliveryID) {
		t.Fatal("owner ack")
	}
	assertWorkQueueInvalidation(t, q, r, false, false, true, false)
	if !q.Nack("alpha", first.DeliveryID) {
		t.Fatal("nack")
	}
	assertWorkQueueInvalidation(t, q, r, true, true, true, false)
	retry, ok := q.Claim("gamma", 105)
	if !ok || retry.DeliveryID != 2 || retry.ItemID != 0 || retry.Attempt != 2 || retry.Deadline != 115 {
		t.Fatal("retry claim")
	}
	assertWorkQueueInvalidation(t, q, r, true, true, true, false)
	if !q.Ack("gamma", retry.DeliveryID) {
		t.Fatal("retry ack")
	}
	assertWorkQueueInvalidation(t, q, r, false, false, true, false)
}

func TestWorkQueueVisibilityAndDeadLetter(t *testing.T) {
	q := NewWorkQueueCell[string](NewContext(), 10, 2)
	r := newWorkQueueReaders(q)
	q.Push("poison")
	assertWorkQueueInvalidation(t, q, r, true, true, false, false)
	first, _ := q.Claim("worker-1", 0)
	if first.Deadline != 10 {
		t.Fatal("deadline")
	}
	assertWorkQueueInvalidation(t, q, r, true, true, true, false)
	if q.ReapExpired(10) != 0 {
		t.Fatal("deadline must be live")
	}
	assertWorkQueueInvalidation(t, q, r, false, false, false, false)
	if q.ReapExpired(11) != 1 {
		t.Fatal("first expiry")
	}
	assertWorkQueueInvalidation(t, q, r, true, true, true, false)
	second, _ := q.Claim("worker-2", 11)
	if second.Attempt != 2 || second.Deadline != 21 {
		t.Fatal("second claim")
	}
	assertWorkQueueInvalidation(t, q, r, true, true, true, false)
	if q.ReapExpired(21) != 0 || q.ReapExpired(22) != 1 {
		t.Fatal("second expiry")
	}
	assertWorkQueueInvalidation(t, q, r, false, false, true, true)
	dead := q.DeadLetterItems()
	if len(dead) != 1 || dead[0].ItemID != 0 || dead[0].Attempts != 2 || dead[0].Reason != WorkQueueDeadLetterExpired {
		t.Fatal("dead letter")
	}
}
