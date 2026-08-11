package lazily

import (
	"encoding/json"
	"reflect"
	"sync"
	"testing"
)

func TestDependencyAvailabilityIsExactKeyReactive(t *testing.T) {
	raw, err := specReadFile(specPath("collections", "dependency_reactive_availability.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		conformanceMeta
		Key   string `json:"key"`
		Steps []struct {
			Op struct {
				Type  string `json:"type"`
				Key   string `json:"key"`
				Value int    `json:"value"`
			} `json:"op"`
			Expected struct {
				State        json.RawMessage `json:"state"`
				Recomputes   int             `json:"recomputes"`
				PresentCount int             `json:"present_count"`
				Identity     string          `json:"identity"`
			} `json:"expected"`
		} `json:"steps"`
	}
	// A plain json.Unmarshal here DROPPED any key the struct does not model, in
	// silence — including a key added to a step's `expected` block, which is why
	// the unbound-block guard reported all seven of them as bound by nothing
	// (#lzunboundblockguard). The strict decode makes an unmodelled key a hard
	// error naming the fixture and the key.
	mustStrictJSON(t, "collections/dependency_reactive_availability.json", raw, &fixture)
	if len(fixture.Steps) == 0 {
		t.Fatal("dependency fixture has no steps")
	}

	ctx := NewContext()
	dependencies := NewDependencyMap[string, int](ctx)
	runs := 0
	wanted := NewSlot(ctx, func(c *Compute) DependencyAvailability[int] {
		runs++
		return dependencies.ObserveDependency(c, fixture.Key)
	})
	var identity *Source[DependencyAvailability[int]]
	for index, step := range fixture.Steps {
		switch step.Op.Type {
		case "observe_dependency":
			_ = Get(ctx, wanted)
		case "publish":
			dependencies.Publish(step.Op.Key, step.Op.Value)
		case "unpublish":
			dependencies.Unpublish(step.Op.Key)
		default:
			t.Fatalf("step %d: unsupported dependency operation %q", index, step.Op.Type)
		}

		state := Get(ctx, wanted)
		var actualState any = "Unavailable"
		if state.Available {
			actualState = map[string]any{"Available": float64(state.Value)}
		}
		var expectedState any
		if err := json.Unmarshal(step.Expected.State, &expectedState); err != nil {
			t.Fatalf("step %d state: %v", index, err)
		}
		if !reflect.DeepEqual(actualState, expectedState) {
			t.Fatalf("step %d state = %#v, want %#v", index, actualState, expectedState)
		}
		if runs != step.Expected.Recomputes {
			t.Fatalf("step %d runs = %d, want %d", index, runs, step.Expected.Recomputes)
		}
		if dependencies.PresentCount() != step.Expected.PresentCount {
			t.Fatalf(
				"step %d present count = %d, want %d",
				index,
				dependencies.PresentCount(),
				step.Expected.PresentCount,
			)
		}
		handle, ok := dependencies.Handle(fixture.Key)
		if !ok {
			t.Fatalf("step %d exact-key source is absent", index)
		}
		if identity == nil {
			identity = handle
		} else if handle != identity {
			t.Fatalf("step %d exact-key source identity changed", index)
		}
		if step.Expected.Identity != "wanted-1" {
			t.Fatalf("step %d unexpected fixture identity %q", index, step.Expected.Identity)
		}
	}
}

func TestThreadSafeDependencyFirstObserversShareOneHandle(t *testing.T) {
	ts := NewThreadSafeContext()
	dependencies := NewThreadSafeDependencyMap[string, int](ts)
	const count = 16
	start := make(chan struct{})
	handles := make(chan *Source[DependencyAvailability[int]], count)
	var group sync.WaitGroup
	group.Add(count)
	for range count {
		go func() {
			defer group.Done()
			<-start
			dependencies.ObserveDependency(ts.Context(), "wanted")
			handle, _ := dependencies.Handle("wanted")
			handles <- handle
		}()
	}
	close(start)
	group.Wait()
	close(handles)

	var first *Source[DependencyAvailability[int]]
	for handle := range handles {
		if first == nil {
			first = handle
		} else if handle != first {
			t.Fatal("first observers received different handles")
		}
	}
	if dependencies.PresentCount() != 1 {
		t.Fatalf("present count = %d, want 1", dependencies.PresentCount())
	}
}

func TestAsyncDependencyAvailabilityPreservesIdentity(t *testing.T) {
	ctx := NewAsyncContext()
	dependencies := NewAsyncDependencyMap[string, int](ctx)
	if got := dependencies.ObserveDependency(nil, "wanted"); got != UnavailableDependency[int]() {
		t.Fatalf("initial availability = %#v", got)
	}
	id, _ := dependencies.EntryID("wanted")
	dependencies.Publish("wanted", 9)
	if got := dependencies.ObserveDependency(nil, "wanted"); got != AvailableDependency(9) {
		t.Fatalf("published availability = %#v", got)
	}
	after, _ := dependencies.EntryID("wanted")
	if after != id {
		t.Fatalf("entry identity changed: %d -> %d", id, after)
	}
}
