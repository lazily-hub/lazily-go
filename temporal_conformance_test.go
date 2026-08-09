package lazily

import (
	"encoding/json"
	"path/filepath"
	"testing"
)

// Cross-language conformance for the temporal source primitives (#lztime) — see
// lazily-spec/conformance/temporal/*.json and the lazily-rs contract in
// tests/temporal_conformance.rs.
//
// These are compute fixtures: load the `initial` state, replay each tick(now)
// op, and assert the fire/expiry edge (`returns`), each projected reader value,
// and — the core of the spec — that the primary reader invalidates exactly on
// the fire/expiry edge. Invalidation is observed by wrapping the reader cell in
// a Slot and checking whether its cached value survives the tick (mirrors the rs
// `ctx.is_set` check).
//
// Fixtures mirror lazily-spec/conformance/temporal/ byte-identically. When that
// sibling tree is reachable on disk it is used; otherwise the subtest is skipped
// so drift in an absent spec tree cannot fail the build.

func temporalSpecDir() string {
	return specPath("temporal")
}

// temporalStep is one replay step shared by every temporal fixture. Absent
// fields stay nil so a per-model assertion only checks what applies.
type temporalStep struct {
	Op struct {
		Type string `json:"type"`
		Now  uint64 `json:"now"`
	} `json:"op"`
	Returns  bool `json:"returns"`
	Expected struct {
		Fired       *bool           `json:"fired"`
		Value       json.RawMessage `json:"value"`
		Count       *uint64         `json:"count"`
		NextFire    *uint64         `json:"next_fire"`
		State       *string         `json:"state"`
		Invalidates map[string]bool `json:"invalidates"`
	} `json:"expected"`
}

type temporalFixture struct {
	conformanceMeta
	Initial json.RawMessage `json:"initial"`
	Steps   []temporalStep  `json:"steps"`
}

func loadTemporalFixture(t *testing.T, name string) (temporalFixture, bool) {
	t.Helper()
	path := filepath.Join(temporalSpecDir(), name)
	data, err := specReadFile(path)
	if err != nil {
		return temporalFixture{}, false
	}
	var fx temporalFixture
	mustStrictJSON(t, name, data, &fx)
	return fx, true
}

// assertNextFire compares a (value, ok) source read against the fixture's
// next_fire, which is a number when present and null (nil pointer) when absent.
func assertNextFire(t *testing.T, step int, got uint64, ok bool, want *uint64) {
	t.Helper()
	if want == nil {
		if ok {
			t.Fatalf("step %d: next_fire: got %d, want absent", step, got)
		}
		return
	}
	if !ok {
		t.Fatalf("step %d: next_fire: got absent, want %d", step, *want)
	}
	if got != *want {
		t.Fatalf("step %d: next_fire: got %d, want %d", step, got, *want)
	}
}

func TestTemporalConformance(t *testing.T) {
	fixtures := []string{
		"timer_single_shot.json",
		"interval_periodic.json",
		"cron_pattern.json",
		"deadline_expiry.json",
	}
	for _, name := range fixtures {
		name := name
		t.Run(name, func(t *testing.T) {
			fx, ok := loadTemporalFixture(t, name)
			if !ok {
				t.Skipf("lazily-spec fixture absent: %s", filepath.Join(temporalSpecDir(), name))
			}
			switch fx.Model {
			case "TimerCell":
				replayTimer(t, fx)
			case "IntervalCell":
				replayInterval(t, fx)
			case "CronCell":
				replayCron(t, fx)
			case "DeadlineCell":
				replayDeadline(t, fx)
			default:
				t.Fatalf("unknown temporal model %q", fx.Model)
			}
		})
	}
}

func replayTimer(t *testing.T, fx temporalFixture) {
	var initial struct {
		FireAt uint64 `json:"fire_at"`
	}
	mustStrictJSON(t, fx.Model+" initial", fx.Initial, &initial)
	ctx := NewContext()
	timer := NewTimerCell(ctx, initial.FireAt)
	fired := timer.FiredCell()
	observed := NewSlot(ctx, func(c *Compute) bool { return Get(c, fired) })
	observed.Get() // prime

	for i, step := range fx.Steps {
		edge := timer.Tick(step.Op.Now)
		if edge != step.Returns {
			t.Fatalf("step %d: fire edge: got %v, want %v", i, edge, step.Returns)
		}

		if step.Expected.Fired != nil && timer.HasFired() != *step.Expected.Fired {
			t.Fatalf("step %d: fired: got %v, want %v", i, timer.HasFired(), *step.Expected.Fired)
		}
		// value models Option<()>: "()" when present, null before the fire.
		_, present := timer.Value()
		wantPresent := string(step.Expected.Value) == `"()"`
		if present != wantPresent {
			t.Fatalf("step %d: value present: got %v, want %v (%s)", i, present, wantPresent, step.Expected.Value)
		}
		nf, nfOK := timer.NextFire()
		assertNextFire(t, i, nf, nfOK, step.Expected.NextFire)

		_, warm := observed.Peek()
		observed.Get() // re-prime
		if invalidated := !warm; invalidated != step.Expected.Invalidates["fired"] {
			t.Fatalf("step %d: invalidates.fired: got %v, want %v", i, invalidated, step.Expected.Invalidates["fired"])
		}
	}
}

func replayInterval(t *testing.T, fx temporalFixture) {
	var initial struct {
		Period uint64 `json:"period"`
	}
	mustStrictJSON(t, fx.Model+" initial", fx.Initial, &initial)
	ctx := NewContext()
	iv := NewIntervalCell(ctx, initial.Period)
	count := iv.CountCell()
	observed := NewSlot(ctx, func(c *Compute) uint64 { return Get(c, count) })
	observed.Get()

	for i, step := range fx.Steps {
		edge := iv.Tick(step.Op.Now)
		if edge != step.Returns {
			t.Fatalf("step %d: fire edge: got %v, want %v", i, edge, step.Returns)
		}
		if step.Expected.Count != nil && iv.Count() != *step.Expected.Count {
			t.Fatalf("step %d: count: got %d, want %d", i, iv.Count(), *step.Expected.Count)
		}
		nf, nfOK := iv.NextFire()
		assertNextFire(t, i, nf, nfOK, step.Expected.NextFire)

		_, warm := observed.Peek()
		observed.Get()
		if invalidated := !warm; invalidated != step.Expected.Invalidates["count"] {
			t.Fatalf("step %d: invalidates.count: got %v, want %v", i, invalidated, step.Expected.Invalidates["count"])
		}
	}
}

func replayCron(t *testing.T, fx temporalFixture) {
	var initial struct {
		Cycle   uint64   `json:"cycle"`
		Offsets []uint64 `json:"offsets"`
	}
	mustStrictJSON(t, fx.Model+" initial", fx.Initial, &initial)
	ctx := NewContext()
	cron := NewCronCell(ctx, initial.Cycle, initial.Offsets)
	count := cron.CountCell()
	observed := NewSlot(ctx, func(c *Compute) uint64 { return Get(c, count) })
	observed.Get()

	for i, step := range fx.Steps {
		edge := cron.Tick(step.Op.Now)
		if edge != step.Returns {
			t.Fatalf("step %d: fire edge: got %v, want %v", i, edge, step.Returns)
		}
		if step.Expected.Count != nil && cron.Count() != *step.Expected.Count {
			t.Fatalf("step %d: count: got %d, want %d", i, cron.Count(), *step.Expected.Count)
		}
		nf, nfOK := cron.NextFire()
		assertNextFire(t, i, nf, nfOK, step.Expected.NextFire)

		_, warm := observed.Peek()
		observed.Get()
		if invalidated := !warm; invalidated != step.Expected.Invalidates["count"] {
			t.Fatalf("step %d: invalidates.count: got %v, want %v", i, invalidated, step.Expected.Invalidates["count"])
		}
	}
}

func replayDeadline(t *testing.T, fx temporalFixture) {
	var initial struct {
		Value    string `json:"value"`
		Deadline uint64 `json:"deadline"`
	}
	mustStrictJSON(t, fx.Model+" initial", fx.Initial, &initial)
	ctx := NewContext()
	d := NewDeadlineCell(ctx, initial.Value, initial.Deadline)
	expired := d.ExpiredCell()
	observed := NewSlot(ctx, func(c *Compute) bool { return Get(c, expired) })
	observed.Get()

	for i, step := range fx.Steps {
		edge := d.Tick(step.Op.Now)
		if edge != step.Returns {
			t.Fatalf("step %d: expiry edge: got %v, want %v", i, edge, step.Returns)
		}
		state := d.State()
		if step.Expected.State != nil {
			wantExpired := *step.Expected.State == "Expired"
			if state.IsExpired() != wantExpired {
				t.Fatalf("step %d: state: got expired=%v, want %s", i, state.IsExpired(), *step.Expected.State)
			}
		}
		// value preserved across the flip.
		if state.Value() != initial.Value {
			t.Fatalf("step %d: value: got %q, want %q", i, state.Value(), initial.Value)
		}
		if step.Expected.Value != nil {
			var wantValue string
			if err := json.Unmarshal(step.Expected.Value, &wantValue); err != nil {
				t.Fatalf("step %d: expected.value: %v", i, err)
			}
			if state.Value() != wantValue {
				t.Fatalf("step %d: value: got %q, want %q", i, state.Value(), wantValue)
			}
		}

		_, warm := observed.Peek()
		observed.Get()
		if invalidated := !warm; invalidated != step.Expected.Invalidates["state"] {
			t.Fatalf("step %d: invalidates.state: got %v, want %v", i, invalidated, step.Expected.Invalidates["state"])
		}
	}
}
