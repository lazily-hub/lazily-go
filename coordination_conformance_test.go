package lazily

// Cross-language conformance for distributed coordination (#lzcoord) — see
// lazily-spec/docs/coordination.md and
// lazily-spec/conformance/coordination/*.json.
//
// Replays each primitive's op sequence, asserting the returned value, the
// projected readers, and reader invalidation (via a primed observer Slot whose
// warmth mirrors the rs `ctx.is_set` probe). Mirrors
// lazily-rs/tests/coordination_conformance.rs.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func coordSpecDir() string {
	return filepath.Join("..", "lazily-spec", "conformance", "coordination")
}

type coordOp struct {
	Type  string  `json:"type"`
	Peer  *uint64 `json:"peer"`
	Now   uint64  `json:"now"`
	Ttl   uint64  `json:"ttl"`
	Fence *uint64 `json:"fence"`
}

type coordExpected struct {
	Holder           *uint64         `json:"holder"`
	Held             *bool           `json:"held"`
	Fence            *uint64         `json:"fence"`
	Role             *string         `json:"role"`
	CurrentLeader    *uint64         `json:"current_leader"`
	IsLocked         *bool           `json:"is_locked"`
	PermitsAvailable *uint64         `json:"permits_available"`
	Votes            *uint64         `json:"votes"`
	IsOpen           *bool           `json:"is_open"`
	Invalidates      map[string]bool `json:"invalidates"`
}

type coordStep struct {
	Op       coordOp         `json:"op"`
	Returns  json.RawMessage `json:"returns"`
	Expected coordExpected   `json:"expected"`
}

type coordFixture struct {
	Config struct {
		Me       *uint64 `json:"me"`
		Capacity *uint64 `json:"capacity"`
		Total    *uint64 `json:"total"`
	} `json:"config"`
	Steps []coordStep `json:"steps"`
}

func loadCoordFixture(t *testing.T, name string) (coordFixture, bool) {
	t.Helper()
	path := filepath.Join(coordSpecDir(), name)
	data, err := os.ReadFile(path)
	if err != nil {
		return coordFixture{}, false
	}
	var fx coordFixture
	if err := json.Unmarshal(data, &fx); err != nil {
		t.Fatalf("%s: unmarshal fixture: %v", name, err)
	}
	return fx, true
}

// returnsU64 parses a `returns` field that is a number or JSON null.
func returnsU64(t *testing.T, raw json.RawMessage) *uint64 {
	t.Helper()
	var v *uint64
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("returns as u64/null: %v", err)
	}
	return v
}

// returnsBool parses a `returns` field that is a bool.
func returnsBool(t *testing.T, raw json.RawMessage) bool {
	t.Helper()
	var v bool
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("returns as bool: %v", err)
	}
	return v
}

// checkOptU64 asserts a (value, ok) reader against an expected *uint64 (nil =
// absent).
func checkOptU64(t *testing.T, i int, label string, val uint64, ok bool, want *uint64) {
	t.Helper()
	if want == nil {
		if ok {
			t.Errorf("step %d %s: got %d, want absent", i, label, val)
		}
		return
	}
	if !ok || val != *want {
		t.Errorf("step %d %s: got (%d,%v), want %d", i, label, val, ok, *want)
	}
}

// checkOptGrant asserts an Opt[uint64] fence grant against an expected *uint64.
func checkOptGrant(t *testing.T, i int, label string, got Opt[uint64], want *uint64) {
	t.Helper()
	if want == nil {
		if got.Present {
			t.Errorf("step %d %s: got %d, want null", i, label, got.Value)
		}
		return
	}
	if !got.Present || got.Value != *want {
		t.Errorf("step %d %s: got %+v, want %d", i, label, got, *want)
	}
}

// invalidation checker: warm=false means the observer was invalidated by the op.
func checkInval(t *testing.T, i int, reader string, warm bool, inval map[string]bool) {
	t.Helper()
	want, present := inval[reader]
	if !present {
		t.Fatalf("step %d: fixture missing invalidates.%s", i, reader)
	}
	if got := !warm; got != want {
		t.Errorf("step %d: invalidates.%s: got %v, want %v", i, reader, got, want)
	}
}

func TestCoordinationConformance(t *testing.T) {
	// Guard: skip the whole suite when the sibling spec tree is absent.
	if _, err := os.Stat(filepath.Join(coordSpecDir(), "lease.json")); err != nil {
		t.Skipf("lazily-spec coordination fixtures absent: %s", coordSpecDir())
	}

	t.Run("lease.json", func(t *testing.T) {
		fx, ok := loadCoordFixture(t, "lease.json")
		if !ok {
			t.Skip("absent")
		}
		ctx := NewContext()
		lease := NewLeaseCell[uint64](ctx)
		hc := lease.HolderCell()
		obs := NewSlot(ctx, func(_ *Context) Opt[uint64] { return hc.Get() })
		obs.Get()

		for i, step := range fx.Steps {
			now := step.Op.Now
			switch step.Op.Type {
			case "acquire":
				got := lease.Acquire(*step.Op.Peer, now, step.Op.Ttl)
				checkOptGrant(t, i, "acquire fence", got, returnsU64(t, step.Returns))
			case "renew":
				got := lease.Renew(*step.Op.Peer, now, step.Op.Ttl)
				if want := returnsBool(t, step.Returns); got != want {
					t.Errorf("step %d renew: got %v, want %v", i, got, want)
				}
			case "tick":
				got := lease.Tick(now)
				if want := returnsBool(t, step.Returns); got != want {
					t.Errorf("step %d tick: got %v, want %v", i, got, want)
				}
			default:
				t.Fatalf("step %d: unknown op %q", i, step.Op.Type)
			}

			hv, hok := lease.Holder(now)
			checkOptU64(t, i, "holder", hv, hok, step.Expected.Holder)
			if got := lease.IsHeld(now); got != *step.Expected.Held {
				t.Errorf("step %d held: got %v, want %v", i, got, *step.Expected.Held)
			}
			if got := lease.Fence(); got != *step.Expected.Fence {
				t.Errorf("step %d fence: got %d, want %d", i, got, *step.Expected.Fence)
			}

			_, warm := obs.Peek()
			obs.Get()
			checkInval(t, i, "holder", warm, step.Expected.Invalidates)
		}
	})

	t.Run("leader.json", func(t *testing.T) {
		fx, ok := loadCoordFixture(t, "leader.json")
		if !ok {
			t.Skip("absent")
		}
		if fx.Config.Me == nil {
			t.Fatal("leader fixture missing config.me")
		}
		ctx := NewContext()
		leader := NewLeaderCell[uint64](ctx, *fx.Config.Me)
		lc := leader.CurrentLeaderCell()
		obs := NewSlot(ctx, func(_ *Context) Opt[uint64] { return lc.Get() })
		obs.Get()

		for i, step := range fx.Steps {
			now := step.Op.Now
			var role LeaderRole
			switch step.Op.Type {
			case "campaign":
				role = leader.Campaign(now, step.Op.Ttl)
			case "contend":
				role = leader.Contend(*step.Op.Peer, now, step.Op.Ttl)
			case "tick":
				role = leader.Tick(now)
			default:
				t.Fatalf("step %d: unknown op %q", i, step.Op.Type)
			}

			if step.Expected.Role == nil {
				t.Fatalf("step %d: missing expected.role", i)
			}
			if got := role.String(); got != *step.Expected.Role {
				t.Errorf("step %d role: got %s, want %s", i, got, *step.Expected.Role)
			}
			cv, cok := leader.CurrentLeader(now)
			checkOptU64(t, i, "current_leader", cv, cok, step.Expected.CurrentLeader)

			_, warm := obs.Peek()
			obs.Get()
			checkInval(t, i, "current_leader", warm, step.Expected.Invalidates)
		}
	})

	t.Run("lock.json", func(t *testing.T) {
		fx, ok := loadCoordFixture(t, "lock.json")
		if !ok {
			t.Skip("absent")
		}
		ctx := NewContext()
		lock := NewLockCell[uint64](ctx)
		ic := lock.IsLockedCell()
		obs := NewSlot(ctx, func(_ *Context) bool { return ic.Get() })
		obs.Get()

		for i, step := range fx.Steps {
			now := step.Op.Now
			switch step.Op.Type {
			case "acquire":
				got := lock.Acquire(*step.Op.Peer, now, step.Op.Ttl)
				checkOptGrant(t, i, "acquire fence", got, returnsU64(t, step.Returns))
			case "validate":
				got := lock.Validate(*step.Op.Fence)
				if want := returnsBool(t, step.Returns); got != want {
					t.Errorf("step %d validate: got %v, want %v", i, got, want)
				}
			case "tick":
				got := lock.Tick(now)
				if want := returnsBool(t, step.Returns); got != want {
					t.Errorf("step %d tick: got %v, want %v", i, got, want)
				}
			default:
				t.Fatalf("step %d: unknown op %q", i, step.Op.Type)
			}

			if got := lock.IsLocked(now); got != *step.Expected.IsLocked {
				t.Errorf("step %d is_locked: got %v, want %v", i, got, *step.Expected.IsLocked)
			}
			if got := lock.Fence(); got != *step.Expected.Fence {
				t.Errorf("step %d fence: got %d, want %d", i, got, *step.Expected.Fence)
			}

			_, warm := obs.Peek()
			obs.Get()
			checkInval(t, i, "is_locked", warm, step.Expected.Invalidates)
		}
	})

	t.Run("semaphore.json", func(t *testing.T) {
		fx, ok := loadCoordFixture(t, "semaphore.json")
		if !ok {
			t.Skip("absent")
		}
		if fx.Config.Capacity == nil {
			t.Fatal("semaphore fixture missing config.capacity")
		}
		ctx := NewContext()
		sem := NewSemaphoreCell(ctx, *fx.Config.Capacity)
		pc := sem.PermitsAvailableCell()
		obs := NewSlot(ctx, func(_ *Context) uint64 { return pc.Get() })
		obs.Get()

		for i, step := range fx.Steps {
			switch step.Op.Type {
			case "acquire":
				got := sem.Acquire()
				if want := returnsBool(t, step.Returns); got != want {
					t.Errorf("step %d acquire: got %v, want %v", i, got, want)
				}
			case "release":
				sem.Release()
			default:
				t.Fatalf("step %d: unknown op %q", i, step.Op.Type)
			}

			if got := sem.PermitsAvailable(); got != *step.Expected.PermitsAvailable {
				t.Errorf("step %d permits_available: got %d, want %d", i, got, *step.Expected.PermitsAvailable)
			}

			_, warm := obs.Peek()
			obs.Get()
			checkInval(t, i, "permits_available", warm, step.Expected.Invalidates)
		}
	})

	t.Run("quorum.json", func(t *testing.T) {
		fx, ok := loadCoordFixture(t, "quorum.json")
		if !ok {
			t.Skip("absent")
		}
		if fx.Config.Total == nil {
			t.Fatal("quorum fixture missing config.total")
		}
		ctx := NewContext()
		q := Quorum[uint64](ctx, *fx.Config.Total)
		oc := q.IsOpenCell()
		obs := NewSlot(ctx, func(_ *Context) bool { return oc.Get() })
		obs.Get()

		for i, step := range fx.Steps {
			if step.Op.Peer == nil {
				t.Fatalf("step %d: quorum op missing peer", i)
			}
			got := q.Arrive(*step.Op.Peer)
			if want := returnsBool(t, step.Returns); got != want {
				t.Errorf("step %d vote: got %v, want %v", i, got, want)
			}
			if got := q.Count(); got != *step.Expected.Votes {
				t.Errorf("step %d votes: got %d, want %d", i, got, *step.Expected.Votes)
			}
			if got := q.IsOpen(); got != *step.Expected.IsOpen {
				t.Errorf("step %d is_open: got %v, want %v", i, got, *step.Expected.IsOpen)
			}

			_, warm := obs.Peek()
			obs.Get()
			checkInval(t, i, "is_open", warm, step.Expected.Invalidates)
		}
	})
}
