package lazily

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// Cross-language conformance for fault-tolerance primitives (#lzresilience) —
// mirrors lazily-rs/tests/resilience_conformance.rs, replaying
// lazily-spec/conformance/resilience/*.json byte-identically. When the sibling
// spec tree is absent the subtest is skipped.

func resilienceSpecDir() string {
	return filepath.Join("..", "lazily-spec", "conformance", "resilience")
}

type resilienceOp struct {
	Type    string `json:"type"`
	Success bool   `json:"success"`
	Now     uint64 `json:"now"`
	Timeout uint64 `json:"timeout"`
}

type resilienceStep struct {
	Op       resilienceOp    `json:"op"`
	Returns  json.RawMessage `json:"returns"`
	Expected struct {
		State       string          `json:"state"`
		Delay       uint64          `json:"delay"`
		InUse       uint64          `json:"in_use"`
		IsTimedOut  bool            `json:"is_timed_out"`
		Invalidates map[string]bool `json:"invalidates"`
	} `json:"expected"`
}

type resilienceFixture struct {
	Config json.RawMessage  `json:"config"`
	Steps  []resilienceStep `json:"steps"`
}

func loadResilienceFixture(t *testing.T, name string) (resilienceFixture, bool) {
	t.Helper()
	path := filepath.Join(resilienceSpecDir(), name)
	data, err := os.ReadFile(path)
	if err != nil {
		return resilienceFixture{}, false
	}
	var fx resilienceFixture
	if err := json.Unmarshal(data, &fx); err != nil {
		t.Fatalf("%s: unmarshal fixture: %v", name, err)
	}
	return fx, true
}

func resReturnsBool(t *testing.T, raw json.RawMessage) bool {
	t.Helper()
	var b bool
	if err := json.Unmarshal(raw, &b); err != nil {
		t.Fatalf("returns is not a bool: %s", raw)
	}
	return b
}

func resReturnsU64(t *testing.T, raw json.RawMessage) uint64 {
	t.Helper()
	var n uint64
	if err := json.Unmarshal(raw, &n); err != nil {
		t.Fatalf("returns is not a number: %s", raw)
	}
	return n
}

func TestResilienceConformance(t *testing.T) {
	t.Run("circuit_breaker.json", func(t *testing.T) {
		fx, ok := loadResilienceFixture(t, "circuit_breaker.json")
		if !ok {
			t.Skipf("lazily-spec fixture absent: %s", filepath.Join(resilienceSpecDir(), "circuit_breaker.json"))
		}
		var cfg struct {
			Window           int    `json:"window"`
			FailureThreshold int    `json:"failure_threshold"`
			ResetTimeout     uint64 `json:"reset_timeout"`
		}
		if err := json.Unmarshal(fx.Config, &cfg); err != nil {
			t.Fatalf("config: %v", err)
		}
		ctx := NewContext()
		cb := NewCircuitBreakerCell(ctx, cfg.Window, cfg.FailureThreshold, cfg.ResetTimeout)
		sc := cb.StateCell()
		observed := NewSlot(ctx, func(c *Compute) BreakerState { return Get(c, sc) })
		observed.Get()

		for i, step := range fx.Steps {
			switch step.Op.Type {
			case "record":
				cb.Record(step.Op.Success, step.Op.Now)
			case "allow":
				got := cb.Allow(step.Op.Now)
				if want := resReturnsBool(t, step.Returns); got != want {
					t.Fatalf("step %d allow: got %v, want %v", i, got, want)
				}
			default:
				t.Fatalf("step %d: unknown op %q", i, step.Op.Type)
			}
			if got := cb.State().String(); got != step.Expected.State {
				t.Fatalf("step %d state: got %s, want %s", i, got, step.Expected.State)
			}
			_, warm := observed.Peek()
			observed.Get()
			if (!warm) != step.Expected.Invalidates["state"] {
				t.Fatalf("step %d inval(state): got %v, want %v", i, !warm, step.Expected.Invalidates["state"])
			}
		}
	})

	t.Run("retry.json", func(t *testing.T) {
		fx, ok := loadResilienceFixture(t, "retry.json")
		if !ok {
			t.Skipf("lazily-spec fixture absent: %s", filepath.Join(resilienceSpecDir(), "retry.json"))
		}
		var cfg struct {
			Base uint64 `json:"base"`
			Cap  uint64 `json:"cap"`
		}
		if err := json.Unmarshal(fx.Config, &cfg); err != nil {
			t.Fatalf("config: %v", err)
		}
		ctx := NewContext()
		r := NewRetryPolicyCell(ctx, cfg.Base, cfg.Cap)
		dc := r.DelayCell()
		observed := NewSlot(ctx, func(c *Compute) uint64 { return Get(c, dc) })
		observed.Get()

		for i, step := range fx.Steps {
			if step.Op.Type != "next" {
				t.Fatalf("step %d: unknown op %q", i, step.Op.Type)
			}
			got := r.NextDelay()
			if want := resReturnsU64(t, step.Returns); got != want {
				t.Fatalf("step %d delay: got %d, want %d", i, got, want)
			}
			if r.Delay() != step.Expected.Delay {
				t.Fatalf("step %d expected.delay: got %d, want %d", i, r.Delay(), step.Expected.Delay)
			}
			_, warm := observed.Peek()
			observed.Get()
			if (!warm) != step.Expected.Invalidates["delay"] {
				t.Fatalf("step %d inval(delay): got %v, want %v", i, !warm, step.Expected.Invalidates["delay"])
			}
		}
	})

	t.Run("bulkhead.json", func(t *testing.T) {
		fx, ok := loadResilienceFixture(t, "bulkhead.json")
		if !ok {
			t.Skipf("lazily-spec fixture absent: %s", filepath.Join(resilienceSpecDir(), "bulkhead.json"))
		}
		var cfg struct {
			Capacity uint64 `json:"capacity"`
		}
		if err := json.Unmarshal(fx.Config, &cfg); err != nil {
			t.Fatalf("config: %v", err)
		}
		ctx := NewContext()
		b := NewBulkheadCell(ctx, cfg.Capacity)
		uc := b.PermitsInUseCell()
		observed := NewSlot(ctx, func(c *Compute) uint64 { return Get(c, uc) })
		observed.Get()

		for i, step := range fx.Steps {
			switch step.Op.Type {
			case "acquire":
				got := b.Acquire()
				if want := resReturnsBool(t, step.Returns); got != want {
					t.Fatalf("step %d acquire: got %v, want %v", i, got, want)
				}
			case "release":
				b.Release()
			default:
				t.Fatalf("step %d: unknown op %q", i, step.Op.Type)
			}
			if b.PermitsInUse() != step.Expected.InUse {
				t.Fatalf("step %d in_use: got %d, want %d", i, b.PermitsInUse(), step.Expected.InUse)
			}
			_, warm := observed.Peek()
			observed.Get()
			if (!warm) != step.Expected.Invalidates["in_use"] {
				t.Fatalf("step %d inval(in_use): got %v, want %v", i, !warm, step.Expected.Invalidates["in_use"])
			}
		}
	})

	t.Run("timeout.json", func(t *testing.T) {
		fx, ok := loadResilienceFixture(t, "timeout.json")
		if !ok {
			t.Skipf("lazily-spec fixture absent: %s", filepath.Join(resilienceSpecDir(), "timeout.json"))
		}
		ctx := NewContext()
		to := NewTimeoutCell(ctx)
		tc := to.IsTimedOutCell()
		observed := NewSlot(ctx, func(c *Compute) bool { return Get(c, tc) })
		observed.Get()

		for i, step := range fx.Steps {
			var got bool
			switch step.Op.Type {
			case "arm":
				to.Arm(step.Op.Now, step.Op.Timeout)
				got = false
			case "tick":
				got = to.Tick(step.Op.Now)
			default:
				t.Fatalf("step %d: unknown op %q", i, step.Op.Type)
			}
			if want := resReturnsBool(t, step.Returns); got != want {
				t.Fatalf("step %d edge: got %v, want %v", i, got, want)
			}
			if to.IsTimedOut() != step.Expected.IsTimedOut {
				t.Fatalf("step %d is_timed_out: got %v, want %v", i, to.IsTimedOut(), step.Expected.IsTimedOut)
			}
			_, warm := observed.Peek()
			observed.Get()
			if (!warm) != step.Expected.Invalidates["is_timed_out"] {
				t.Fatalf("step %d inval(is_timed_out): got %v, want %v", i, !warm, step.Expected.Invalidates["is_timed_out"])
			}
		}
	})
}
