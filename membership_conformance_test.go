package lazily

import (
	"path/filepath"
	"strconv"
	"testing"
)

// Cross-language conformance for membership + failure detection (#lzmemb) — see
// lazily-spec/conformance/membership/membership_lifecycle.json.
//
// Replays the SWIM lifecycle: each op asserts the acted peers' state, the
// alive_set (the reactive PeerSet, compared as a set), and that the PeerSet
// reader invalidates exactly when the alive set changes (via a primed Slot's
// warmth).

func membershipSpecDir() string {
	return filepath.Join("..", "lazily-spec", "conformance", "membership")
}

type membershipOp struct {
	Type string `json:"type"`
	Peer uint64 `json:"peer"`
	Now  uint64 `json:"now"`
}

type membershipExpected struct {
	States      map[string]string `json:"states"`
	AliveSet    []uint64          `json:"alive_set"`
	Invalidates bool              `json:"invalidates"`
}

type membershipStep struct {
	Op       membershipOp       `json:"op"`
	Expected membershipExpected `json:"expected"`
}

type membershipFixture struct {
	conformanceMeta
	Config struct {
		PhiThreshold   float64 `json:"phi_threshold"`
		SuspectTimeout uint64  `json:"suspect_timeout"`
		MaxSamples     int     `json:"max_samples"`
		MinStd         float64 `json:"min_std"`
	} `json:"config"`
	Initial struct {
		Peers []uint64 `json:"peers"`
	} `json:"initial"`
	Steps []membershipStep `json:"steps"`
}

func loadMembershipFixture(t *testing.T, name string) (membershipFixture, bool) {
	t.Helper()
	path := filepath.Join(membershipSpecDir(), name)
	data, err := specReadFile(path)
	if err != nil {
		return membershipFixture{}, false
	}
	var fx membershipFixture
	mustStrictJSON(t, name, data, &fx)
	return fx, true
}

func TestMembershipConformance(t *testing.T) {
	const name = "membership_lifecycle.json"
	t.Run(name, func(t *testing.T) {
		fx, ok := loadMembershipFixture(t, name)
		if !ok {
			t.Skipf("lazily-spec fixture absent: %s", filepath.Join(membershipSpecDir(), name))
		}

		config := MembershipConfig{
			PhiThreshold:   fx.Config.PhiThreshold,
			SuspectTimeout: fx.Config.SuspectTimeout,
			MaxSamples:     fx.Config.MaxSamples,
			MinStd:         fx.Config.MinStd,
		}

		ctx := NewContext()
		m := NewMembershipCell[uint64](ctx, config)

		// `initial.peers` is the alive set the steps start from. It is empty in
		// today's corpus, which is exactly why it has to be consumed rather than
		// assumed: an unread seed key is indistinguishable from a satisfied one
		// until the day the corpus fills it in.
		for _, p := range fx.Initial.Peers {
			m.Join(p, 0)
		}
		if got, want := m.PeerSet(), fx.Initial.Peers; len(got) != len(want) {
			t.Fatalf("initial alive set: got %d peers, want %d", len(got), len(want))
		}
		for _, p := range fx.Initial.Peers {
			if st, known := m.State(p); !known || st != Alive {
				t.Fatalf("initial peer %d: got (%v,%v), want Alive", p, st, known)
			}
		}

		// Observe the PeerSet reader (reads the version cell inside PeerSet).
		observed := NewSlot(ctx, func(c *Compute) int {
			Get(c, m.VersionCell())
			return len(m.PeerSet())
		})
		observed.Get() // prime

		for i, step := range fx.Steps {
			op := step.Op
			switch op.Type {
			case "join":
				m.Join(op.Peer, op.Now)
			case "heartbeat":
				m.Heartbeat(op.Peer, op.Now)
			case "leave":
				m.Leave(op.Peer, op.Now)
			case "tick":
				m.Tick(op.Now)
			default:
				t.Fatalf("step %d: unknown op %q", i, op.Type)
			}

			// Per-peer state.
			for peerStr, want := range step.Expected.States {
				id, err := strconv.ParseUint(peerStr, 10, 64)
				if err != nil {
					t.Fatalf("step %d: bad peer id %q: %v", i, peerStr, err)
				}
				got, known := m.State(id)
				if !known {
					t.Fatalf("step %d %s: peer %d unknown, want %s", i, op.Type, id, want)
				}
				if got.String() != want {
					t.Fatalf("step %d %s: state of peer %d: got %s, want %s", i, op.Type, id, got, want)
				}
			}

			// Alive set (compared as a set).
			wantSet := map[uint64]struct{}{}
			for _, p := range step.Expected.AliveSet {
				wantSet[p] = struct{}{}
			}
			gotSet := map[uint64]struct{}{}
			for _, p := range m.PeerSet() {
				gotSet[p] = struct{}{}
			}
			if len(gotSet) != len(wantSet) {
				t.Fatalf("step %d %s: alive_set size: got %v, want %v", i, op.Type, membershipKeysOf(gotSet), step.Expected.AliveSet)
			}
			for p := range wantSet {
				if _, ok := gotSet[p]; !ok {
					t.Fatalf("step %d %s: alive_set: got %v, want %v", i, op.Type, membershipKeysOf(gotSet), step.Expected.AliveSet)
				}
			}

			// PeerSet invalidation: the slot is invalidated (not warm) exactly
			// when the alive set changed.
			_, warm := observed.Peek()
			observed.Get() // re-prime
			if invalidated := !warm; invalidated != step.Expected.Invalidates {
				t.Fatalf("step %d %s: invalidates: got %v, want %v", i, op.Type, invalidated, step.Expected.Invalidates)
			}
		}
	})
}

func membershipKeysOf(m map[uint64]struct{}) []uint64 {
	out := make([]uint64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
