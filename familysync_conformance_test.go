package lazily

// Conformance replay of the reactive family-granularity sync fixture
// (#lzfamilysync) from lazily-spec (conformance/familysync). Mirrors lazily-rs
// tests/familysync_conformance.rs + lazily-zig crdt_plane.zig, and the
// lazily-formal FamilySync.lean laws (applyOp_eq_merge, applyOp_present,
// applyOp_absent_adopts, present_merge, applyOp_idem, aggregate_converges).
//
// A keyed op for a family entry that is NOT registered locally MATERIALIZES the
// entry on ingest instead of being dropped: membership propagates, values are
// adopted, a later last-writer-wins update converges, re-ingest is idempotent,
// and a derived aggregate over the family (count of `true` entries) converges.

import (
	"encoding/json"
	"testing"
)

// boolState encodes a bool family value as a one-byte inline register.
func boolState(v bool) IpcValue {
	if v {
		return IpcValueInline{Bytes: []byte{1}}
	}
	return IpcValueInline{Bytes: []byte{0}}
}

// stateBool decodes a bool family register.
func stateBool(state IpcValue) bool {
	inline, ok := state.(IpcValueInline)
	return ok && len(inline.Bytes) > 0 && inline.Bytes[0] != 0
}

// suffixOf returns the segment after the first `/` of a NodeKey path.
func suffixOf(key NodeKey) string {
	path := key.Path()
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[i+1:]
		}
	}
	return path
}

// syncFrameOf builds a full sync frame (frontier + ops) from a runtime — the Go
// analog of the Zig CrdtPlaneRuntime.syncFrame.
func syncFrameOf(r *CrdtPlaneRuntime) CrdtSync {
	return CrdtSync{Frontier: r.FrontierEntries(), Ops: r.Ops()}
}

func TestFamilySyncMaterializesRemoteKeysOnIngest(t *testing.T) {
	origin := NewCrdtPlaneRuntime(1)
	defer origin.Close()
	origin.RegisterFamilyLww("live")

	target := NewCrdtPlaneRuntime(2)
	defer target.Close()
	target.RegisterFamilyLww("live")
	epochBefore := target.MembershipEpoch()

	// Origin adds two entries under `live`.
	origin.FamilySetLww("live", "2", boolState(true), 100)
	origin.FamilySetLww("live", "3", boolState(true), 101)

	frame := syncFrameOf(origin)
	applied := target.Ingest(frame)
	if applied < 2 {
		t.Fatalf("applied = %d, want >= 2", applied)
	}

	// Membership propagated + epoch bumped.
	if got := len(target.FamilyKeys("live")); got != 2 {
		t.Fatalf("family key count = %d, want 2", got)
	}
	if target.MembershipEpoch() == epochBefore {
		t.Fatalf("membership epoch did not bump (%d)", epochBefore)
	}
	if v, ok := target.FamilyValueLww("live", "2"); !ok || !stateBool(v) {
		t.Fatalf(`live/2 = (%v,%v), want (true,true)`, v, ok)
	}
	if v, ok := target.FamilyValueLww("live", "3"); !ok || !stateBool(v) {
		t.Fatalf(`live/3 = (%v,%v), want (true,true)`, v, ok)
	}

	// Re-ingest is idempotent.
	if reapplied := target.Ingest(frame); reapplied != 0 {
		t.Fatalf("re-ingest applied = %d, want 0", reapplied)
	}
}

type familyFixture struct {
	Namespace string `json:"namespace"`
	ValueType string `json:"value_type"`
	Scenarios []struct {
		Name       string `json:"name"`
		OriginPeer PeerId `json:"origin_peer"`
		TargetPeer PeerId `json:"target_peer"`
		OriginSets []struct {
			Key   string `json:"key"`
			Value bool   `json:"value"`
			Now   int64  `json:"now"`
		} `json:"origin_sets"`
		Reingest bool `json:"reingest"`
		Expect   struct {
			TargetKeys         []string        `json:"target_keys"`
			TargetValues       map[string]bool `json:"target_values"`
			TargetPresentCount int             `json:"target_present_count"`
			TargetCountTrue    int             `json:"target_count_true"`
			TargetEpochBumped  bool            `json:"target_epoch_bumped"`
			ReingestApplied    int             `json:"reingest_applied"`
		} `json:"expect"`
	} `json:"scenarios"`
}

func TestFamilySyncMaterializeOnIngestConformance(t *testing.T) {
	raw := loadConformanceFixture(t, "familysync", "materialize_on_ingest.json")
	var fx familyFixture
	if err := json.Unmarshal(raw, &fx); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	if fx.ValueType != "bool" {
		t.Fatalf("value_type = %q, want bool", fx.ValueType)
	}
	ns := fx.Namespace

	for _, sc := range fx.Scenarios {
		t.Run(sc.Name, func(t *testing.T) {
			origin := NewCrdtPlaneRuntime(sc.OriginPeer)
			defer origin.Close()
			origin.RegisterFamilyLww(ns)

			target := NewCrdtPlaneRuntime(sc.TargetPeer)
			defer target.Close()
			target.RegisterFamilyLww(ns)
			epochBefore := target.MembershipEpoch()

			for _, set := range sc.OriginSets {
				origin.FamilySetLww(ns, set.Key, boolState(set.Value), set.Now)
			}

			frame := syncFrameOf(origin)
			if applied := target.Ingest(frame); applied <= 0 {
				t.Fatalf("applied = %d, want > 0", applied)
			}

			if sc.Reingest {
				if got := target.Ingest(frame); got != sc.Expect.ReingestApplied {
					t.Fatalf("re-ingest applied = %d, want %d", got, sc.Expect.ReingestApplied)
				}
			}

			// Membership propagation: exact key set (order-independent).
			gotKeys := target.FamilyKeys(ns)
			if len(gotKeys) != len(sc.Expect.TargetKeys) {
				t.Fatalf("key count = %d, want %d", len(gotKeys), len(sc.Expect.TargetKeys))
			}
			for _, want := range sc.Expect.TargetKeys {
				found := false
				for _, gk := range gotKeys {
					if suffixOf(gk) == want {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("expected key %q in %v", want, gotKeys)
				}
			}
			if len(gotKeys) != sc.Expect.TargetPresentCount {
				t.Fatalf("present count = %d, want %d", len(gotKeys), sc.Expect.TargetPresentCount)
			}

			// Value adoption / LWW convergence.
			for suffix, want := range sc.Expect.TargetValues {
				v, ok := target.FamilyValueLww(ns, suffix)
				if !ok {
					t.Fatalf("missing value for %q", suffix)
				}
				if stateBool(v) != want {
					t.Fatalf("%s/%s = %v, want %v", ns, suffix, stateBool(v), want)
				}
			}

			// Derived-aggregate transparency: count of `true` entries converges.
			countTrue := 0
			for _, gk := range gotKeys {
				v, ok := target.FamilyValueLww(ns, suffixOf(gk))
				if ok && stateBool(v) {
					countTrue++
				}
			}
			if countTrue != sc.Expect.TargetCountTrue {
				t.Fatalf("count_true = %d, want %d", countTrue, sc.Expect.TargetCountTrue)
			}

			if sc.Expect.TargetEpochBumped && target.MembershipEpoch() == epochBefore {
				t.Fatalf("epoch not bumped (still %d)", epochBefore)
			}
		})
	}
}
