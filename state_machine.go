// Flat finite state machine backed by a reactive Cell.
//
// Ported from lazily-dart lib/src/state_machine.dart (feature row: "Flat state
// machine"). The state lives in a Cell, so any Slot, Signal, Memo, or
// subscriber that reads the state is automatically invalidated when the machine
// transitions.
//
// The transition function is pure: (state, event) -> (next, ok). Returning
// ok == false rejects the event (a guard); returning ok == true accepts the
// event and sets the cell to the returned state. A self-transition that returns
// an equal state is accepted but suppressed by the Cell's != guard, so no
// downstream cascade fires.
//
// Both S and E must be comparable: S because it is stored in a Cell (which
// requires comparable for its PartialEq guard), and E for symmetry with the
// reactive family (events are compared by value at call sites and never boxed).

package lazily

// Transition is the pure transition function for a StateMachine. Given the
// current state and an incoming event it returns the next state and whether the
// event was accepted. Returning ok == false rejects the event (acts as a
// guard); the machine's state is left unchanged.
type Transition[S comparable, E comparable] func(state S, event E) (next S, ok bool)

// StateMachine is a flat finite state machine whose current state lives in a
// reactive Cell. Reading State inside a computation subscribes the reader, so
// downstream reactives recompute when the machine transitions to a different
// state.
type StateMachine[S comparable, E comparable] struct {
	ctx        *Context
	cell       *Cell[S]
	transition Transition[S, E]
}

// NewStateMachine creates a machine bound to ctx with the given initial state
// and transition function.
func NewStateMachine[S comparable, E comparable](ctx *Context, initial S, transition Transition[S, E]) *StateMachine[S, E] {
	return &StateMachine[S, E]{
		ctx:        ctx,
		cell:       NewCell[S](ctx, initial),
		transition: transition,
	}
}

// State returns the current state. Reading inside a computation subscribes the
// reader.
func (m *StateMachine[S, E]) State() S { return m.cell.Get() }

// Cell returns the underlying Cell holding the state value.
func (m *StateMachine[S, E]) Cell() *Cell[S] { return m.cell }

// Send delivers an event to the machine. It returns true if the transition
// function accepted the event (ok == true), false if it was rejected. A
// self-transition that returns an equal state returns true but does not
// invalidate dependents (the != guard on the cell).
func (m *StateMachine[S, E]) Send(event E) bool {
	next, ok := m.transition(m.cell.Peek(), event)
	if ok {
		m.cell.Set(next)
		return true
	}
	return false
}

// OnTransition registers a handler fired with (old, new) on a transition to a
// different state. It is not called on registration. It returns a disposer;
// call it to stop observing.
//
// This is an EFFECT, not a callback registered on the Cell — observation in a
// reactive graph is a declared dependency edge. The effect reads the state cell,
// which is what makes it a dependent; a captured `prev` turns the level-triggered
// rerun into an edge-triggered (old, new) pair. Mirrors lazily-rs
// StateMachine::on_transition (src/state_machine.rs).
//
// Batching consequence: an effect reruns once per settled cascade, so a batch
// that walks A -> B -> C reports the single transition (A, C) rather than
// (A, B) and (B, C). That is intended — a batch asserts atomicity, and the
// intermediate B was never an observable state of the graph.
func (m *StateMachine[S, E]) OnTransition(handler func(oldState, newState S)) func() {
	var prev S
	primed := false
	effect := NewEffect(m.ctx, func(ctx *Context) func() {
		current := m.cell.Get()
		// The first run only establishes the baseline — OnTransition is not
		// called on registration.
		if primed && current != prev {
			handler(prev, current)
		}
		prev = current
		primed = true
		return nil
	})
	return effect.Dispose
}
