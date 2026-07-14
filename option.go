// Shared comparable optional used across the realtime + distributed primitive
// families (temporal / rateshape / coordination / presence / windowing).
//
// Go cells require a comparable payload, so the analogue of rs `Option<T>` is a
// small comparable struct rather than a pointer. Storing it in a Cell gets the
// PartialEq store-guard for free: emitting the same value twice does not
// cascade, and an absent value never touches the cell.

package lazily

// Opt is a comparable optional value. Present distinguishes a set value from the
// zero value. Comparable whenever T is comparable, so it can back a Cell.
type Opt[T comparable] struct {
	Present bool
	Value   T
}

// Some builds a present Opt.
func Some[T comparable](v T) Opt[T] { return Opt[T]{Present: true, Value: v} }

// None builds an absent Opt.
func None[T comparable]() Opt[T] { return Opt[T]{} }

// OptStr is the Option<string> projection several fixtures exercise.
type OptStr = Opt[string]
