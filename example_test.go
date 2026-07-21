package lazily_test

import (
	"fmt"

	lazily "github.com/lazily-hub/lazily-go"
)

// Greeter demonstrates the idiomatic Go equivalent of a lazily-"decorated"
// method (see the README § Reactive members on a struct). Go has no decorators,
// so reactive members are wired as Slot/Cell/Memo/Signal fields in the
// constructor and exposed via thin accessor methods.
type Greeter struct {
	Name     *lazily.SourceCell[string]
	greeting *lazily.FormulaCell[string] // the "decorated" lazy member
}

// NewGreeter wires the reactive members. greeting tracks Name automatically and
// recomputes only after Name changes.
func NewGreeter(ctx *lazily.Context) *Greeter {
	g := &Greeter{Name: lazily.NewSourceCell(ctx, "")}
	g.greeting = lazily.NewFormulaCell(ctx, func(*lazily.Context) string {
		return "Hello, " + g.Name.Get() + "!"
	})
	return g
}

// Greeting reads like a normal method but is lazy, cached, and reactive.
func (g *Greeter) Greeting() string { return g.greeting.Get() }

// ExampleGreeter shows a reactive member exposed as a method: it is computed
// lazily on first read, cached, and recomputed only when a dependency changes.
func ExampleGreeter() {
	ctx := lazily.NewContext()
	g := NewGreeter(ctx)

	g.Name.Set("World")
	fmt.Println(g.Greeting()) // computed on first read, then cached
	fmt.Println(g.Greeting()) // cached — no recompute

	g.Name.Set("Go")
	fmt.Println(g.Greeting()) // recomputed once, because Name changed
	// Output:
	// Hello, World!
	// Hello, World!
	// Hello, Go!
}
