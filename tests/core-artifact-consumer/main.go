package main

import (
	"fmt"

	lazily "github.com/lazily-hub/lazily-go"
)

type schema struct{ Label string }

func main() {
	ctx := lazily.NewContext()
	source := lazily.NewSource(ctx, 1)
	computed := lazily.NewComputed(ctx, func(c *lazily.Compute) int {
		return lazily.Get(c, source) * 2
	})
	runs := 0
	scope := ctx.Scope()
	lazily.Own(scope, lazily.NewEffect(ctx, func(c *lazily.Compute) func() {
		if lazily.Get(c, computed) < 2 {
			panic("computed value was not observable")
		}
		runs++
		return nil
	}))
	ctx.Batch(func() {
		source.Set(2)
		source.Set(3)
	})
	if computed.Get() != 6 || runs != 2 {
		panic(fmt.Sprintf("batch result = %d, runs = %d", computed.Get(), runs))
	}
	scope.Close()
	source.Set(4)
	if runs != 2 {
		panic("closed scope still observed updates")
	}
	typed := lazily.NewSource(ctx, schema{Label: "typed"})
	if typed.Get().Label != "typed" {
		panic("typed context value was not preserved")
	}
	fmt.Println("lazily-go artifact consumer: OK")
}
