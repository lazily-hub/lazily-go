module github.com/lazily-hub/lazily-go

// Go 1.24 is the floor: the deprecated CellMap / SlotMap compatibility aliases
// are generic type aliases, fully supported only from Go 1.24 (Go 1.23 gates
// them behind GOEXPERIMENT=aliastypeparams).
go 1.24
