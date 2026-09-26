module github.com/lazily-hub/lazily-go

// Go 1.24 is the floor: the deprecated CellMap / SlotMap compatibility aliases
// are generic type aliases, fully supported only from Go 1.24 (Go 1.23 gates
// them behind GOEXPERIMENT=aliastypeparams).
go 1.24

require github.com/jackc/pgx/v5 v5.7.6

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	golang.org/x/crypto v0.37.0 // indirect
	golang.org/x/sync v0.13.0 // indirect
	golang.org/x/text v0.24.0 // indirect
)
