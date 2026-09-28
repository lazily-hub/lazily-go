module github.com/lazily-hub/lazily-go

// Go 1.24 is the floor: the deprecated CellMap / SlotMap compatibility aliases
// are generic type aliases, fully supported only from Go 1.24 (Go 1.23 gates
// them behind GOEXPERIMENT=aliastypeparams).
go 1.24

// No `require` block, and that is enforced (#lzgooptionalpgx).
// The PostgreSQL integration suite that needs a driver lives in its own module
// under integration/postgres, so consumers of this module resolve nothing.
// scripts/check-module-dependencies.sh fails if a requirement reappears here or
// if that suite stops existing.
