// Package postgres holds lazily-go's PostgreSQL integration tests.
//
// It is a SEPARATE MODULE on purpose (#lzgooptionalpgx). The tests here need a
// real PostgreSQL driver, and Go has no optional dependencies: anything the root
// module's packages or tests import lands in the root `require` block and travels
// to every consumer of github.com/lazily-hub/lazily-go. Keeping them here leaves
// that block EMPTY, so a reactive-signals library ships no database dependency
// for the sake of one integration suite.
//
// scripts/check-module-dependencies.sh enforces both halves: the root block stays
// empty AND these tests still exist and build, because an empty block is also
// what deleting the suite would produce.
package postgres
