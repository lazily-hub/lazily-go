module github.com/lazily-hub/lazily-go/integration/postgres

// Matches the root module's floor; see the root go.mod for why 1.24.
go 1.24

require (
	github.com/jackc/pgx/v5 v5.7.6
	github.com/lazily-hub/lazily-go v0.0.0
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	golang.org/x/crypto v0.37.0 // indirect
	golang.org/x/sync v0.13.0 // indirect
	golang.org/x/text v0.24.0 // indirect
)

// The tests here exercise the checkout they sit in, never a published version.
replace github.com/lazily-hub/lazily-go => ../..
