package lazily_test

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	lazily "github.com/lazily-hub/lazily-go"
)

const (
	expectedGeneration = 1
	expectedWindow     = 2
)

type coreSymbol struct {
	abstractID      string
	kind            string
	path            string
	signature       string
	introduced      int
	deprecatedSince *int
	replacement     string
	earliestRemoval *int
}

var expectedCoreSymbols = [][4]string{
	{"context", "type", "github.com/lazily-hub/lazily-go.Context", "type Context struct"},
	{"source", "type", "github.com/lazily-hub/lazily-go.Source", "type Source[T comparable] struct"},
	{"computed", "type", "github.com/lazily-hub/lazily-go.Computed", "type Computed[T any] struct"},
	{"effect", "type", "github.com/lazily-hub/lazily-go.Effect", "type Effect struct"},
	{"batch", "operation", "github.com/lazily-hub/lazily-go.Context.Batch", "func(*Context, func())"},
	{"scope", "operation", "github.com/lazily-hub/lazily-go.Context.Scope", "func(*Context) *TeardownScope"},
	{"scope", "type", "github.com/lazily-hub/lazily-go.TeardownScope", "type TeardownScope struct"},
	{"typed_context", "type", "github.com/lazily-hub/lazily-go.Context", "type Context struct"},
}

func parseCoreNumber(value, field string) (int, error) {
	n, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer, got %q", field, value)
	}
	return n, nil
}

func optionalCoreNumber(value, field string) (*int, error) {
	if value == "-" {
		return nil, nil
	}
	n, err := parseCoreNumber(value, field)
	if err != nil {
		return nil, err
	}
	return &n, nil
}

func validateCoreSnapshot(snapshot string) error {
	var generation, window *int
	var symbols []coreSymbol

	for index, line := range strings.Split(snapshot, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		switch {
		case len(fields) == 2 && fields[0] == "generation":
			value, err := parseCoreNumber(fields[1], "generation")
			if err != nil {
				return err
			}
			generation = &value
		case len(fields) == 2 && fields[0] == "deprecation_window_generations":
			value, err := parseCoreNumber(fields[1], "deprecation window")
			if err != nil {
				return err
			}
			window = &value
		case len(fields) == 9 && fields[0] == "symbol":
			introduced, err := parseCoreNumber(fields[5], "introduced generation")
			if err != nil {
				return err
			}
			deprecated, err := optionalCoreNumber(fields[6], "deprecated generation")
			if err != nil {
				return err
			}
			removal, err := optionalCoreNumber(fields[8], "earliest removal generation")
			if err != nil {
				return err
			}
			replacement := fields[7]
			if replacement == "-" {
				replacement = ""
			}
			symbols = append(symbols, coreSymbol{
				abstractID:      fields[1],
				kind:            fields[2],
				path:            fields[3],
				signature:       fields[4],
				introduced:      introduced,
				deprecatedSince: deprecated,
				replacement:     replacement,
				earliestRemoval: removal,
			})
		default:
			return fmt.Errorf("invalid snapshot row %d: %s", index+1, line)
		}
	}

	if generation == nil || window == nil {
		return fmt.Errorf("snapshot is missing generation policy")
	}
	if *generation != expectedGeneration || *window != expectedWindow {
		return fmt.Errorf("unsupported Core policy: generation %d, window %d", *generation, *window)
	}
	if len(symbols) != len(expectedCoreSymbols) {
		return fmt.Errorf("Core API symbol count changed: expected %d, got %d", len(expectedCoreSymbols), len(symbols))
	}

	seen := make(map[string]bool)
	for index, symbol := range symbols {
		expected := expectedCoreSymbols[index]
		actual := [4]string{symbol.abstractID, symbol.kind, symbol.path, symbol.signature}
		if actual != expected {
			return fmt.Errorf("Core API drift: expected %q, got %q", expected, actual)
		}
		key := symbol.abstractID + "\x00" + symbol.path
		if seen[key] {
			return fmt.Errorf("duplicate Core mapping: %s", symbol.path)
		}
		seen[key] = true
		if symbol.introduced > *generation {
			return fmt.Errorf("%s is introduced after this generation", symbol.path)
		}

		if symbol.deprecatedSince == nil && symbol.replacement == "" && symbol.earliestRemoval == nil {
			continue
		}
		if symbol.deprecatedSince == nil || symbol.replacement == "" || symbol.earliestRemoval == nil ||
			*symbol.earliestRemoval < *symbol.deprecatedSince+*window {
			return fmt.Errorf("%s has an incomplete deprecation lifecycle", symbol.path)
		}
	}
	return nil
}

// These method expressions are native compile-time compatibility gates. Any
// incompatible receiver, parameter, or return change fails before tests run.
var _ func(*lazily.Context, func()) = (*lazily.Context).Batch
var _ func(*lazily.Context) *lazily.TeardownScope = (*lazily.Context).Scope

// Instantiate the promised generic shapes. Computed deliberately uses a
// non-comparable type so narrowing its `any` constraint is also a compile error.
var _ *lazily.Source[int]
var _ *lazily.Computed[[]byte]
var _ *lazily.Effect

func readCoreSnapshot(t *testing.T) string {
	t.Helper()
	contents, err := os.ReadFile("api/core-v1.tsv")
	if err != nil {
		t.Fatalf("read Core API snapshot: %v", err)
	}
	return string(contents)
}

func TestCommittedCoreAPISnapshotMatchesNativeSurface(t *testing.T) {
	if err := validateCoreSnapshot(readCoreSnapshot(t)); err != nil {
		t.Fatal(err)
	}
}

func TestCoreAPIGateRejectsSymbolMutations(t *testing.T) {
	snapshot := readCoreSnapshot(t)
	removed := strings.Replace(snapshot,
		"symbol\teffect\ttype\tgithub.com/lazily-hub/lazily-go.Effect\ttype Effect struct\t1\t-\t-\t-\n",
		"", 1)
	if err := validateCoreSnapshot(removed); err == nil {
		t.Fatal("removed Core symbol passed the compatibility gate")
	}

	added := snapshot + "symbol\tpreview\ttype\tgithub.com/lazily-hub/lazily-go.Preview\ttype Preview struct\t1\t-\t-\t-\n"
	if err := validateCoreSnapshot(added); err == nil {
		t.Fatal("added Core symbol passed without an intentional snapshot update")
	}
}

func TestCoreAPIGateRejectsIncompleteDeprecationLifecycle(t *testing.T) {
	snapshot := readCoreSnapshot(t)
	invalid := strings.Replace(snapshot,
		"symbol\teffect\ttype\tgithub.com/lazily-hub/lazily-go.Effect\ttype Effect struct\t1\t-\t-\t-",
		"symbol\teffect\ttype\tgithub.com/lazily-hub/lazily-go.Effect\ttype Effect struct\t1\t1\t-\t-", 1)
	err := validateCoreSnapshot(invalid)
	if err == nil || !strings.Contains(err.Error(), "incomplete deprecation lifecycle") {
		t.Fatalf("deprecation mutation should fail lifecycle validation, got %v", err)
	}
}
