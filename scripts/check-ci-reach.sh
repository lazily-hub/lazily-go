#!/usr/bin/env bash
# CI-reachability guard (#lzcheckcireachguard).
#
# Fails the build when `make check` runs a gate that CI never reaches. That is the
# drift this guard exists for: someone adds a target to `check`, it passes locally
# forever, and no CI job ever executes it — which is exactly how #lzinteroppeerci
# happened. The interop peer, the single cross-binding wire-compatibility gate, was
# in every binding's `check` and in no binding's workflow, for months.
#
# It also exists because the obvious hand-audit is WRONG. Grepping the workflows
# for "make check" reported all nine bindings as covered; every one of those hits
# was a COMMENT. Comments are the reason this is a script and not a convention:
# only `run:` bodies count here, and comment lines inside them are stripped before
# anything is matched.
#
# WHAT IT PROVES
#
#   For every target in `check`'s prerequisite closure, THE CI STEP PINNED FOR
#   THAT TARGET invokes the same program with the same distinguishing flags.
#
#   "The pinned step", not "some step anywhere in the workflow", since
#   #reversereachdirection. See EXPECTED_GATE_STEPS below for why, and for the
#   measured exit-0 verdicts the flat version returned.
#
#   And, since #pinreachclosure, three things about the closure ITSELF, which
#   nothing here used to audit:
#
#     A. that `make -n check` really runs each closure target's commands (the
#        ORACLE — the awk-derived closure is source text, and make conditionals
#        make source text and executed set two different things),
#     B. WHICH targets the closure contains, by SET EQUALITY against
#        EXPECTED_CLOSURE_TARGETS, and
#     C. which of them legitimately carry no gate, by set equality against
#        EXPECTED_NOGATE_TARGETS, because `no gate` is an exemption and a target
#        can be moved into it by emptying its recipe.
#
#   B is meaningful only on top of A. Each block carries the measured pre-fix
#   verdict for the attack it closes.
#
# WHAT IT DOES NOT PROVE
#
#   That CI runs it against the same inputs, in the same environment, or that the
#   command means the same thing there. Reach is a floor, not equivalence. The
#   sibling guards (conformance-coverage, assertion-keys, scenario-coverage) are
#   what prove a run examined anything.
#
# HOW A TARGET IS MATCHED
#
#   Recipes are read through `make -n`, so make variables are already expanded and
#   we compare real command lines rather than source text. `make -p` is
#   deliberately NOT used: it dumps the entire environment to stdout, which would
#   print every secret in the job's env into the CI log.
#
#   Each command is split on the shell's sequencing operators, redirections are
#   dropped, and the remainder is reduced to an ANCHOR: the program basename plus
#   its subcommands and flag NAMES (values dropped), with path arguments reduced to
#   basenames and bare path globs discarded. A target is reached when EVERY one of
#   its anchors is a subsequence of some command's token list IN THE CI STEP
#   PINNED FOR IT (EXPECTED_GATE_STEPS), or when CI runs `make <target>` directly.
#   Every, not any: a target that runs two gates and is half-covered by CI is a
#   gap, and "any" would report it green.
#
#   Keeping flag names in the anchor is what makes the guard falsifiable rather
#   than decorative: `go test -race` does not match a CI step that only runs
#   `go test -count=1`, so dropping the race job reddens this guard instead of
#   being absorbed by the plain test job.
#
#   An argument that is still a VARIABLE reference at this point — `$MANIFEST` in
#   a CI step, or a `$$VAR` a recipe leaves for the shell — names a value the
#   guard cannot resolve, so it becomes a WILDCARD matching exactly one token on
#   the other side (#lzcireachvaranchor). Make and CI routinely spell the same
#   path differently, one through an expanded `$(VAR)` and the other through the
#   environment, and they are the same command. Dropping the token instead, which
#   is what this used to do, lost the argument as well as its value and reported
#   a step that genuinely ran the gate as unreachable — a false RED that cost one
#   binding a hardcoded second spelling of the path plus a hand-written equality
#   assertion, which is a new drift surface invented to satisfy a guard whose job
#   is detecting drift. Arity still counts: `script.sh $A` does not match a CI
#   step that passes no argument at all.
#
#   Commands whose program is a shell builtin or a plain file/text utility carry no
#   gate, so they contribute no anchor. A target with no non-trivial command at all
#   (a mkdir-only reset step, say) is reported as carrying no gate and is not
#   required to appear in CI. It cannot fail a build, so it cannot hide one.
#
# THE EXCUSE LIST IS THE OTHER HALF OF THE DELIVERABLE
#
#   scripts/ci-reach.conf names the workflows that count and the targets that are
#   deliberately local-only, each with a reason. It is the one place a reader can
#   see what this binding does not enforce in CI, in the same spirit as
#   KNOWN_UNCOVERED. Excuses are checked in BOTH directions: an excused target that
#   CI turns out to reach fails too, so the list cannot rot into a list of things
#   that used to be true. Since #pinreachclosure they are also checked for SCOPE:
#   an excuse naming a target that is not in the closure at all is an error, not a
#   silent no-op, the same reverse check KNOWN_UNCOVERED already applies.
set -euo pipefail

MAKE_BIN="${MAKE:-make}"
# Stand-in name for a run: step with no `name:`. Deliberately not a string a real
# step name can be; the same literal is emitted by ci_commands/ci_step_names.
UNNAMED_STEP="$(printf '\002unnamed')"
ROOT_TARGET="${CI_REACH_ROOT_TARGET:-check}"
CONF="${CI_REACH_CONF:-scripts/ci-reach.conf}"

if [ ! -f Makefile ]; then
	echo "check-ci-reach: no Makefile in $(pwd)" >&2
	exit 1
fi

# ------------------------------------------------------- closure membership pin

# WHICH targets `check` runs, not how many (#pinreachclosure).
#
# Measured, on a scratch copy of this repo's Makefile verified byte-identical with
# `cmp` first, with `test-interop-peer` deleted from `check`'s prerequisite list
# and nothing else changed:
#
#   reached  fmt-check ... reached  ci-reach
#   no gate  check                            recipe runs no checkable command
#   check-ci-reach: OK — 8 target(s) reached by CI, 0 excused, 1 carrying no gate
#   exit 0
#
# The gate that left is the interop peer: the single cross-binding
# WIRE-COMPATIBILITY check, whose months-long absence from every binding's CI
# (#lzinteroppeerci) is the reason this script exists at all. The guard approved,
# printed one fewer line, and never said a word about which line.
#
# The vacuity floor near the bottom cannot see this. It fires when
# reached + excused + unreached is ZERO — when EVERY gate has left. It catches
# losing all nine and approves losing eight, which is the shape of an attack
# nobody runs. This is the one that gets run, because dropping a prerequisite is a
# one-line self-approving edit.
#
# SET EQUALITY, deliberately, not a count.
#
#   A FLOOR passes a swap: drop the interop peer, add a lint target, the count is
#   unchanged and the wire gate is gone.
#   A CEILING self-disables: it starts at zero slack and gains slack with every
#   legitimate migration until the same attack passes again.
#
# The property that matters is fails-when-stale, never passes-when-stale — the
# same reasoning that replaced MAX_LEDGERED_BLOCKS with EXPECTED_LEDGERED_BLOCKS
# in this family. The `EXPECTED_` prefix already means exact equality in these
# repos, so the name carries the semantics; `MIN_`, `MAX_` and `KNOWN_` would all
# misstate it.
#
# It lives here, at the top of the guard, sorted, one entry per line with a note
# on what the target gates — the shape KNOWN_UNCOVERED already uses at the top of
# the sibling scripts/check-conformance-coverage.sh. No new configuration file:
# ci-reach.conf answers a different question (what this binding does NOT enforce),
# and this list is a claim about what it DOES.
#
# Yes, it duplicates the Makefile's `check:` line. That is the mechanism, not an
# accident: the pin makes a retiring edit INCOMPLETE, so it cannot be a one-line
# deletion, and its second place is a reviewable statement of intent.
#
# Not "the attack was invisible in a diff" — that argument was measured and does
# not hold. lazily-kt checked it against five attacks and it explained exactly
# one: the rename, the `ifeq` decoupling, the neutered recipe and the recipe swap
# are all equally visible one-line edits, and three of those four went undetected
# before #pinreachclosure. Visibility is not what these pins buy. What they buy
# is stated once, in lazily-gd's words: NO SILENT CHANGE, never correctness. A
# pin cannot name a gate that never existed, and written from a broken Makefile
# it would faithfully pin the breakage.
#
# THE RECIPE SWAP IS CLOSED, AND IT WAS NEVER HALF-CAUGHT HERE
#
#   #closeremaininghalf recorded the recipe swap as open and recorded half of it
#   as already caught, by "an anchor-collision rung [that] refuses two members
#   that reduce to the same anchor set". THAT RUNG DOES NOT EXIST IN THIS SCRIPT.
#   Measured against the guard as it stood at d8cc305, on a byte-verified scratch
#   copy of this Makefile with `test-interop-peer:`'s recipe replaced by
#   `go build ./...` — another MEMBER's gate, the case the note called caught:
#
#     reached  test-interop-peer
#     check-ci-reach: OK — 9 target(s) reached by CI, 0 excused, 1 carrying no gate
#     exit 0, verdict byte-identical (`cmp`) to healthy
#
#   Neither half was caught. The claim was inherited from a sibling binding that
#   really has that rung and was never checked against this file. It is corrected
#   here rather than deleted, because a guard header that overstates its own
#   coverage is what stops the next reader from looking.
#
#   Both halves are closed now, by pinning per gate the NAME of the CI step that
#   runs it and requiring that gate's anchors INSIDE that step
#   (#reversereachdirection — EXPECTED_GATE_STEPS below, which carries the
#   measured pre-fix verdict for each attack). Repoint a recipe at any other
#   step's command — another member's gate, or a step no member runs — and its
#   anchors are no longer in ITS step, so the guard exits 1 naming the target, its
#   pinned step, and the anchor absent from it. No per-recipe pin, so the churn is
#   step-name-rate and not recipe-rate: a recipe gaining a flag moves the recipe
#   and the CI step together, and the mapping does not move. That is the property
#   the per-recipe pin lacked, and why that one was right to decline.
#
# THREE THINGS THESE PINS STILL DO NOT CATCH. Recorded here rather than in a
# commit message, because this is where the next reader looks.
#
#   * A RECIPE WEAKENED INSIDE ITS OWN PINNED STEP. The step mapping closes
#     moving a gate; it does not close blunting one in place, because a target
#     anchor is matched as a SUBSEQUENCE and extra tokens on the CI side are
#     allowed by design — that is what lets CI be the stricter of the two. So
#     dropping a flag from the RECIPE still matches. Measured post-fix, on
#     byte-verified scratch copies of this Makefile, both at exit 0 with the
#     verdict byte-identical to healthy:
#
#       test-interop-peer: CGO_ENABLED=1 go run -race ./cmd/lazily-interop-peer
#                                         (no --self-check)     reached, exit 0
#       test-interop-peer: go run ./cmd/lazily-interop-peer --self-check
#                                         (no -race)            reached, exit 0
#
#     The OPPOSITE direction is red, which is the direction that protects CI:
#     dropping `--self-check` from the CI step's own run: body fails with
#     "step `Interop peer self-check (#lzinteroppeerci)` does not match
#     `go run -race lazily-interop-peer --self-check`". So the floor holds
#     against CI decaying and not against the recipe decaying. Closing that needs
#     the per-recipe pin declined above for churn, and it is a WEAKER attack than
#     the swap: CI's step still runs the real command, so the wire break is still
#     caught in CI and it is local closeout that degrades. #lzinteroppeerci was
#     the inverse of that, and the inverse is caught.
#   * ORDER. `check:`'s prerequisites are a set here, and a set cannot carry
#     order. Nothing in go depends on it today: every closure target is
#     self-contained and the one real ordering (the suite writes evidence, the
#     coverage guard reads it) lives inside `test`'s own recipe.
#   * EDGES. Dropping an edge between two closure members can leave this set
#     identical AND pass the oracle, when another member already pulls the
#     dependency into the root's run — `make check` keeps working while
#     `make <target>` alone breaks. go's closure is a depth-1 star today, so
#     there is no such edge to drop; it arrives the moment a member gains a
#     prerequisite that is also a member.
EXPECTED_ROOT_TARGET="check"
EXPECTED_CLOSURE_TARGETS=(
	"assertion-ordering-check" # observation ordering (#lzassertordering)
	"build"                    # go build ./...
	"check"                    # the root itself — @echo only, carries no gate
	"ci-reach"                 # this guard, so CI has to reach it too
	"conformance-coverage"     # rungs 1+4 (#lzguardsnotinci); pulls in `test`
	"fmt-check"                # gofmt
	"race"                     # CRDT reads are graph WRITES (v0.23.2 data race)
	"test"                     # go test -count=1 ./... + the evidence recorders
	"test-interop-peer"        # cross-binding wire compatibility (#lzinteroppeerci)
	"vet"                      # go vet ./...
)

# Which closure targets are ALLOWED to carry no gate (#pinreachclosure, part C).
#
# `no gate` is an EXEMPTION: a target whose recipe runs no checkable command is
# reported and then excused from having to appear in CI, on the reasoning in the
# header that a recipe which cannot fail cannot hide a failure. True of a
# mkdir-only reset step. Not true of a target that USED to carry a gate, because
# emptying a recipe moves it into the exemption silently.
#
# Measured on a byte-verified scratch copy of this Makefile, with
# `test-interop-peer`'s `CGO_ENABLED=1 go run -race ./cmd/lazily-interop-peer
# --self-check` replaced by an `@echo`, membership and every name left intact:
#
#   no gate  test-interop-peer                recipe runs no checkable command
#   check-ci-reach: OK — 8 target(s) reached by CI, 0 excused, 2 carrying no gate
#   exit 0
#
# The membership pin above is satisfied — the name never moved. So the set of
# exempt targets is pinned too. Today it is just the root, whose recipe is one
# `@echo`, which makes this pin cheap to hold and specific enough to be worth
# holding.
EXPECTED_NOGATE_TARGETS=(
	"check" # `@echo "lazily-go: check OK"` — the root announces, it does not gate
)

# ------------------------------------------------------ the gate-to-step mapping

# WHICH CI STEP runs each gate (#reversereachdirection).
#
# Until now `anchor_reached` asked whether SOME command in a flat set — every
# `run:` body in the workflow, merged — contained the target's anchors. A flat
# haystack cannot tell "CI runs this gate" from "CI runs something that looks like
# this gate, somewhere else, for another reason". So a recipe could be repointed
# at any command the workflow already runs and reach stayed green.
#
# MEASURED, against the guard as it stood at d8cc305, on byte-verified scratch
# copies of this Makefile (restored and re-`cmp`ed after each):
#
#   1. `test-interop-peer:` -> `go test -run Conformance -v .` — a real command in
#      a real step of this workflow ("Conformance replay executed, zero skips")
#      that NO closure member runs:
#
#        reached  test-interop-peer
#        check-ci-reach: OK — 9 target(s) reached by CI, 0 excused, 1 carrying no gate
#        exit 0, byte-identical (`cmp`) to the healthy verdict
#
#   2. `test-interop-peer:` -> `go build ./...` — another MEMBER's gate:
#
#        same output, exit 0, byte-identical to the healthy verdict
#
#   In both, `make check` no longer runs the cross-binding WIRE-COMPATIBILITY
#   check at all. #lzinteroppeerci is the months-long absence of exactly that gate
#   from CI and the reason this script exists; the guard written for it approved
#   its removal twice without printing a different character.
#
# So reach is asked INSIDE the pinned step. Repoint a member's recipe anywhere
# else and its anchors are not in ITS step: exit 1, naming the member, the step,
# and the anchor.
#
# WHY A STEP NAME AND NOT THE RECIPE TEXT. A per-recipe pin (a second spelling of
# each recipe in this file) churns at RECIPE rate: every added flag edits it, so it
# gets updated reflexively and decays into the passes-when-stale check this family
# already removed once. A step name churns at STEP-NAME rate. A recipe gaining a
# flag moves the recipe and its CI step together and leaves this mapping alone,
# which is why this pin can be held honestly and that one could not.
#
# WHERE THIS DESIGN DOES NOT APPLY, and why it applies fully here. It asserts
# nothing where CI's instruction is "run the make target" rather than a spelling
# of the gate: `make check` faithfully runs whatever the root runs, so there is no
# independent CI-side spelling to cross-examine and a repoint is undetectable from
# CI — correctly. lazily-gd is excluded for that reason. lazily-go is the opposite
# extreme, measured rather than assumed: this workflow invokes make ZERO times.
# All eight `make check` / `make test` hits `grep` finds in ci.yml are COMMENTS,
# which is the same trap recorded at the top of this file, so all 9 gate-carrying
# members are anchor-reached and all 9 are mappable. `make_invokes` below still
# credits reach without anchors if a step ever runs `make <target>` directly; such
# a target has no CI-side spelling to pin, so it is excluded from this mapping and
# pinning it is refused.
#
# IT ALSO CLOSES THE SAME WEAKNESS RUNNING THE OTHER WAY: DELETING A STEP.
# Found in lazily-cs, which deleted its whole `test` step and stayed green because
# a NARROWER step's anchor was a token superset of the broad member's. Swept here
# against all 9 mapped gates, by deleting each pinned step from a byte-verified
# scratch copy of ci.yml and running the d8cc305 guard: every one exits 1 already.
# It does not reproduce in go — but it is ONE FLAG deep, and the flag is in the
# Makefile:
#
#   go test -count=1 ./...        -> anchor `go test -count`
#   go test -run Conformance -v . -> anchor `go test -run Conformance -v`
#
# `-count` is the only token keeping the `test` member out of the conformance
# step's command. Drop `-count=1` from the `test` recipe — an ordinary edit,
# defensible on its own terms — and, measured against d8cc305 with the `test` step
# deleted from ci.yml outright:
#
#   reached  test
#   check-ci-reach: OK — 9 target(s) reached by CI, 0 excused, 1 carrying no gate
#   exit 0, byte-identical (`cmp`) to the healthy verdict
#
# `go test ./...` — the entire suite — gone from CI, credited to the `-run
# Conformance` step that replays a filtered subset of it. Post-fix that is exit 1
# at the step-exists rung, naming the pin and the step ci.yml no longer has.
#
# So `-count=1` in the `test` recipe is load-bearing for THIS GUARD and not only
# for Go's test cache, which is the only reason the Makefile gives for it. Both
# reasons are real and neither is a substitute for the other. The step mapping is
# what makes that coupling stop mattering.
#
# THE ARRAY IS NOT THE CHECK, and the verdict line LIES without the scoping.
# Falsified by reverting only the one line that asks the scoped question —
# `anchor_reached_in "$a" "$step_scope_file"` back to `anchor_reached "$a"` —
# with this array, its set-equality rung, the step-exists rung and the
# duplicate-name rung all left in place. Attack 1 above returns to exit 0, and the
# run still prints `step-mapped — 9 gate(s) matched INSIDE the CI step pinned for
# each, 9 mapping entr(ies), set-equal` while matching nothing of the kind. The
# mapping is data; the scoped reach is the check. They have to move together.
#
# ONE STEP PER ENTRY, and a target may appear more than once if its anchors
# legitimately span two steps. Measured here: every one of the 9 members reduces
# to exactly ONE anchor, and each anchor is satisfied by exactly ONE of the
# workflow's 10 anchor-bearing steps — no member spans two steps and no member's
# anchor is ambiguous between two — so step-scoping reddens nothing that is
# legitimate today. Two members sharing one step is NOT refused: it is a
# legitimate CI shape (one step running two gates), and it is not how the swap
# hides, because the swapped member's own pinned step is what is checked.
EXPECTED_GATE_STEPS=(
	"assertion-ordering-check|Assertion observation ordering (#lzassertordering)"
	"build|build"
	"ci-reach|CI-reachability guard (#lzcheckcireachguard)"
	"conformance-coverage|Conformance coverage + scenario ledger guard (#lzguardsnotinci)"
	"fmt-check|gofmt"
	"race|race (cgo)"
	"test|test"
	"test-interop-peer|Interop peer self-check (#lzinteroppeerci)"
	"vet|vet"
)

# A pin that names nothing pins nothing, and an empty array is what a bad merge or
# a stray edit leaves behind. Vacuity floor for the pin itself (#lzvacuousrun).
if [ "${#EXPECTED_CLOSURE_TARGETS[@]}" -eq 0 ]; then
	echo "check-ci-reach: EXPECTED_CLOSURE_TARGETS is empty — a pin that names no target pins nothing" >&2
	exit 1
fi

pin_dupes="$(printf '%s\n' "${EXPECTED_CLOSURE_TARGETS[@]}" | sort | uniq -d)"
if [ -n "$pin_dupes" ]; then
	echo "check-ci-reach: EXPECTED_CLOSURE_TARGETS names the same target more than once:" >&2
	while IFS= read -r d; do
		[ -n "$d" ] || continue
		echo "  - $d" >&2
	done <<<"$pin_dupes"
	echo "A closure is a SET. A duplicate entry is harmless to the equality check and" >&2
	echo "a sign the list was edited by appending rather than read — fix it there." >&2
	exit 1
fi

# The exemption pin has to be a SUBSET of the membership pin: a name exempted
# from CI reach that the closure does not even contain is a typo that reads as a
# decision. Static, so it is caught without asking make anything.
pin_all="$(printf '%s\n' "${EXPECTED_CLOSURE_TARGETS[@]}")"
pin_nogate_static="$(printf '%s\n' "${EXPECTED_NOGATE_TARGETS[@]:+${EXPECTED_NOGATE_TARGETS[@]}}")"
for exempt in "${EXPECTED_NOGATE_TARGETS[@]:+${EXPECTED_NOGATE_TARGETS[@]}}"; do
	if ! grep -qxF "$exempt" <<<"$pin_all"; then
		echo "check-ci-reach: EXPECTED_NOGATE_TARGETS names '$exempt', which is not in EXPECTED_CLOSURE_TARGETS." >&2
		echo "  Only a target in the closure can be exempted from carrying a gate." >&2
		exit 1
	fi
done

# Pin the root's NAME too, so the pin cannot be satisfied by auditing a different
# root than the one it was written for. EXPECTED_CLOSURE_TARGETS is the closure of
# one specific root; compared against another root's closure it is two unrelated
# sets, and the set-difference dump would be noise rather than a diagnosis.
if [ "$ROOT_TARGET" != "$EXPECTED_ROOT_TARGET" ]; then
	echo "check-ci-reach: auditing root target '$ROOT_TARGET', but the closure pin is written for '$EXPECTED_ROOT_TARGET'." >&2
	echo "  Either restore the root target name, or move the pin to the new root" >&2
	echo "  deliberately — EXPECTED_ROOT_TARGET and every entry in" >&2
	echo "  EXPECTED_CLOSURE_TARGETS, in the same commit." >&2
	exit 1
fi

# -------------------------------------------- the gate-to-step mapping, checked

# ------------------------------------------------- the make-invoked mode pin

# WHICH members CI reaches by invoking make, rather than by spelling the gate
# (#reversereachdirection).
#
# `make_invokes` credits a member with reach and asks NOTHING about anchors: CI's
# instruction is "run the target", so it faithfully runs whatever the recipe says
# and there is no independent CI-side spelling to cross-examine. That is correct,
# and it makes which members are in that mode a load-bearing fact — a member in
# make mode has no gate-step entry and no anchor check.
#
# EXPECTED_GATE_STEPS alone does NOT pin it. Found in lazily-cpp and lazily-dart,
# reproduced here: change ONE member's CI step body to `make <that member>` AND
# delete its EXPECTED_GATE_STEPS entry, in the same edit. Each half alone exits 1;
# together they cancel, because the deleted entry was the only evidence that the
# mode had changed. Measured on the tree at ddeaec6, on byte-verified scratch
# copies, with `race (cgo)`'s body set to `make race` and `"race|race (cgo)"`
# removed:
#
#   reached  race
#   check-ci-reach: step-mapped — 8 gate(s) ... 8 mapping entr(ies), set-equal
#   check-ci-reach: OK — 9 target(s) reached by CI, 0 excused, 1 carrying no gate
#   exit 0
#
# The ONLY trace was 9 dropping to 8 on one line. The CI step that runs this guard
# checks the exit status and greps for `check-ci-reach: OK`; both are satisfied, so
# that goes GREEN on the runner. dart put it exactly: a population pinned only as
# the COMPLEMENT of another pinned population is not pinned against an edit that
# moves both together. A COUNT IS NOT A PIN.
#
# And it is not cosmetic. Loaded up — the interop peer's CI step set to
# `make test-interop-peer`, its gate-step entry dropped, and its recipe repointed
# at `go build ./...`, all in one edit — the same measurement gives:
#
#   check-ci-reach: OK — 9 target(s) reached by CI, 0 excused, 1 carrying no gate
#   exit 0, with `check-ci-reach: OK` printed for CI to grep
#   `make -n check | grep -c lazily-interop-peer` -> 0
#
# The cross-binding WIRE-COMPATIBILITY gate running NOWHERE, local or CI, with the
# guard built for its absence reporting OK. That is a fifth route to retiring
# #lzinteroppeerci, and it exists only because the mode was unpinned.
#
# So the mode is pinned directly, by SET EQUALITY in both directions, and it is
# MUTUALLY EXCLUSIVE with EXPECTED_GATE_STEPS: every gate-carrying, non-excused
# member is in exactly one of the two arrays, so the two together are set-equal to
# that whole population and neither can absorb a member the other drops.
#
# Empty today, and that is the honest value: this workflow invokes make zero
# times. Empty is a CLAIM here, not a missing pin — the day a step becomes
# `make <target>`, the equality fails and names the target.
EXPECTED_MAKE_INVOKED_TARGETS=()

# A mapping that names nothing maps nothing — the same vacuity rule as the pin
# above (#lzvacuousrun).
if [ "${#EXPECTED_GATE_STEPS[@]}" -eq 0 ]; then
	echo "check-ci-reach: EXPECTED_GATE_STEPS is empty — a mapping that names no gate maps nothing" >&2
	exit 1
fi

map_targets=()
map_steps=()
for entry in "${EXPECTED_GATE_STEPS[@]}"; do
	case "$entry" in
	*'|'*) ;;
	*)
		echo "check-ci-reach: EXPECTED_GATE_STEPS entry '$entry' has no '|' — each entry is 'target|CI step name'" >&2
		exit 1
		;;
	esac
	mt="${entry%%|*}"
	ms="${entry#*|}"
	if [ -z "$mt" ] || [ -z "$ms" ]; then
		echo "check-ci-reach: EXPECTED_GATE_STEPS entry '$entry' has an empty target or step name" >&2
		exit 1
	fi
	# A step name containing '|' would split at the wrong place and silently pin a
	# prefix of the real name, which would then fail to match any step and read as
	# a missing gate. Refuse it instead of diagnosing the wrong thing.
	case "$ms" in
	*'|'*)
		echo "check-ci-reach: EXPECTED_GATE_STEPS entry '$entry' contains more than one '|'; a step name cannot contain '|'" >&2
		exit 1
		;;
	esac
	if ! grep -qxF "$mt" <<<"$pin_all"; then
		echo "check-ci-reach: EXPECTED_GATE_STEPS maps '$mt', which is not in EXPECTED_CLOSURE_TARGETS." >&2
		echo "  Only a target in the closure has a gate for a CI step to run." >&2
		exit 1
	fi
	if grep -qxF "$mt" <<<"$pin_nogate_static"; then
		echo "check-ci-reach: EXPECTED_GATE_STEPS maps '$mt', which is pinned as carrying NO gate." >&2
		echo "  A target with no checkable command is exempt from CI reach, so there is" >&2
		echo "  no gate for a step to run. Remove it from one pin or the other." >&2
		exit 1
	fi
	map_targets+=("$mt")
	map_steps+=("$ms")
done

mki_list="$(printf '%s\n' "${EXPECTED_MAKE_INVOKED_TARGETS[@]:+${EXPECTED_MAKE_INVOKED_TARGETS[@]}}")"
for t in "${EXPECTED_MAKE_INVOKED_TARGETS[@]:+${EXPECTED_MAKE_INVOKED_TARGETS[@]}}"; do
	if ! grep -qxF "$t" <<<"$pin_all"; then
		echo "check-ci-reach: EXPECTED_MAKE_INVOKED_TARGETS names '$t', which is not in EXPECTED_CLOSURE_TARGETS." >&2
		echo "  Only a target in the closure can be reached by CI at all." >&2
		exit 1
	fi
	if grep -qxF "$t" <<<"$pin_nogate_static"; then
		echo "check-ci-reach: EXPECTED_MAKE_INVOKED_TARGETS names '$t', which is pinned as carrying NO gate." >&2
		echo "  A target with no checkable command needs no reach of any kind." >&2
		exit 1
	fi
done

# MUTUAL EXCLUSION is the whole point of having two arrays rather than one array
# and its complement. A member in both would be checked in one mode and excused
# from the other, which is the state the combined edit above manufactured.
for t in "${EXPECTED_MAKE_INVOKED_TARGETS[@]:+${EXPECTED_MAKE_INVOKED_TARGETS[@]}}"; do
	if grep -qxF "$t" <<<"$(printf '%s\n' "${EXPECTED_GATE_STEPS[@]}" | sed 's/|.*$//')"; then
		echo "check-ci-reach: '$t' is in BOTH EXPECTED_MAKE_INVOKED_TARGETS and EXPECTED_GATE_STEPS." >&2
		echo "  A member is reached either by CI spelling its gate (a step pin) or by CI" >&2
		echo "  invoking make (no anchor check at all). Not both — pick the one CI does." >&2
		exit 1
	fi
done

mki_dupes="$(printf '%s\n' "${EXPECTED_MAKE_INVOKED_TARGETS[@]:+${EXPECTED_MAKE_INVOKED_TARGETS[@]}}" | grep -v '^$' | sort | uniq -d || true)"
if [ -n "$mki_dupes" ]; then
	echo "check-ci-reach: EXPECTED_MAKE_INVOKED_TARGETS repeats a target:" >&2
	while IFS= read -r d; do
		[ -n "$d" ] || continue
		echo "  - $d" >&2
	done <<<"$mki_dupes"
	exit 1
fi

map_dupes="$(printf '%s\n' "${EXPECTED_GATE_STEPS[@]}" | sort | uniq -d)"
if [ -n "$map_dupes" ]; then
	echo "check-ci-reach: EXPECTED_GATE_STEPS repeats an identical target|step entry:" >&2
	while IFS= read -r d; do
		[ -n "$d" ] || continue
		echo "  - $d" >&2
	done <<<"$map_dupes"
	echo "A duplicate is harmless to the checks below and a sign the list was edited by" >&2
	echo "appending rather than read — fix it there." >&2
	exit 1
fi

# ---------------------------------------------------------------- configuration

workflows=()
workflow_count=0
excused_targets=()
excused_reasons=()
excuse_count=0

if [ -f "$CONF" ]; then
	while IFS= read -r line || [ -n "$line" ]; do
		line="${line%%$'\r'}"
		case "$line" in
		'#'* | '') continue ;;
		esac
		key="${line%%:*}"
		val="${line#*:}"
		val="$(printf '%s' "$val" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
		case "$key" in
		workflow)
			workflows+=("$val")
			workflow_count=$((workflow_count + 1))
			;;
		excuse)
			tgt="${val%%[[:space:]]*}"
			reason="${val#"$tgt"}"
			reason="$(printf '%s' "$reason" | sed -e 's/^[[:space:]]*//')"
			if [ -z "$reason" ]; then
				echo "check-ci-reach: excuse for '$tgt' has no reason — an excuse without a reason is not an excuse" >&2
				exit 1
			fi
			excused_targets+=("$tgt")
			excused_reasons+=("$reason")
			excuse_count=$((excuse_count + 1))
			;;
		*)
			echo "check-ci-reach: unknown key '$key' in $CONF" >&2
			exit 1
			;;
		esac
	done <"$CONF"
fi

if [ "$workflow_count" -eq 0 ]; then
	workflows=(".github/workflows/ci.yml")
	workflow_count=1
fi

for wf in "${workflows[@]}"; do
	if [ ! -f "$wf" ]; then
		echo "check-ci-reach: workflow '$wf' listed in $CONF does not exist" >&2
		exit 1
	fi
done

# ------------------------------------------------------- make target extraction

# A Makefile may set .RECIPEPREFIX to something other than tab (lazily-rs uses
# `>`), which puts recipe lines at column 0 where a rule line lives. Without this
# a recipe such as `>cargo test --features a:b` reads as a rule named `>cargo`.
RECIPE_PREFIX="$(awk -F= '/^[[:space:]]*\.RECIPEPREFIX[[:space:]]*[:+]?=/ {
	v = $2; gsub(/^[[:space:]]+|[[:space:]]+$/, "", v); if (v != "") print substr(v, 1, 1); exit
}' Makefile)"

# Prerequisites of a target, straight from the Makefile source, with `\`
# continuations joined and trailing comments removed. Order-only prerequisites are
# dropped: they constrain ordering, not what runs.
prereqs_of() {
	awk -v target="$1" -v rp="$RECIPE_PREFIX" '
		BEGIN { pat = "^" target ":([^=]|$)"; if (rp == "") rp = "\t" }
		{
			line = $0
			# Only the ACTUAL recipe prefix marks a recipe line. Treating any
			# leading whitespace as one loses a rule that is merely indented,
			# which under a non-tab .RECIPEPREFIX is perfectly legal make and
			# collapses the whole closure to a single target. A continuation is
			# exempt: under the default tab prefix a wrapped prerequisite list is
			# normally tab-indented.
			if (!cont && substr(line, 1, 1) == rp) next
			sub(/^[[:space:]]+/, "", line)
			if (cont) {
				buf = buf " " line
				if (line ~ /\\[[:space:]]*$/) next
				cont = 0
				emit(buf)
				exit
			}
			if (line !~ pat) next
			buf = line
			if (line ~ /\\[[:space:]]*$/) { cont = 1; next }
			emit(buf)
			exit
		}
		function emit(s,   rest, n, i, parts) {
			gsub(/\\/, " ", s)
			sub(/#.*$/, "", s)
			rest = substr(s, index(s, ":") + 1)
			sub(/\|.*$/, "", rest)
			n = split(rest, parts, /[[:space:]]+/)
			for (i = 1; i <= n; i++) if (parts[i] != "") print parts[i]
		}
	' Makefile
}

# Is this name an explicit rule in the Makefile?
is_makefile_target() {
	awk -v target="$1" -v rp="$RECIPE_PREFIX" '
		BEGIN { pat = "^" target ":([^=]|$)"; if (rp == "") rp = "\t"; found = 0 }
		substr($0, 1, 1) == rp { next }
		{ line = $0; sub(/^[[:space:]]+/, "", line) }
		line ~ pat { found = 1; exit }
		END { exit found ? 0 : 1 }
	' Makefile
}

# Breadth-first closure of ROOT_TARGET's prerequisites, parents before children.
closure=""
queue="$ROOT_TARGET"
seen=" "
while [ -n "$queue" ]; do
	current="${queue%%$'\n'*}"
	if [ "$current" = "$queue" ]; then queue=""; else queue="${queue#*$'\n'}"; fi
	[ -n "$current" ] || continue
	case "$seen" in
	*" $current "*) continue ;;
	esac
	seen="$seen$current "
	closure="$closure$current"$'\n'
	while IFS= read -r dep; do
		[ -n "$dep" ] || continue
		if is_makefile_target "$dep"; then
			queue="$queue$dep"$'\n'
		fi
	done < <(prereqs_of "$current")
done

# `make -n` for a target emits its prerequisites' commands first, then its own.
# Asking make for the prerequisite list alone yields exactly that prefix — make
# applies the same de-duplication to both invocations — so removing it leaves the
# target's own recipe. Diagnostics make writes about targets it has nothing to do
# for are not commands and are dropped.
# A recipe line broken across physical lines with `\` reaches the shell as ONE
# command, and make -n prints it the way the Makefile spells it. Joining here is
# what keeps `VAR=x \` + `go test ./...` from being read as two commands, the
# second of which is where the whole gate lives.
join_continuations() {
	awk '
		{
			line = $0
			if (line ~ /\\[[:space:]]*$/) {
				sub(/\\[[:space:]]*$/, "", line)
				buf = buf line " "
				next
			}
			print buf line
			buf = ""
		}
		END { if (buf != "") print buf }
	'
}

dry_run() {
	"$MAKE_BIN" -n "$@" 2>/dev/null | grep -v -e '^make\[' -e '^make:' | join_continuations || true
}

own_commands() {
	local target="$1"
	local deps=()
	local dep_count=0
	while IFS= read -r dep; do
		[ -n "$dep" ] || continue
		if is_makefile_target "$dep"; then
			deps+=("$dep")
			dep_count=$((dep_count + 1))
		fi
	done < <(prereqs_of "$target")

	if [ "$dep_count" -eq 0 ]; then
		dry_run "$target"
		return
	fi
	local prefix
	prefix="$(dry_run "${deps[@]}" | wc -l)"
	dry_run "$target" | tail -n +"$((prefix + 1))"
}

# `make -n` must SUCCEED for every target in the closure (#lzgrepcpipefail).
#
# `dry_run` above reads a recipe through `make -n` with `2>/dev/null` on make and a
# trailing `|| true` on the pipeline. Both are individually right — a `grep -v`
# that filters out every line of make's chatter is a legitimate zero, and that is
# what the `|| true` is for — but together they also swallow make FAILING. make
# exits 2 on a prerequisite it has no rule for, writes its diagnosis to stderr, and
# produces an EMPTY stdout. Empty stdout is exactly what a recipe with nothing
# checkable in it produces, so the target lands in `nogate` and this guard reports
# OK having verified nothing whatsoever about it. The `|| true` is correct about
# the grep and still leaves make's own failure indistinguishable from a legitimate
# zero; those are two different questions sharing one exit status.
#
# MEASURED, not reasoned. Reasoning is what missed it: judged against the Makefile
# as written, no target has a file prerequisite, so no `make -n` can fail — which
# is a property of today's source, not a safety property, and this guard's whole
# job is to survive the commit that changes it. Adding a stamp file, a generated
# header or a vendored directory as a prerequisite is an ordinary commit that
# disarms the guard without touching the guard.
#
# Add `test: does-not-exist.stamp` to a byte-verified scratch copy of this repo's
# Makefile and, before this probe existed:
#
#   no gate  test                     recipe runs no checkable command
#   no gate  conformance-coverage     recipe runs no checkable command
#   check-ci-reach: OK — 7 target(s) reached by CI, 0 excused, 3 carrying no gate
#   exit 0
#
# Two gates, not one: `conformance-coverage` depends on `test`, so `make -n` fails
# for it too. The gate that proves the suite ran and the gate that audits the
# coverage ledger both went invisible to the reachability guard, and nothing went
# red.
#
# So make's status is checked HERE, in the MAIN shell, per target, before any
# verdict is printed. It cannot live inside `dry_run`: every caller runs that in a
# `$(...)` or a pipeline, where an `exit 1` kills only the subshell and the caller
# reads back the same empty string. Per target so the diagnostic names the one
# that dropped out, and make's stderr is carried VERBATIM because that message
# names the missing prerequisite, which is the whole diagnosis.
probe_failures=""
probe_count=0
while IFS= read -r target; do
	[ -n "$target" ] || continue
	# 2>&1 >/dev/null captures stderr ONLY: stderr is duplicated onto the
	# substitution's pipe first, then stdout is discarded. Order matters.
	if ! probe_err="$("$MAKE_BIN" -n "$target" 2>&1 >/dev/null)"; then
		probe_failures="$probe_failures  - $target"$'\n'
		while IFS= read -r eline; do
			[ -n "$eline" ] || continue
			probe_failures="$probe_failures      $eline"$'\n'
		done <<<"$probe_err"
		probe_count=$((probe_count + 1))
	fi
done <<<"$closure"

if [ "$probe_count" -gt 0 ]; then
	echo "check-ci-reach: '$MAKE_BIN -n' FAILED for $probe_count target(s) in '$ROOT_TARGET''s closure:" >&2
	printf '%s' "$probe_failures" >&2
	echo >&2
	echo "This guard reads every recipe through \`make -n\`. A target make cannot even" >&2
	echo "DRY-RUN yields an empty recipe, which is indistinguishable from a recipe that" >&2
	echo "carries no checkable command — so each target above would be reported as" >&2
	echo "'carrying no gate' and the verdict would be OK over a gate nobody checked." >&2
	echo "Fix the Makefile. Do not silence this by excusing the target: an excuse says" >&2
	echo "CI deliberately does not run a gate, not that the gate cannot be read." >&2
	exit 1
fi

# ------------------------------------------------ the make-derived oracle (A)

# Does `make -n $ROOT_TARGET` actually run each closure target's commands?
# (#pinreachclosure, part A — the load-bearing half of this pin.)
#
# `prereqs_of` derives the closure by awk-scanning Makefile SOURCE TEXT for the
# first line matching `^$ROOT_TARGET:` and stopping there. It never asks make, so
# it cannot see make conditionals — and that makes the source text and the
# executed set two different things:
#
#   ifeq ($(SKIP_SLOW),)
#   check: fmt-check vet build test race test-interop-peer ... ci-reach
#   else
#   check: fmt-check vet build test conformance-coverage ... ci-reach
#   endif
#
# Measured on a byte-verified scratch copy of this Makefile. With `SKIP_SLOW=1`,
# `make -n check` runs NEITHER `go test -race ./...` NOR the interop peer, and the
# guard's entire verdict — including a membership pin reporting `10 target(s) ...
# set-equal` — is byte-identical (`cmp -s`) to the healthy run. The awk closure is
# the same in both states, so a set-equality pin over it is the same in both
# states and passes the compromised one BY CONSTRUCTION. `ifeq (0,1)` as the first
# branch does it with no variable at all, which means a plain `make check` is the
# compromised state.
#
# So the closure is cross-examined against make. For each target, every command
# line make prints for it must appear verbatim in the root's own `make -n` output.
# Line-exact is sound here because both sides come from the same make with the
# same variable expansions, joined by the same `join_continuations`.
#
# `make -n`, never `make -p`: -p builds the default goal and dumps the entire
# environment to stdout, which would print every secret in a CI job's env into the
# log. Reach is still a floor, not equivalence — see WHAT IT DOES NOT PROVE.
root_cmds="$(dry_run "$ROOT_TARGET")"

oracle_mismatch=""
oracle_count=0
while IFS= read -r target; do
	[ -n "$target" ] || continue
	while IFS= read -r cmd; do
		[ -n "$cmd" ] || continue
		if ! grep -qxF "$cmd" <<<"$root_cmds"; then
			oracle_mismatch="$oracle_mismatch  - $target"$'\n'
			oracle_mismatch="$oracle_mismatch      not run by '$MAKE_BIN -n $ROOT_TARGET': $cmd"$'\n'
			oracle_count=$((oracle_count + 1))
			break
		fi
	done < <(dry_run "$target")
done <<<"$closure"

if [ "$oracle_count" -gt 0 ]; then
	echo "check-ci-reach: $oracle_count target(s) are in '$ROOT_TARGET''s awk-derived closure but '$MAKE_BIN -n $ROOT_TARGET' does not run their commands:" >&2
	printf '%s' "$oracle_mismatch" >&2
	echo >&2
	echo "The closure above is read from Makefile SOURCE TEXT — the first '^$ROOT_TARGET:'" >&2
	echo "line — and make conditionals make source text and executed set two different" >&2
	echo "things. A target listed in a branch make does not take is audited here, named in" >&2
	echo "the verdict as reached, and never run." >&2
	echo "  * Unintended: the prerequisite lives in a conditional branch that is not being" >&2
	echo "    taken. Move it out, or make the condition unconditional." >&2
	echo "  * Intended (a deliberately optional gate): that is an EXCUSE, with a reason, in" >&2
	echo "    $CONF — plus removal from EXPECTED_CLOSURE_TARGETS if it has left the closure" >&2
	echo "    for good. An optional gate must not be able to read as an enforced one." >&2
	exit 1
fi

# ------------------------------------------- closure membership, both directions

# Reported separately and BY NAME, because the two directions are different
# mistakes with different remedies (#pinreachclosure).
#
# `grep -qxF` against a here-string, never a pipe: a producer piped into `grep -q`
# inverts on a MATCH, because grep exits at the first hit, the producer takes
# SIGPIPE, and `pipefail` hands the pipeline that status. A here-string has no
# writer to kill.
pin_list="$(printf '%s\n' "${EXPECTED_CLOSURE_TARGETS[@]}")"

pin_missing=""
pin_missing_count=0
for want in "${EXPECTED_CLOSURE_TARGETS[@]}"; do
	if ! grep -qxF "$want" <<<"$closure"; then
		pin_missing="$pin_missing  - $want"$'\n'
		pin_missing_count=$((pin_missing_count + 1))
	fi
done

pin_extra=""
pin_extra_count=0
while IFS= read -r have; do
	[ -n "$have" ] || continue
	if ! grep -qxF "$have" <<<"$pin_list"; then
		pin_extra="$pin_extra  - $have"$'\n'
		pin_extra_count=$((pin_extra_count + 1))
	fi
done <<<"$closure"

if [ "$pin_missing_count" -gt 0 ] || [ "$pin_extra_count" -gt 0 ]; then
	if [ "$pin_missing_count" -gt 0 ]; then
		echo "check-ci-reach: $pin_missing_count pinned target(s) are NOT in '$ROOT_TARGET''s closure:" >&2
		printf '%s' "$pin_missing" >&2
		echo >&2
		echo "A pinned target that '$ROOT_TARGET' no longer reaches means the GATE STOPPED" >&2
		echo "RUNNING — it was deleted from a prerequisite list, or renamed. That is the" >&2
		echo "failure this pin exists for; without it the verdict below would have been OK" >&2
		echo "over a smaller closure, with the count silently one lower." >&2
		echo "  * If the gate should still run: restore the prerequisite. Do not edit the pin." >&2
		echo "  * If you are retiring the gate on purpose: delete its entry from" >&2
		echo "    EXPECTED_CLOSURE_TARGETS in this script, in the same commit, so the" >&2
		echo "    decision is reviewable instead of invisible." >&2
		echo "Those are not interchangeable. Reaching for the second one to make a red go" >&2
		echo "away is how the pin becomes decoration." >&2
	fi
	if [ "$pin_extra_count" -gt 0 ]; then
		[ "$pin_missing_count" -gt 0 ] && echo >&2
		echo "check-ci-reach: $pin_extra_count target(s) in '$ROOT_TARGET''s closure are NOT pinned:" >&2
		printf '%s' "$pin_extra" >&2
		echo >&2
		echo "A new gate is welcome; an UNPINNED one is not, because this pin is the only" >&2
		echo "thing that will notice the day it disappears again. Add it to" >&2
		echo "EXPECTED_CLOSURE_TARGETS above — sorted, with a short note on what it gates." >&2
	fi
	exit 1
fi

# An excuse naming a target OUTSIDE the closure, the mirror image of the pin and
# symmetric with the reverse check KNOWN_UNCOVERED already applies in
# scripts/check-conformance-coverage.sh ("lists 'X', which is not in the canonical
# corpus").
#
# Measured, on a scratch copy of scripts/ci-reach.conf verified byte-identical
# with `cmp` first, with two excuses appended — `bench-scale` (a real target in
# this Makefile that `check` does not run) and `totally-nonexistent-target`:
#
#   check-ci-reach: OK — 9 target(s) reached by CI, 0 excused, 1 carrying no gate
#   exit 0
#
# `0 excused`. Both were dropped on the floor without a word. So this is not a
# typo check: an excuse could name a LIVE gate that `check` does not run, read as
# accepted, and stand in the one file that is supposed to tell a reader what this
# binding does not enforce. Excuses were already verified in both directions
# against CI REACH; against the closure they claim membership of, they were
# verified in neither.
orphan_excuses=""
orphan_count=0
for i in "${!excused_targets[@]}"; do
	if ! grep -qxF "${excused_targets[$i]}" <<<"$closure"; then
		orphan_excuses="$orphan_excuses  - ${excused_targets[$i]}  (${excused_reasons[$i]})"$'\n'
		orphan_count=$((orphan_count + 1))
	fi
done

if [ "$orphan_count" -gt 0 ]; then
	echo "check-ci-reach: $orphan_count excuse(s) in $CONF name a target that is not in '$ROOT_TARGET''s closure:" >&2
	printf '%s' "$orphan_excuses" >&2
	echo >&2
	echo "An excuse says CI deliberately does not run a gate that 'make $ROOT_TARGET' DOES" >&2
	echo "run. A name outside the closure excuses nothing, and it reads as though a gate" >&2
	echo "is accounted for when it is not." >&2
	echo "  * Misspelled or renamed target: fix the name." >&2
	echo "  * The target really did leave '$ROOT_TARGET': delete the excuse — and check" >&2
	echo "    whether the prerequisite was supposed to leave, which the pin above asks" >&2
	echo "    about separately." >&2
	exit 1
fi

# ------------------------------------------------------------- workflow scraping

# Command lines from every `run:` step, each tagged with the NAME of the step it
# came from (#reversereachdirection). Comment lines inside a run body are
# stripped here — the whole reason this guard is a script.
#
# The step name is the only stable CI-side identity a gate can be pinned to. It
# survives the recipe gaining a flag, because the recipe and its step move
# together; it does not survive the gate being moved to a different step, which is
# the attack EXPECTED_GATE_STEPS closes.
#
# A new list item clears the name, so the previous step's name cannot leak into an
# unnamed one. That reset is applied AFTER the in-block branch below: a `- ` at
# the start of a line inside a run body is part of a command, not a step boundary.
# A run: step with no name is reported as UNNAMED and refused where the anchors
# are matched — it has no identity to pin, and adding a `name:` is not a
# behaviour change.
# The NAME of every run: step, one line per step, in file order. Separate from
# ci_commands because duplicate-name detection has to count step OCCURRENCES:
# two distinct steps sharing a name are exactly what must be refused, and any
# de-duplicated view of them is the one view that cannot see it.
ci_step_names() {
	awk '
		{
			line = $0
			indent = match(line, /[^ ]/) - 1
			if (indent < 0) indent = 9999

			if (inblock) {
				if (line ~ /^[[:space:]]*$/) next
				if (indent <= block_indent) inblock = 0
				else next
			}

			if (line ~ /^[[:space:]]*-[[:space:]]/) stepname = ""
			if (line ~ /^[[:space:]]*(-[[:space:]]+)?name:[[:space:]]*[^[:space:]]/) {
				nm = line
				sub(/^[[:space:]]*(-[[:space:]]+)?name:[[:space:]]*/, "", nm)
				sub(/[[:space:]]+$/, "", nm)
				if (nm ~ /^".*"$/ || nm ~ /^'"'"'.*'"'"'$/) nm = substr(nm, 2, length(nm) - 2)
				stepname = nm
				next
			}
			if (line ~ /^[[:space:]]*(-[[:space:]]+)?run:[[:space:]]*[|>][-+]?[[:space:]]*$/) {
				inblock = 1
				block_indent = indent
				print (stepname == "" ? "\002unnamed" : stepname)
				next
			}
			if (line ~ /^[[:space:]]*(-[[:space:]]+)?run:[[:space:]]*[^|>[:space:]]/) {
				print (stepname == "" ? "\002unnamed" : stepname)
			}
		}
	' "$@"
}

ci_commands() {
	awk '
		function out(c) { printf "%s\t%s\n", (stepname == "" ? "\002unnamed" : stepname), c }
		function flush() { if (buf != "") { out(buf); buf = "" } }
		{
			line = $0
			indent = match(line, /[^ ]/) - 1
			if (indent < 0) indent = 9999

			if (inblock) {
				if (line ~ /^[[:space:]]*$/) next
				if (indent <= block_indent) { flush(); inblock = 0 }
				else {
					sub(/^[[:space:]]+/, "", line)
					if (substr(line, 1, 1) == "#") next
					if (line ~ /\\[[:space:]]*$/) {
						sub(/\\[[:space:]]*$/, "", line)
						buf = buf " " line
						next
					}
					if (buf != "") { out(buf " " line); buf = "" } else out(line)
					next
				}
			}

			if (line ~ /^[[:space:]]*-[[:space:]]/) stepname = ""
			if (line ~ /^[[:space:]]*(-[[:space:]]+)?name:[[:space:]]*[^[:space:]]/) {
				nm = line
				sub(/^[[:space:]]*(-[[:space:]]+)?name:[[:space:]]*/, "", nm)
				sub(/[[:space:]]+$/, "", nm)
				# A YAML scalar may be quoted; the pin spells the name, not the
				# quoting, so strip a single matching pair.
				if (nm ~ /^".*"$/ || nm ~ /^'"'"'.*'"'"'$/) nm = substr(nm, 2, length(nm) - 2)
				stepname = nm
				next
			}
			if (line ~ /^[[:space:]]*(-[[:space:]]+)?run:[[:space:]]*[|>][-+]?[[:space:]]*$/) {
				inblock = 1
				block_indent = indent
				buf = ""
				next
			}
			if (line ~ /^[[:space:]]*(-[[:space:]]+)?run:[[:space:]]*[^|>[:space:]]/) {
				sub(/^[[:space:]]*(-[[:space:]]+)?run:[[:space:]]*/, "", line)
				out(line)
			}
		}
		END { flush() }
	' "$@"
}

# ------------------------------------------------------------------- normalizing

# Reduce command text to anchors, one per line, each a space-separated token list.
anchors() {
	awk '
		BEGIN {
			# Sentinel for an unresolvable variable reference. Deliberately not a
			# string any real argument can be.
			ANY = "\001any"
			split(": true false echo printf cd pushd popd mkdir rmdir rm cp mv ln touch " \
			      "export unset set local read eval exec trap wait sleep exit return " \
			      "if then else elif fi for while until do done case esac function " \
			      "test [ [[ pwd ls cat head tail sed awk grep egrep fgrep sort uniq " \
			      "wc tr cut paste tee xargs env dirname basename date git", t, / /)
			for (i in t) if (t[i] != "") trivial[t[i]] = 1
		}
		{
			n = split(split_unquoted($0), cmds, /\n/)
			for (i = 1; i <= n; i++) emit(cmds[i])
		}
		# Split on the shell'"'"'s sequencing operators, but ONLY outside quotes. Doing
		# this before quotes are stripped is what stops a `;` inside a message —
		# `echo "missing $(DIR); clone the sibling"` — from being read as a second
		# command and inventing an anchor for a gate that does not exist. That is a
		# false RED, so it costs a real target its verdict.
		function split_unquoted(s,   i, c, nxt, len, inq, q, out) {
			out = ""; inq = 0; q = ""; len = length(s)
			for (i = 1; i <= len; i++) {
				c = substr(s, i, 1)
				if (inq) {
					if (c == q) { inq = 0; q = "" }
					out = out c
					continue
				}
				if (c == "\"" || c == "'"'"'" || c == "`") { inq = 1; q = c; out = out c; continue }
				nxt = substr(s, i + 1, 1)
				if (c == ";") { out = out "\n"; continue }
				if ((c == "&" && nxt == "&") || (c == "|" && nxt == "|")) { out = out "\n"; i++; continue }
				if (c == "|") { out = out "\n"; continue }
				out = out c
			}
			return out
		}
		function emit(cmd,   m, j, tok, out, prog, started, parts) {
			gsub(/[`"'"'"']/, " ", cmd)
			gsub(/\$\(/, " ", cmd)
			gsub(/\$\{/, " ", cmd)
			gsub(/[(){}]/, " ", cmd)
			m = split(cmd, parts, /[[:space:]]+/)
			prog = ""
			out = ""
			started = 0
			for (j = 1; j <= m; j++) {
				tok = parts[j]
				if (tok == "" || tok == "\\") continue
				if (tok ~ /^[0-9]*>>?$/ || tok == "<" || tok ~ /^[0-9]+>&[0-9]+$/) break
				if (!started) {
					if (tok ~ /^[A-Za-z_][A-Za-z0-9_]*=/) continue
					started = 1
					prog = tok
					sub(/.*\//, "", prog)
					if (prog == "" || (prog in trivial)) return
					out = prog
					continue
				}
				if (tok ~ /^-/) {
					sub(/=.*$/, "", tok)
					out = out " " tok
					continue
				}
				if (tok ~ /^\.{1,3}$/ || tok ~ /^\.{1,2}\/\.{0,3}$/) continue
				if (tok ~ /\//) {
					sub(/\/+$/, "", tok)
					sub(/.*\//, "", tok)
					if (tok == "" || tok ~ /^\.{1,3}$/) continue
				}
					# A token that is still a shell/make VARIABLE reference names a
					# value this guard cannot resolve — a CI step spelling a path as
					# "$LAZILY_CONFORMANCE_MANIFEST" and a Makefile recipe spelling the
					# same path through an expanded $(VAR) are the same command. Dropping
					# it (what this used to do) loses the ARGUMENT as well as its value,
					# so `script.sh <path>` no longer matched a CI step that really ran
					# `script.sh "$PATH"` and the target was reported unreachable. That is
					# a false RED, and it cost lazily-cpp a hardcoded second spelling of
					# the path plus a hand-written equality assertion to keep the two in
					# sync — a new drift surface invented to satisfy a guard that exists
					# to detect drift.
					#
					# Emit a WILDCARD instead: one token that matches one token, so arity
					# is preserved. `script.sh $A` still fails against a CI step that
					# passes no argument at all. This is the same looseness the normalizer
					# already applies to paths, which it reduces to basenames — reach is a
					# floor, not equivalence, exactly as the header says.
					if (substr(tok, 1, 1) == "$") { out = out " " ANY; continue }
				out = out " " tok
			}
			if (started && out != "") print out
		}
	'
}

# --------------------------------------------------------------------- matching

ci_raw="$(mktemp)"
ci_anchor="$(mktemp)"
ci_step_anchor="$(mktemp)"
trap 'rm -f "$ci_raw" "$ci_anchor" "$ci_step_anchor"' EXIT
ci_commands "${workflows[@]}" >"$ci_raw"

# Anchors, STEP-QUALIFIED: one `<step name>\t<anchor>` line each. The flat
# `$ci_anchor` is derived from this rather than scraped separately, so the two
# cannot drift into disagreeing about what CI runs — the flat one is still what
# `make_invokes` and the EXCUSE staleness check consult, both of which ask
# "anywhere in CI" and are right to.
while IFS=$'\t' read -r step_name step_cmd; do
	[ -n "$step_cmd" ] || continue
	while IFS= read -r a; do
		[ -n "$a" ] || continue
		printf '%s\t%s\n' "$step_name" "$a"
	done < <(printf '%s\n' "$step_cmd" | anchors)
done <"$ci_raw" | sort -u >"$ci_step_anchor"

cut -f2- <"$ci_step_anchor" | sort -u >"$ci_anchor"

if [ ! -s "$ci_anchor" ]; then
	echo "check-ci-reach: no run: steps found in ${workflows[*]} — a guard with an empty haystack passes everything" >&2
	exit 1
fi

# An UNNAMED run: step carrying a gate has no CI-side identity to pin, so a gate
# could be moved into it and no mapping would notice (#reversereachdirection).
# Refused rather than tolerated: adding a `name:` is not a behaviour change. Only
# steps that contribute an ANCHOR matter — a `mkdir`-only step carries no gate, so
# an unnamed one hides nothing.
if cut -f1 <"$ci_step_anchor" | grep -qxF "$UNNAMED_STEP"; then
	echo "check-ci-reach: ${workflows[*]} has a run: step with no 'name:' that runs a real command:" >&2
	grep -F "$UNNAMED_STEP$(printf '\t')" <"$ci_step_anchor" | cut -f2- | while IFS= read -r a; do
		echo "  - $a" >&2
	done
	echo >&2
	echo "Gates are pinned to STEP NAMES (EXPECTED_GATE_STEPS), so an unnamed step is a" >&2
	echo "place a gate can be moved to without any mapping noticing. Give it a 'name:'." >&2
	exit 1
fi

# Step names must be UNIQUE across the workflows that count, or "inside that step"
# names two places and the mapping stops meaning one of them. Measured across the
# family: names are NOT globally unique everywhere (rs has 69 run: steps and 65
# distinct names), so this is asserted rather than assumed. This workflow has 13
# run: steps and 13 distinct names.
#
# Counted over step OCCURRENCES, from `ci_step_names`, and never over
# `$ci_step_anchor`: that file is `sort -u`ed, so two distinct steps sharing a
# name and a command collapse to one line there and a duplicate would be
# invisible to the very check meant to find it.
# The unnamed sentinel is filtered out: two unnamed steps are not a name
# collision, and the check above already refuses an unnamed step that carries a
# gate. Reporting the sentinel here would print a control character at the reader
# instead of a diagnosis.
step_name_dupes="$(ci_step_names "${workflows[@]}" | grep -vxF "$UNNAMED_STEP" | sort | uniq -d || true)"
if [ -n "$step_name_dupes" ]; then
	echo "check-ci-reach: ${workflows[*]} uses the same step name more than once:" >&2
	while IFS= read -r d; do
		[ -n "$d" ] || continue
		echo "  - $d" >&2
	done <<<"$step_name_dupes"
	echo >&2
	echo "EXPECTED_GATE_STEPS pins a gate to a step NAME, so a duplicated name pins two" >&2
	echo "steps at once and the gate could live in either. Rename one, or qualify the" >&2
	echo "mapping by job before relying on it." >&2
	exit 1
fi

# A pinned step name that names no step in the workflow is a different mistake
# from a gate that is missing from its step, and it gets its own diagnosis: the
# reach check below would otherwise report every one of that gate's anchors as
# absent and send the reader looking at the recipe instead of at the pin.
# Reported against the ANCHOR-BEARING steps: pinning a gate to a step that runs
# only `mkdir` would satisfy a name check and never satisfy reach.
ci_gate_step_names="$(cut -f1 <"$ci_step_anchor" | sort -u)"
map_step_unknown=""
map_step_unknown_count=0
for i in "${!map_steps[@]}"; do
	if ! grep -qxF "${map_steps[$i]}" <<<"$ci_gate_step_names"; then
		map_step_unknown="$map_step_unknown  - ${map_targets[$i]} -> ${map_steps[$i]}"$'\n'
		map_step_unknown_count=$((map_step_unknown_count + 1))
	fi
done

if [ "$map_step_unknown_count" -gt 0 ]; then
	echo "check-ci-reach: $map_step_unknown_count entr(ies) in EXPECTED_GATE_STEPS name a step that ${workflows[*]} does not have (or that runs no checkable command):" >&2
	printf '%s' "$map_step_unknown" >&2
	echo >&2
	echo "The step names that DO carry a gate in that workflow are:" >&2
	while IFS= read -r n; do
		[ -n "$n" ] || continue
		echo "  * $n" >&2
	done <<<"$ci_gate_step_names"
	echo >&2
	echo "  * A step was renamed: update the mapping to the new name, in the same commit." >&2
	echo "  * A step was deleted: the gate has left CI. That is the failure this guard" >&2
	echo "    exists for — add the step back, or excuse the target in $CONF with a" >&2
	echo "    reason and drop its mapping entry." >&2
	exit 1
fi

# Does CI contain a command whose tokens contain this anchor as an in-order
# subsequence? Extra flags and arguments on the CI side are fine; missing ones are
# not.
#
# $2 is the file of candidate anchors, so the same subsequence matcher serves both
# questions: "anywhere in CI" (the flat set) and "inside this gate's own pinned
# step" (#reversereachdirection). They are different questions and the scoped one
# is the verdict. The flat one is asked in exactly two places below, neither of
# which can credit a gate: for an EXCUSED target, where "does CI reach this at
# all" is precisely what the staleness check means, and in the MISSING diagnostic,
# to tell the reader whether SOME other step runs the gate — which is what
# separates "the gate left CI" from "the gate, or the recipe, moved".
anchor_reached_in() {
	awk -v want="$1" '
		BEGIN { ANY = "\001any"; wn = split(want, w, / /) }
		{
			hn = split($0, h, / /)
			wi = 1
			# A wildcard on EITHER side matches, because either side may be the
			# one that spelled the argument through a variable.
			for (hi = 1; hi <= hn && wi <= wn; hi++)
				if (h[hi] == w[wi] || h[hi] == ANY || w[wi] == ANY) wi++
			if (wi > wn) { found = 1; exit }
		}
		END { exit found ? 0 : 1 }
	' "$2"
}

anchor_reached() { anchor_reached_in "$1" "$ci_anchor"; }

# The anchors belonging to ONE named step, written to $2. Empty when the name
# matches no step in the workflow, which the caller reports as a mapping that
# names a step CI does not have.
step_anchors_into() {
	awk -F'\t' -v want="$1" '$1 == want { print $2 }' "$ci_step_anchor" >"$2"
}

# Which step(s) is this gate pinned to? Empty output means unpinned.
steps_for_target() {
	local t="$1" i
	for i in "${!map_targets[@]}"; do
		[ "${map_targets[$i]}" = "$t" ] && printf '%s\n' "${map_steps[$i]}"
	done
	return 0
}

# CI invoking the target through make counts as reach without any anchor work.
make_invokes() {
	awk -v target="$1" '
		{
			n = split($0, t, / /)
			if (t[1] != "make") next
			for (i = 2; i <= n; i++) if (t[i] == target) { found = 1; exit }
		}
		END { exit found ? 0 : 1 }
	' "$ci_anchor"
}

is_excused() {
	local t="$1" i
	for i in "${!excused_targets[@]}"; do
		[ "${excused_targets[$i]}" = "$t" ] && return 0
	done
	return 1
}

excuse_reason() {
	local t="$1" i
	for i in "${!excused_targets[@]}"; do
		if [ "${excused_targets[$i]}" = "$t" ]; then
			printf '%s' "${excused_reasons[$i]}"
			return
		fi
	done
}

unreached=""
unreached_count=0
stale=""
stale_count=0
nogate=""
nogate_count=0
unpinned=""
unpinned_count=0
mode_lost=""
mode_lost_count=0
mki_seen=""
mki_ok=0
reached=0
excused_ok=0
mapped=0
step_scope_file="$(mktemp)"
one_step_file="$(mktemp)"
trap 'rm -f "$ci_raw" "$ci_anchor" "$ci_step_anchor" "$step_scope_file" "$one_step_file"' EXIT
# Which targets the mapping is required to cover, filled in as the loop
# classifies each one. Compared against EXPECTED_GATE_STEPS after the loop.
need_map=""

while IFS= read -r target; do
	[ -n "$target" ] || continue

	target_anchors="$(own_commands "$target" | anchors | sort -u || true)"

	if [ -z "$target_anchors" ]; then
		nogate="$nogate$target"$'\n'
		nogate_count=$((nogate_count + 1))
		continue
	fi

	# A target CI invokes as `make <target>` is reached without any anchor work,
	# and it has no independent CI-side spelling of the gate — CI's instruction is
	# "run the target", so it faithfully runs whatever the recipe says. There is
	# nothing for a step pin to cross-examine, so such a target is excluded from
	# EXPECTED_GATE_STEPS rather than mapped to whichever step runs make
	# (#reversereachdirection). This workflow invokes make zero times, so today
	# this branch is never taken; it exists so that the day a `make <target>` step
	# appears, the mapping requirement DROPS that target by name instead of going
	# quietly stale around it. Measured: replacing the `race (cgo)` step's body
	# with `make race` keeps `reached  race` and refuses its mapping entry —
	# "1 entr(ies) in EXPECTED_GATE_STEPS map a target that does not need a step:
	# - race". The remedy is to delete the entry AND add the target to
	# EXPECTED_MAKE_INVOKED_TARGETS; pointing the entry at whichever step runs
	# make would assert nothing.
	#
	# That refusal is NOT on its own what pins the mode. Deleting the entry in the
	# same edit cancels it — see EXPECTED_MAKE_INVOKED_TARGETS above for the
	# measured exit-0 verdict and why the mode needs its own set-equality. Same
	# calibration as lazily-cs, which maps 8 of its 10 members and refuses a pin
	# for the one reached through `make package-check`.
	if make_invokes "$target"; then
		if is_excused "$target"; then
			stale="$stale$target"$'\n'
			stale_count=$((stale_count + 1))
		else
			mki_seen="$mki_seen$target"$'\n'
			reached=$((reached + 1))
			mki_ok=$((mki_ok + 1))
			printf 'reached  %s\n' "$target"
		fi
		continue
	fi

	# An EXCUSED target is asked the flat question — "does CI reach this gate at
	# ALL" — because that is exactly what the staleness check means, and an
	# excused gate has no pinned step by construction.
	if is_excused "$target"; then
		hit=1
		while IFS= read -r a; do
			[ -n "$a" ] || continue
			anchor_reached "$a" || hit=0
		done <<<"$target_anchors"
		if [ "$hit" -eq 1 ]; then
			stale="$stale$target"$'\n'
			stale_count=$((stale_count + 1))
		else
			excused_ok=$((excused_ok + 1))
			printf 'excused  %-32s %s\n' "$target" "$(excuse_reason "$target")"
		fi
		continue
	fi

	# THE MODE RUNG RUNS FIRST, ahead of the unpinned rung below. A member pinned
	# as make-invoked that CI no longer invokes through make has had its STEP
	# deleted or rewritten; telling the reader to add a gate-step pin would send
	# them to fix the pin instead of the step, which is the wrong repair for a
	# deleted step. lazily-cpp hit exactly that ordering and had to reverse it.
	if grep -qxF "$target" <<<"$mki_list"; then
		mode_lost="$mode_lost$target"$'\n'
		mode_lost_count=$((mode_lost_count + 1))
		printf 'MODE     %-32s pinned as make-invoked, but no CI step runs `%s %s`\n' "$target" "$MAKE_BIN" "$target"
		continue
	fi

	# A real gate. Reach is asked INSIDE its pinned step(s) and nowhere else
	# (#reversereachdirection): "some command somewhere in the workflow" is what
	# let a recipe be repointed at another step's command for free.
	need_map="$need_map$target"$'\n'
	target_steps="$(steps_for_target "$target")"
	if [ -z "$target_steps" ]; then
		unpinned="$unpinned$target"$'\n'
		unpinned_count=$((unpinned_count + 1))
		printf 'UNPINNED %s\n' "$target"
		continue
	fi

	# The union of the pinned steps' anchors. A union, because a target whose
	# anchors legitimately span two steps is pinned to both; it is still strictly
	# narrower than the flat set, which is every step at once.
	: >"$step_scope_file"
	while IFS= read -r sname; do
		[ -n "$sname" ] || continue
		step_anchors_into "$sname" "$one_step_file"
		cat "$one_step_file" >>"$step_scope_file"
	done <<<"$target_steps"

	hit=1
	missing_anchors=""
	while IFS= read -r a; do
		[ -n "$a" ] || continue
		if ! anchor_reached_in "$a" "$step_scope_file"; then
			hit=0
			missing_anchors="$missing_anchors$a"$'\n'
		fi
	done <<<"$target_anchors"

	if [ "$hit" -eq 1 ]; then
		reached=$((reached + 1))
		mapped=$((mapped + 1))
		printf 'reached  %s\n' "$target"
	else
		unreached="$unreached$target"$'\n'
		unreached_count=$((unreached_count + 1))
		printf 'MISSING  %s\n' "$target"
		while IFS= read -r a; do
			[ -n "$a" ] || continue
			while IFS= read -r sname; do
				[ -n "$sname" ] || continue
				printf '           step `%s` does not match `%s`\n' "$sname" "$a"
			done <<<"$target_steps"
			if anchor_reached "$a"; then
				printf '           (another CI step DOES run it — a gate moved, or a recipe repointed)\n'
			fi
		done <<<"$missing_anchors"
	fi
done <<<"$closure"

while IFS= read -r target; do
	[ -n "$target" ] || continue
	printf 'no gate  %-32s recipe runs no checkable command\n' "$target"
done <<<"$nogate"

# ------------------------------------------------ the classification pin (C)

# `no gate` is an EXEMPTION, so which targets hold it is pinned by set equality
# against EXPECTED_NOGATE_TARGETS (#pinreachclosure, part C). See that
# declaration for the measured pre-fix verdict: emptying a recipe moved a target
# into the exemption at exit 0 with its name and its membership untouched.
nogate_extra=""
nogate_extra_count=0
nogate_gone=""
nogate_gone_count=0
nogate_seen="$(printf '%s' "$nogate")"
pin_nogate="$(printf '%s\n' "${EXPECTED_NOGATE_TARGETS[@]:+${EXPECTED_NOGATE_TARGETS[@]}}")"

while IFS= read -r t; do
	[ -n "$t" ] || continue
	if ! grep -qxF "$t" <<<"$pin_nogate"; then
		nogate_extra="$nogate_extra  - $t"$'\n'
		nogate_extra_count=$((nogate_extra_count + 1))
	fi
done <<<"$nogate_seen"

for t in "${EXPECTED_NOGATE_TARGETS[@]:+${EXPECTED_NOGATE_TARGETS[@]}}"; do
	if ! grep -qxF "$t" <<<"$nogate_seen"; then
		nogate_gone="$nogate_gone  - $t"$'\n'
		nogate_gone_count=$((nogate_gone_count + 1))
	fi
done

classification_bad=0
if [ "$nogate_extra_count" -gt 0 ]; then
	classification_bad=1
	echo >&2
	echo "check-ci-reach: $nogate_extra_count target(s) carry no gate but are not pinned as exempt:" >&2
	printf '%s' "$nogate_extra" >&2
	echo "A target reported 'no gate' is EXEMPT from having to appear in CI. Emptying a" >&2
	echo "recipe therefore retires a gate without touching a prerequisite list or a name," >&2
	echo "which is why the exempt set is pinned rather than merely counted." >&2
	echo "  * If the gate should still run: restore the recipe." >&2
	echo "  * If the target really is a no-op step now: add it to EXPECTED_NOGATE_TARGETS" >&2
	echo "    with a note on why it cannot hide a failure." >&2
fi
if [ "$nogate_gone_count" -gt 0 ]; then
	classification_bad=1
	echo >&2
	echo "check-ci-reach: $nogate_gone_count target(s) pinned as exempt now carry a gate:" >&2
	printf '%s' "$nogate_gone" >&2
	echo "That is an improvement, and the pin is stale. Delete the entry from" >&2
	echo "EXPECTED_NOGATE_TARGETS so the gate is held to CI reach like every other." >&2
	echo "Leaving it there would exempt a real gate the day the two disagree again." >&2
fi

# ------------------------------------------- the gate-to-step mapping, both ways

# The mapping must cover EXACTLY the gates that need it (#reversereachdirection):
# every closure member that carries a gate, is not excused, and is not reached by
# a bare `make <target>` step. Both directions, reported separately, because they
# are different mistakes:
#
#   * A gate with NO entry falls back to nothing — there is no flat path left for
#     it to be credited through, so it is already RED above as UNPINNED. Named
#     again here with the remedy.
#   * An entry for a target that no longer needs one — retired, excused, or now
#     invoked as `make <target>` — is a mapping that has gone stale around the
#     thing it was written for. Deleting it is the deliberate edit; leaving it is
#     how a mapping becomes decoration.
map_pin_list="$(printf '%s\n' "${map_targets[@]}" | sort -u)"

map_missing=""
map_missing_count=0
while IFS= read -r t; do
	[ -n "$t" ] || continue
	if ! grep -qxF "$t" <<<"$map_pin_list"; then
		map_missing="$map_missing  - $t"$'\n'
		map_missing_count=$((map_missing_count + 1))
	fi
done <<<"$need_map"

map_extra=""
map_extra_count=0
while IFS= read -r t; do
	[ -n "$t" ] || continue
	if ! grep -qxF "$t" <<<"$need_map"; then
		map_extra="$map_extra  - $t"$'\n'
		map_extra_count=$((map_extra_count + 1))
	fi
done <<<"$map_pin_list"

# The mode pin, SET-EQUAL in both directions (#reversereachdirection). Reported
# separately from the mapping because the remedies differ, and reported BY NAME
# because "one fewer" is the diagnosis that failed.
mode_new=""
mode_new_count=0
while IFS= read -r t; do
	[ -n "$t" ] || continue
	if ! grep -qxF "$t" <<<"$mki_list"; then
		mode_new="$mode_new  - $t"$'\n'
		mode_new_count=$((mode_new_count + 1))
	fi
done <<<"$mki_seen"

mode_bad=0
if [ "$mode_new_count" -gt 0 ]; then
	mode_bad=1
	echo >&2
	echo "check-ci-reach: $mode_new_count member(s) are now reached by CI invoking make, and are not pinned as such:" >&2
	printf '%s' "$mode_new" >&2
	echo "A member in make mode gets NO anchor check: CI runs the target, so whatever the" >&2
	echo "recipe says is what runs, in both places. Switching a step to 'make <target>'" >&2
	echo "therefore turns off the only thing that cross-examines that recipe." >&2
	echo "  * Deliberate: add the target to EXPECTED_MAKE_INVOKED_TARGETS and delete its" >&2
	echo "    EXPECTED_GATE_STEPS entry, in the same commit. Both, so the mode change is a" >&2
	echo "    reviewable statement rather than a count going down by one." >&2
	echo "  * Not deliberate: restore the step's own spelling of the gate." >&2
fi
if [ "$mode_lost_count" -gt 0 ]; then
	mode_bad=1
	echo >&2
	echo "check-ci-reach: $mode_lost_count member(s) pinned as make-invoked that no CI step invokes through make:" >&2
	while IFS= read -r t; do
		[ -n "$t" ] || continue
		echo "  - $t" >&2
	done <<<"$mode_lost"
	echo "The STEP changed, not the pin. Its 'make $ROOT_TARGET'-style invocation was" >&2
	echo "deleted or rewritten, so nothing in CI reaches this gate at all now." >&2
	echo "  * Restore the step, or give the gate its own step and move the target from" >&2
	echo "    EXPECTED_MAKE_INVOKED_TARGETS to EXPECTED_GATE_STEPS with that step's name." >&2
	echo "  * Do NOT reach for a gate-step pin to clear this: the missing thing is the" >&2
	echo "    step." >&2
fi

mapping_bad=0
if [ "$map_missing_count" -gt 0 ]; then
	mapping_bad=1
	echo >&2
	echo "check-ci-reach: $map_missing_count gate(s) run by 'make $ROOT_TARGET' have no entry in EXPECTED_GATE_STEPS:" >&2
	printf '%s' "$map_missing" >&2
	echo "Reach is checked INSIDE the CI step pinned for each gate, so an unmapped gate" >&2
	echo "is a gate nothing checks. Add 'target|CI step name' to EXPECTED_GATE_STEPS," >&2
	echo "sorted, naming the step that really runs it." >&2
fi
if [ "$map_extra_count" -gt 0 ]; then
	mapping_bad=1
	echo >&2
	echo "check-ci-reach: $map_extra_count entr(ies) in EXPECTED_GATE_STEPS map a target that does not need a step:" >&2
	printf '%s' "$map_extra" >&2
	echo "A target is mapped only while it carries a gate, is not excused, and is not" >&2
	echo "invoked by CI as 'make <target>'. If one of those changed, delete the entry in" >&2
	echo "the same commit as the change, so the decision is reviewable. If none of them" >&2
	echo "changed, the target left the closure — which the membership pin above asks" >&2
	echo "about separately, and is the more serious finding." >&2
fi

# A guard that examined nothing must not report OK — the same vacuity rule the
# conformance guards apply (#lzvacuousrun).
if [ "$((reached + excused_ok + unreached_count))" -eq 0 ]; then
	echo "check-ci-reach: '$ROOT_TARGET' has no prerequisite target carrying a gate — nothing was verified" >&2
	exit 1
fi

status=0
if [ "$classification_bad" -eq 1 ]; then
	status=1
fi
if [ "$mapping_bad" -eq 1 ]; then
	status=1
fi
if [ "$mode_bad" -eq 1 ]; then
	status=1
fi
if [ "$unpinned_count" -gt 0 ]; then
	status=1
fi
if [ "$stale_count" -gt 0 ]; then
	echo >&2
	while IFS= read -r t; do
		[ -n "$t" ] || continue
		echo "check-ci-reach: '$t' is excused in $CONF but CI DOES reach it — remove the excuse" >&2
	done <<<"$stale"
	status=1
fi

if [ "$unreached_count" -gt 0 ]; then
	echo >&2
	echo "check-ci-reach: $unreached_count target(s) run by 'make $ROOT_TARGET' that their own CI step does not reach:" >&2
	while IFS= read -r t; do
		[ -n "$t" ] || continue
		echo "  - $t" >&2
	done <<<"$unreached"
	echo >&2
	echo "Each is checked against the step EXPECTED_GATE_STEPS pins for it, not against" >&2
	echo "the workflow as a whole. So this is one of three things, and they are not" >&2
	echo "interchangeable:" >&2
	echo "  * The gate is genuinely absent from CI: add a step that runs it, or excuse" >&2
	echo "    the target with a reason in $CONF." >&2
	echo "  * The gate moved to a DIFFERENT step: repoint the mapping entry at that" >&2
	echo "    step. The '(another CI step DOES run it)' note above says when this is" >&2
	echo "    the case." >&2
	echo "  * The RECIPE was repointed at something CI already runs, so the gate no" >&2
	echo "    longer runs anywhere. That is #reversereachdirection's attack; fix the" >&2
	echo "    Makefile, and do NOT move the mapping to follow it." >&2
	status=1
fi

if [ "$status" -eq 0 ]; then
	echo "check-ci-reach: closure pinned — ${#EXPECTED_CLOSURE_TARGETS[@]} target(s) under root '$EXPECTED_ROOT_TARGET', set-equal, all run by '$MAKE_BIN -n $EXPECTED_ROOT_TARGET'; ${#EXPECTED_NOGATE_TARGETS[@]} pinned exempt"
	echo "check-ci-reach: step-mapped — $mapped gate(s) matched INSIDE the CI step pinned for each, ${#EXPECTED_GATE_STEPS[@]} mapping entr(ies), set-equal; $mki_ok reached by make invocation, ${#EXPECTED_MAKE_INVOKED_TARGETS[@]} pinned as such, set-equal"
	echo "check-ci-reach: OK — $reached target(s) reached by CI, $excused_ok excused, $nogate_count carrying no gate"
fi
exit "$status"
