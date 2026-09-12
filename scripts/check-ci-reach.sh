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
#   For every target in `check`'s prerequisite closure, at least one CI `run:`
#   step invokes the same program with the same distinguishing flags.
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
#   its anchors is a subsequence of some CI command's token list, or when CI runs
#   `make <target>` directly. Every, not any: a target that runs two gates and is
#   half-covered by CI is a gap, and "any" would report it green.
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
# accident: the attack's whole advantage was being invisible in a diff of one
# line, and the pin turns it into a two-place edit whose second place is a
# reviewable statement of intent.
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

# Command lines from every `run:` step. Comment lines inside a run body are
# stripped here — the whole reason this guard is a script.
ci_commands() {
	awk '
		function flush() { if (buf != "") { print buf; buf = "" } }
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
					if (buf != "") { print buf " " line; buf = "" } else print line
					next
				}
			}

			if (line ~ /^[[:space:]]*(-[[:space:]]+)?run:[[:space:]]*[|>][-+]?[[:space:]]*$/) {
				inblock = 1
				block_indent = indent
				buf = ""
				next
			}
			if (line ~ /^[[:space:]]*(-[[:space:]]+)?run:[[:space:]]*[^|>[:space:]]/) {
				sub(/^[[:space:]]*(-[[:space:]]+)?run:[[:space:]]*/, "", line)
				print line
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
trap 'rm -f "$ci_raw" "$ci_anchor"' EXIT
ci_commands "${workflows[@]}" >"$ci_raw"
anchors <"$ci_raw" | sort -u >"$ci_anchor"

if [ ! -s "$ci_anchor" ]; then
	echo "check-ci-reach: no run: steps found in ${workflows[*]} — a guard with an empty haystack passes everything" >&2
	exit 1
fi

# Does CI contain a command whose tokens contain this anchor as an in-order
# subsequence? Extra flags and arguments on the CI side are fine; missing ones are
# not.
anchor_reached() {
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
	' "$ci_anchor"
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
reached=0
excused_ok=0

while IFS= read -r target; do
	[ -n "$target" ] || continue

	target_anchors="$(own_commands "$target" | anchors | sort -u || true)"

	if [ -z "$target_anchors" ]; then
		nogate="$nogate$target"$'\n'
		nogate_count=$((nogate_count + 1))
		continue
	fi

	hit=1
	missing_anchors=""
	if ! make_invokes "$target"; then
		while IFS= read -r a; do
			[ -n "$a" ] || continue
			if ! anchor_reached "$a"; then
				hit=0
				missing_anchors="$missing_anchors$a"$'\n'
			fi
		done <<<"$target_anchors"
	fi

	if is_excused "$target"; then
		if [ "$hit" -eq 1 ]; then
			stale="$stale$target"$'\n'
			stale_count=$((stale_count + 1))
		else
			excused_ok=$((excused_ok + 1))
			printf 'excused  %-32s %s\n' "$target" "$(excuse_reason "$target")"
		fi
		continue
	fi

	if [ "$hit" -eq 1 ]; then
		reached=$((reached + 1))
		printf 'reached  %s\n' "$target"
	else
		unreached="$unreached$target"$'\n'
		unreached_count=$((unreached_count + 1))
		printf 'MISSING  %s\n' "$target"
		while IFS= read -r a; do
			[ -n "$a" ] || continue
			printf '           no CI run: step matches `%s`\n' "$a"
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
	echo "check-ci-reach: $unreached_count target(s) run by 'make $ROOT_TARGET' that no CI run: step reaches:" >&2
	while IFS= read -r t; do
		[ -n "$t" ] || continue
		echo "  - $t" >&2
	done <<<"$unreached"
	echo >&2
	echo "Add a CI step that runs it, or add an excuse with a reason to $CONF." >&2
	status=1
fi

if [ "$status" -eq 0 ]; then
	echo "check-ci-reach: closure pinned — ${#EXPECTED_CLOSURE_TARGETS[@]} target(s) under root '$EXPECTED_ROOT_TARGET', set-equal, all run by '$MAKE_BIN -n $EXPECTED_ROOT_TARGET'; ${#EXPECTED_NOGATE_TARGETS[@]} pinned exempt"
	echo "check-ci-reach: OK — $reached target(s) reached by CI, $excused_ok excused, $nogate_count carrying no gate"
fi
exit "$status"
