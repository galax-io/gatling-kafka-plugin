#!/usr/bin/env bash
#
# check-kotlin-examples.sh — compile src/test/kotlin/ against the built plugin.
#
# The sbt build handles Scala and Java only, deliberately: wiring a Kotlin toolchain into it for four
# example files was considered and rejected (specs/006-v2-cleanup-sweep, clarification Q3). The cost of
# that decision is that nothing catches drift in these files automatically — every one of them had
# rotted into a non-compiling state by 2.0.0, one of them against an API removed in 1.0.0.
#
# This is the compensating measure: an opt-in check, run by hand, that compiles the examples exactly as
# a Kotlin user would. It adds no build dependency — the compiler is fetched into the coursier cache on
# first use and nothing in the build references it.
#
# Run it whenever a published entry point changes, and before a release.
#
# Requires: coursier (`cs`), a JDK, and a prior `sbt Test/compile`.
# Env: KOTLIN_VERSION overrides the compiler version (default below).
set -euo pipefail

KOTLIN_VERSION="${KOTLIN_VERSION:-2.0.21}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KT_DIR="$ROOT/src/test/kotlin"
OUT="$(mktemp -d)"
trap 'rm -rf "$OUT"' EXIT

command -v cs >/dev/null || { echo "check-kotlin-examples: coursier (cs) not found" >&2; exit 2; }

echo "check-kotlin-examples: resolving Kotlin $KOTLIN_VERSION"
KOTLINC_CP="$(cs fetch --classpath "org.jetbrains.kotlin:kotlin-compiler:$KOTLIN_VERSION")"
KOTLIN_STDLIB="$(cs fetch --classpath "org.jetbrains.kotlin:kotlin-stdlib:$KOTLIN_VERSION")"

echo "check-kotlin-examples: exporting the project's test classpath"
PROJECT_CP="$(cd "$ROOT" && sbt -batch -error "export Test/fullClasspath" | tail -1)"
[ -n "$PROJECT_CP" ] || { echo "check-kotlin-examples: empty classpath — run 'sbt Test/compile' first" >&2; exit 1; }

# Collected with read, not `mapfile`: that is a bash 4 builtin and macOS ships bash 3.2 as /bin/bash,
# so `env bash` finds a shell without it unless a newer one happens to be first on PATH.
SOURCES=()
while IFS= read -r f; do SOURCES+=("$f"); done < <(find "$KT_DIR" -name '*.kt' | sort)
[ ${#SOURCES[@]} -gt 0 ] || { echo "check-kotlin-examples: no .kt sources under $KT_DIR" >&2; exit 1; }
echo "check-kotlin-examples: compiling ${#SOURCES[@]} file(s)"

java -cp "$KOTLINC_CP" org.jetbrains.kotlin.cli.jvm.K2JVMCompiler \
  -no-stdlib -jvm-target 17 \
  -cp "$PROJECT_CP:$KOTLIN_STDLIB" \
  -d "$OUT" \
  "${SOURCES[@]}"

echo "check-kotlin-examples: OK — every Kotlin example compiles against the current API"
