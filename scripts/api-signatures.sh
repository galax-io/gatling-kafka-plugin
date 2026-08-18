#!/usr/bin/env bash
#
# api-signatures.sh <jar> — normalised public API listing, one "<class> | <member>" per line.
#
# Diff two of these to get the break surface between releases:
#
#   cs fetch --classpath org.galaxio:gatling-kafka-plugin_2.13:1.3.0 | tr ':' '\n' | grep gatling-kafka
#   scripts/api-signatures.sh <old.jar> > old.txt
#   sbt package && scripts/api-signatures.sh target/scala-2.13/gatling-kafka-plugin_2.13-*.jar > new.txt
#   diff old.txt new.txt
#
# Anonymous and lambda classes are filtered; the compiler-generated \ members that remain are
# deliberate — they move with the method that produced them and are useful evidence of what changed.
#
# This is a release-time tool, not a build gate. The permanent guard is MiMa (issue #217).
set -euo pipefail
JAR="$1"
CLASSES=()
while IFS= read -r c; do CLASSES+=("$c"); done < <(
  unzip -Z1 "$JAR" '*.class' \
    | grep -v '\$anon\$\|\$anonfun\$\|\$lambda\$' \
    | sed 's|/|.|g; s|\.class$||' \
    | sort
)
[ ${#CLASSES[@]} -gt 0 ] || { echo "api-signatures: no classes in $JAR" >&2; exit 1; }

# One javap for the whole jar, not one per class: javap accepts many class names, and starting a JVM
# per class turned a seconds-long job into a minutes-long one. The declaring class is recovered from
# the type-declaration line javap emits before each disassembly — the token right after the
# class/interface/enum keyword, not the last one on the line, which would pick up `extends` targets.
javap -public -cp "$JAR" "${CLASSES[@]}" 2>/dev/null \
  | awk '
      /^Compiled from/ { next }
      /^}$/            { next }
      /^$/             { next }
      /^[^ \t]/ {
        for (i = 1; i <= NF; i++)
          if ($i == "class" || $i == "interface" || $i == "enum") { cls = $(i + 1); break }
        sub(/<.*$/, "", cls)
      }
      {
        if (cls == "") next
        member = $0
        sub(/^ /, "", member)   # one leading space, matching the per-class form this replaced
        sub(/ $/, "", member)
        print cls " | " member
      }
    ' \
  | sort -u
