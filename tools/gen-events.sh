#!/usr/bin/env bash
# schemas/*.avdl  ->  schemas/gen/*.avsc (one per named type, registered in Karapace)
#                 ->  pkg/events/*_gen.go (Go types via hamba/avro avrogen)
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/tools/versions.env"
JAR="$ROOT/.tools/avro-tools-$AVRO_TOOLS_VERSION.jar"
export PATH="$ROOT/.tools/bin:$ROOT/.tools/go/bin:$PATH" GOTOOLCHAIN=local GOPATH="$ROOT/.tools/gopath"

rm -rf "$ROOT/schemas/gen" && mkdir -p "$ROOT/schemas/gen"
for idl in "$ROOT"/schemas/*.avdl; do
  name="$(basename "$idl" .avdl)"
  java -jar "$JAR" idl2schemata "$idl" "$ROOT/schemas/gen/" 2>/dev/null
  # Topic value types are the records documented with "topic: ..."; their .avsc files are
  # self-contained, so every generated schema string parses on its own.
  mapfile -t roots < <(java -jar "$JAR" idl "$idl" /dev/stdout 2>/dev/null \
    | jq -r '.types[] | select((.doc // "") | startswith("topic:")) | .name')
  files=(); for r in "${roots[@]}"; do files+=("$ROOT/schemas/gen/$r.avsc"); done
  out="$ROOT/pkg/events/${name}_gen.go"
  avrogen -pkg events -o "$out" -tags json:camel -encoders -enums -strict-types \
    -pkgdoc "Package events contains Go types generated from schemas/*.avdl. DO NOT EDIT." \
    "${files[@]}"
  # avrogen emits enum declarations once per reference; keep the first declaration of each.
  python3 - "$out" <<'PY'
import re, sys
path = sys.argv[1]
src = open(path).read()
block = re.compile(r'type (\w+) string\n\nconst \(\n(?:\t.*\n)*?\)\n\n?')
seen = set()
def keep_first(m):
    if m.group(1) in seen:
        return ""
    seen.add(m.group(1))
    return m.group(0)
open(path, "w").write(block.sub(keep_first, src))
PY
  "$ROOT/.tools/go/bin/gofmt" -w "$out"
done
echo "generated: $(ls "$ROOT"/schemas/gen/*.avsc | wc -l) schemas, $(ls "$ROOT"/pkg/events/*_gen.go | wc -l) Go files"
