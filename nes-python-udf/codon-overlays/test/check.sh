#!/bin/bash
# Checks the overlay merge: compiles udf/o1.py against lib/ with the overlays in ov/ and compares the output with
# expected.txt. Usage: check.sh BUILD_DIR (e.g. cmake-build-relwithdebinfo-libstdcxx-none)
set -e
HERE=$(cd "$(dirname "$0")" && pwd)
OUT=$(mktemp)
NES_CODON_PYTHON_PATH=$HERE/lib NES_CODON_PYTHON_OVERLAYS=$HERE/ov "$1/nes-python-udf/benchmark/runner/nes-python-udf-runner" \
  --compiler Codon --entrypoint o1.udf --import-path "$HERE/udf" --returns VARSIZED --input "$HERE/rows.tsv" --rounds 0 \
  --arg column:0:VARSIZED --output "$OUT" > /dev/null
diff "$OUT" "$HERE/expected.txt" && echo "overlay merge: ok"
rm -f "$OUT"
