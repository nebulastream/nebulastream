#!/bin/bash
# Creates the native inputs of run_experiment.py: udf/ids_<name>.bin with the int64 values 0..n-1 in little endian, and
# udf/wide_<k>.bin with rows of id and k more int64 fields, below the data
# directory (default /dev/shm/nes-udf-perf-eval, a RAM disk, so they are gone after a restart). The memory of the files is on
# NUMA node 0, where the worker threads of the layouts run.
set -euo pipefail
data=${1:-/dev/shm/nes-udf-perf-eval}
mkdir -p "$data/udf"
generator=$(mktemp)
trap 'rm -f "$generator"' EXIT
gcc -O2 -x c -o "$generator" - <<'C'
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
int main(int argc, char** argv) {
    /* <rows> <file> [fields]: each row has the fields id, id + 1, ..., id + fields - 1 */
    uint64_t n = strtoull(argv[1], 0, 10);
    FILE* out = fopen(argv[2], "wb");
    uint64_t fields = argc > 3 ? strtoull(argv[3], 0, 10) : 1;
    static int64_t block[1 << 16];
    if (argc < 3 || !out || fields == 0 || fields > 64) return 1;
    const size_t rows = (1 << 16) / fields;
    for (uint64_t i = 0; i < n;) {
        size_t count = n - i < rows ? n - i : rows;
        for (size_t k = 0; k < count; ++k)
            for (uint64_t f = 0; f < fields; ++f) block[k * fields + f] = (int64_t)(i + k + f);
        if (fwrite(block, 8 * fields, count, out) != count) return 1;
        i += count;
    }
    return fclose(out) != 0;
}
C
for spec in 60000000:60m 480000000:480m 1920000000:1920m; do
    numactl --membind=0 "$generator" "${spec%:*}" "$data/udf/ids_${spec#*:}.bin"
done
# Wide rows for --wide of run_experiment.py: id and 3, 7, 15 or 31 more INT64 fields that no UDF reads. The NATIVE format needs a
# row size that divides the buffer size, so the rows have 32, 64, 128 and 256 bytes.
for spec in 3:480000000 7:240000000 15:120000000 31:60000000; do
    numactl --membind=0 "$generator" "${spec#*:}" "$data/udf/wide_${spec%:*}.bin" $(( ${spec%:*} + 1 ))
done
ls -la "$data/udf"
