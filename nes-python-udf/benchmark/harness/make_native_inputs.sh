#!/bin/bash
# Creates the native inputs of run_experiment.py: udf/ids_<name>.bin with the int64 values 0..n-1 in little endian, below the data
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
    uint64_t n = strtoull(argv[1], 0, 10);
    FILE* out = fopen(argv[2], "wb");
    static int64_t block[1 << 16];
    if (argc < 3 || !out) return 1;
    for (uint64_t i = 0; i < n;) {
        size_t count = n - i < (1 << 16) ? n - i : (1 << 16);
        for (size_t k = 0; k < count; ++k) block[k] = (int64_t)(i + k);
        if (fwrite(block, 8, count, out) != count) return 1;
        i += count;
    }
    return fclose(out) != 0;
}
C
for spec in 60000000:60m 480000000:480m 1920000000:1920m; do
    numactl --membind=0 "$generator" "${spec%:*}" "$data/udf/ids_${spec#*:}.bin"
done
ls -la "$data/udf"
