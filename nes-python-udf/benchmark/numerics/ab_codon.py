"""A/B of two nes-python-udf-runner builds (new, old) on the kernels of kernels.py in both PYTHON_COMPAT modes, interleaved, 3
repetitions; prints the median best ns per row. E87: old = Codon fork 94ba688, new = 3b8c3a9, same NES commit.

Usage (from the repository root): ab_codon.py NEW_RUNNER OLD_RUNNER INPUT.tsv OUTPUT
"""
import json, subprocess, sys, statistics, os
new, old, data, out = sys.argv[1:5]
K = {"int_floordiv_mod": "a,b", "int_add_mul": "a,b", "float_div": "f,b", "float_floordiv_mod": "f,b", "float_pow_int": "f,b", "float_list_contains": "f,b"}
C = {"a": "0:INT64", "b": "1:INT64", "f": "2:FLOAT64"}
R = {"int_floordiv_mod": "INT64", "int_add_mul": "INT64"}
res = {}
for rep in range(3):
    for k, cols in K.items():
        for mode in ("0", "1"):
            for name, runner in (("old", old), ("new", new)):
                cmd = [runner, "--compiler", "Codon", "--entrypoint", f"kernels.{k}", "--import-path", "nes-python-udf/benchmark/numerics",
                       "--returns", R.get(k, "FLOAT64"), "--input", data, "--rounds", "5", "--output", out, "--python-compat", mode]
                for c in cols.split(","): cmd += ["--arg", "column:" + C[c]]
                p = subprocess.run(cmd, capture_output=True, text=True)
                line = [l for l in p.stdout.splitlines() if l.startswith("{")]
                res.setdefault((k, mode, name), []).append(json.loads(line[-1])["best_ns_per_row"] if line else float("nan"))
for k in K:
    for mode in ("0", "1"):
        o = statistics.median(res[(k, mode, "old")]); n = statistics.median(res[(k, mode, "new")])
        print(f"{k:20} compat={mode} old {o:8.1f} new {n:8.1f} new/old {n/o:5.2f}")
