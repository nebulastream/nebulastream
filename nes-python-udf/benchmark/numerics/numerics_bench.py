"""Compares Codon's C numerics with Python numerics (PYTHON_COMPAT, --python-compat 1 of the runner) on the arithmetic kernels of kernels.py.

Runs each kernel with nes-python-udf-runner in both modes on the same seeded input (a: INT64 in [-1e6, 1e6], b: INT64 in
[1, 1000], f: FLOAT64 in [-1e6, 1e6]) and appends the runner's JSON line per kernel and mode to results/<experiment>.jsonl.
Each line says whether the results equal those of the other mode. Needs a clean working tree.

Usage: numerics_bench.py --experiment NAME --scratch DIR [--rows N] [--rounds R] [--build DIR]
"""

import argparse
import json
import os
import random
import subprocess

HERE = os.path.dirname(os.path.abspath(__file__))
REPOSITORY = os.path.abspath(os.path.join(HERE, "../../.."))
KERNELS = {"int_floordiv_mod": ("a,b", "INT64"), "int_add_mul": ("a,b", "INT64"), "float_div": ("f,b", "FLOAT64"),
           "float_floordiv_mod": ("f,b", "FLOAT64"), "float_pow_int": ("f,b", "FLOAT64"),
           "float_list_contains": ("f,b", "FLOAT64")}
COLUMNS = {"a": (0, "INT64"), "b": (1, "INT64"), "f": (2, "FLOAT64")}


def git(*arguments):
    return subprocess.run(["git", "-C", REPOSITORY, *arguments], capture_output=True, text=True, check=True).stdout.strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--scratch", required=True)
    parser.add_argument("--rows", type=int, default=200000)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--build", default=os.path.join(REPOSITORY, "cmake-build-relwithdebinfo-libstdcxx-none"))
    arguments = parser.parse_args()
    if git("status", "--porcelain", "--untracked-files=no"):
        raise SystemExit("There are uncommitted changes. Commit them first, so that the recorded commit says what was measured.")
    commit = git("rev-parse", "HEAD")
    runner = os.path.join(arguments.build, "nes-python-udf/benchmark/runner/nes-python-udf-runner")
    os.makedirs(arguments.scratch, exist_ok=True)

    data = os.path.join(arguments.scratch, "numerics-input.tsv")
    rng = random.Random(83)
    with open(data, "w") as file:
        for _ in range(arguments.rows):
            file.write(f"{rng.randint(-10**6, 10**6)}\t{rng.randint(1, 1000)}\t{rng.uniform(-1e6, 1e6)!r}\n")

    results = os.path.join(HERE, "../results", arguments.experiment + ".jsonl")
    for kernel, (columns, returns) in KERNELS.items():
        outputs = {}
        for mode in ("c", "py"):
            output = os.path.join(arguments.scratch, f"{kernel}-{mode}.out")
            command = [runner, "--compiler", "Codon", "--entrypoint", f"kernels.{kernel}", "--import-path", HERE,
                       "--returns", returns, "--input", data, "--rounds", str(arguments.rounds), "--output", output,
                "--python-compat", "1" if mode == "py" else "0"]
            for column in columns.split(","):
                index, kind = COLUMNS[column]
                command += ["--arg", f"column:{index}:{kind}"]
            completed = subprocess.run(command, capture_output=True, text=True)
            lines = [line for line in completed.stdout.splitlines() if line.startswith("{")]
            record = json.loads(lines[-1]) if lines else {"error": completed.stderr.strip()[-500:]}
            outputs[mode] = output
            record = {"experiment": arguments.experiment, "commit": commit, "kernel": kernel, "numerics": mode,
                      "return_code": completed.returncode, "command": command, **record}
            if len(outputs) == 2:
                first, second = (open(path, "rb").read() for path in outputs.values())
                record["same_results_as_c"] = first == second
            with open(results, "a") as file:
                file.write(json.dumps(record) + "\n")
            print(f"{kernel:20} {mode:3} best {record.get('best_ns_per_row', float('nan')):10.1f} ns/row  "
                  f"median {record.get('median_ns_per_row', float('nan')):10.1f}  rc {completed.returncode}", flush=True)


if __name__ == "__main__":
    main()
