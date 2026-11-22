"""Summarizes codon_diff.py results of several targets on the same cases: per case group (builtins, repros, programs),
how many cases each target runs equal to CPython (ok), runs with differences (diff), rejects, crashes on or times out.
With --cases, it also prints one line per case with the status of each target.

Usage: compare_targets.py RESULTS.jsonl [...] [--cases]
"""

import argparse
import collections
import json
import os

STATUSES = ["ok", "diff", "reject", "crash", "timeout", "reference-error", "harness-error"]


def group(case):
    parts = case.split("/")
    if "repros" in parts:
        return "repros"
    if "programs" in parts:
        return "programs"
    return "builtins"


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("results", nargs="+")
    parser.add_argument("--cases", action="store_true")
    arguments = parser.parse_args()
    by_target = {}
    for path in arguments.results:
        records = {}
        for line in open(path, encoding="utf-8"):
            record = json.loads(line)
            # codon_diff.py before 9c65037f3b called a reject codon-reject.
            record["status"] = "reject" if record["status"] == "codon-reject" else record["status"]
            records[os.path.basename(record["case"])] = record
        target = next(iter(records.values())).get("compiler", os.path.splitext(os.path.basename(path))[0])
        by_target[target] = records

    targets = list(by_target)
    print("| Group | Target | " + " | ".join(STATUSES[:5]) + " |")
    print("|---|---|" + "---|" * 5)
    for name in ["builtins", "repros", "programs"]:
        for target in targets:
            counts = collections.Counter(r["status"] for r in by_target[target].values() if group(r["case"]) == name)
            if counts:
                print(f"| {name} | {target} | " + " | ".join(str(counts.get(s, 0)) for s in STATUSES[:5]) + " |")

    if arguments.cases:
        cases = sorted(set().union(*by_target.values()))
        print("\ncase " + " ".join(f"{t:10}" for t in targets))
        for case in cases:
            print(case[:48].ljust(48), " ".join(f"{by_target[t].get(case, {}).get('status', '-'):10}" for t in targets))


if __name__ == "__main__":
    main()
