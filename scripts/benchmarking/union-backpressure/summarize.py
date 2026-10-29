#!/usr/bin/env python3

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Prints the median time per scenario, thread count and variant of a results.tsv written by run.sh.

Usage: summarize.py <results.tsv>
"""

import statistics
import sys
from collections import defaultdict


def main():
    times = defaultdict(list)
    failures = defaultdict(int)
    variants = []
    rows = []
    with open(sys.argv[1]) as file:
        for line in file:
            scenario, threads, variant, _, seconds = line.split()
            if variant not in variants:
                variants.append(variant)
            if (scenario, threads) not in rows:
                rows.append((scenario, threads))
            if seconds == "FAIL":
                failures[(scenario, threads, variant)] += 1
            else:
                times[(scenario, threads, variant)].append(float(seconds))

    print(f"{'scenario':<16}{'threads':>8}" + "".join(f"{variant:>16}" for variant in variants))
    for scenario, threads in rows:
        cells = []
        for variant in variants:
            key = (scenario, threads, variant)
            cell = f"{statistics.median(times[key]):.2f}s" if times[key] else ""
            if failures[key]:
                cell += f" {failures[key]}x FAIL"
            cells.append(f"{cell:>16}")
        print(f"{scenario:<16}{threads:>8}" + "".join(cells))


if __name__ == "__main__":
    main()
