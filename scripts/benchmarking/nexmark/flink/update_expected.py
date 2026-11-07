#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Rewrites the expected results of a systest file from Flink reference results.

A query opts in with a comment line before its SQL:

    # reference: q13 --checksum --string-columns 4

The first word names the reference result (<ref-dir>/q13.csv from run_reference.sh); the remaining words are passed
to to_nes_rows.py. The block between the query's `----` line and the next blank line is replaced.

Usage: update_expected.py <test-file> <ref-dir> [--output <file>]
"""

import argparse
import shlex
import subprocess
import sys
from pathlib import Path

CONVERTER = Path(__file__).with_name("to_nes_rows.py")


def expected_rows(reference_dir: Path, spec: str) -> list[str]:
    query, *options = shlex.split(spec)
    output = subprocess.check_output([sys.executable, "-I", str(CONVERTER), str(reference_dir / f"{query}.csv"), *options], text=True)
    return output.splitlines()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("test_file", type=Path)
    parser.add_argument("reference_dir", type=Path)
    parser.add_argument("--output", type=Path, help="write here instead of rewriting the test file in place")
    args = parser.parse_args()

    lines = args.test_file.read_text().splitlines()
    result: list[str] = []
    pending_spec = None
    index = 0
    updated = 0
    while index < len(lines):
        line = lines[index]
        result.append(line)
        index += 1
        if line.startswith("# reference:"):
            pending_spec = line.removeprefix("# reference:").strip()
        elif line == "----" and pending_spec is not None:
            result.extend(expected_rows(args.reference_dir, pending_spec))
            while index < len(lines) and lines[index].strip():
                index += 1
            pending_spec = None
            updated += 1
    (args.output or args.test_file).write_text("\n".join(result) + "\n")
    print(f"updated {updated} queries", file=sys.stderr)


if __name__ == "__main__":
    main()
