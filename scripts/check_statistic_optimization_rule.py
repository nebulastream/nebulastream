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

"""
Runs the StatisticOptimizationRule end-to-end test and verifies that the rule prints the expected
statistic value while leaving the plan unmodified.

The PoC dataflow is fully deterministic: the build query bakes the constant value 42 into a generator
"constant sequence" source (no randomness), the store writer persists it, and the probe reads it back.
The StatisticOptimizationRule fetches that value through the StatisticRetrievalService and prints:

    StatisticOptimizationRule: retrieved statistic value 42; returning plan unmodified.

This script asserts that exact line (and the value 42) appears in the test output, so the print can be
checked in CI without re-reading the C++ assertions.

Usage:
    scripts/check_statistic_optimization_rule.py [--test-binary PATH] [--expected-value 42]

Exit code 0 on success, non-zero on any failure.
"""
import argparse
import re
import subprocess
import sys
from pathlib import Path

# Default location of the test binary relative to the repository root (parent of scripts/).
REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_TEST_BINARY = REPO_ROOT / "build_dir" / "nes-systests" / "systest" / "tests" / "statistic-coordinator-e2e-test"

GTEST_FILTER = "StatisticCoordinatorE2ETest.OptimizationRulePrintsStatisticAndLeavesPlanUnmodified"

# The rule prints this line (see nes-query-optimizer/src/Rules/Static/StatisticOptimizationRule.cpp).
PRINT_RE = re.compile(
    r"StatisticOptimizationRule: retrieved statistic value (\d+(?:\.\d+)?); returning plan unmodified\."
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--test-binary",
        type=Path,
        default=DEFAULT_TEST_BINARY,
        help=f"Path to the statistic-coordinator-e2e-test binary (default: {DEFAULT_TEST_BINARY})",
    )
    parser.add_argument(
        "--expected-value",
        type=float,
        default=42.0,
        help="Deterministic statistic value the rule must print (default: 42).",
    )
    args = parser.parse_args()

    if not args.test_binary.is_file():
        print(f"ERROR: test binary not found: {args.test_binary}", file=sys.stderr)
        print("Build it first, e.g.: cmake --build build_dir --target statistic-coordinator-e2e-test", file=sys.stderr)
        return 2

    cmd = [str(args.test_binary), f"--gtest_filter={GTEST_FILTER}"]
    print(f"Running: {' '.join(cmd)}")
    completed = subprocess.run(cmd, capture_output=True, text=True)
    output = completed.stdout + completed.stderr

    if completed.returncode != 0:
        print("ERROR: test binary exited non-zero (test failed).", file=sys.stderr)
        print(output, file=sys.stderr)
        return 1

    match = PRINT_RE.search(output)
    if not match:
        print("ERROR: expected StatisticOptimizationRule print line not found in test output.", file=sys.stderr)
        print(f"Expected a line matching: {PRINT_RE.pattern}", file=sys.stderr)
        print("----- captured output -----", file=sys.stderr)
        print(output, file=sys.stderr)
        return 1

    printed_value = float(match.group(1))
    if printed_value != args.expected_value:
        print(
            f"ERROR: rule printed statistic value {printed_value}, expected {args.expected_value}.",
            file=sys.stderr,
        )
        return 1

    print(f"OK: StatisticOptimizationRule printed the expected statistic value {printed_value:g}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
