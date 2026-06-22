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
End-to-end check that the StatisticOptimizationRule fires when a query is deployed to a *real* NebulaStream
instance.

Unlike check_statistic_optimization_rule.py (which drives the rule directly from a gtest), this script starts the
actual embedded single-node engine (`nes-repl-embedded`), deploys a genuine user query through the real frontend
optimizer, and verifies the rule's print appears in the instance's output during query creation.

Flow:
  * `nes-repl-embedded` is wired (in the EMBED_ENGINE build) with a StatisticCoordinator + StatisticRetrievalService
    so the optimizer's StatisticOptimizationRule has a service to query.
  * We pipe a small SQL script (create a generator source, then a SELECT) into the REPL on stdin.
  * Optimizing the SELECT runs the StatisticOptimizationRule, which fetches the (deterministic, mock) statistic and
    prints:  StatisticOptimizationRule: retrieved statistic value 42; returning plan unmodified.
  * We assert that line — with the expected value 42 — appears in stdout.

The value is deterministic: the PoC build query bakes the constant 42 into a generator constant-sequence source, so
there is no randomness in the result.

Usage:
    scripts/check_statistic_optimization_rule_e2e.py [--repl-binary PATH] [--expected-value 42] [--timeout 180]

Exit code 0 on success, non-zero otherwise.
"""
import argparse
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_REPL_BINARY = REPO_ROOT / "build_dir" / "nes-frontend" / "apps" / "nes-repl-embedded"

# A minimal deployable query for the embedded engine: a bounded generator source feeding a Void sink. Deploying the
# SELECT triggers optimization (and therefore the StatisticOptimizationRule). MAX_RUNTIME_MS keeps the run short.
QUERY_SCRIPT = """\
CREATE LOGICAL SOURCE endless(val_1 UINT32, ts UINT64);
CREATE PHYSICAL SOURCE FOR endless TYPE Generator SET(
    'ALL' as "SOURCE".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES,
    'CSV' as INPUT_FORMATTER."TYPE",
    3000 as "SOURCE".MAX_RUNTIME_MS,
    'SEQUENCE UINT32 0 10000000 1, SEQUENCE UINT64 0 10000000 1' AS "SOURCE".GENERATOR_SCHEMA);
SELECT TS FROM ENDLESS INTO Void('localhost:8080' AS "SINK"."HOST");
"""

PRINT_RE = re.compile(
    r"StatisticOptimizationRule: retrieved statistic value (\d+(?:\.\d+)?); returning plan unmodified\."
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--repl-binary",
        type=Path,
        default=DEFAULT_REPL_BINARY,
        help=f"Path to the nes-repl-embedded binary (default: {DEFAULT_REPL_BINARY})",
    )
    parser.add_argument(
        "--expected-value",
        type=float,
        default=42.0,
        help="Deterministic statistic value the rule must print (default: 42).",
    )
    parser.add_argument("--timeout", type=int, default=180, help="Seconds to wait for the REPL to finish (default: 180).")
    args = parser.parse_args()

    if not args.repl_binary.is_file():
        print(f"ERROR: nes-repl-embedded binary not found: {args.repl_binary}", file=sys.stderr)
        print("Build it first, e.g.: cmake --build build_dir --target nes-repl-embedded", file=sys.stderr)
        return 2

    # --enable-statistics gives the embedded worker a statistic store and wires the StatisticOptimizationRule.
    # --on-exit STOP_QUERIES tears the deployed query down cleanly before the process exits.
    cmd = [str(args.repl_binary), "--enable-statistics", "--on-exit", "STOP_QUERIES"]
    print(f"Starting NebulaStream instance: {' '.join(cmd)}")
    try:
        completed = subprocess.run(
            cmd,
            input=QUERY_SCRIPT,
            capture_output=True,
            text=True,
            timeout=args.timeout,
            cwd=str(REPO_ROOT),
        )
    except subprocess.TimeoutExpired:
        print(f"ERROR: REPL did not finish within {args.timeout}s.", file=sys.stderr)
        return 1

    output = completed.stdout + completed.stderr

    if completed.returncode != 0:
        print(f"ERROR: nes-repl-embedded exited with code {completed.returncode}.", file=sys.stderr)
        print("----- captured output -----", file=sys.stderr)
        print(output, file=sys.stderr)
        return 1

    match = PRINT_RE.search(output)
    if not match:
        print("ERROR: StatisticOptimizationRule print not observed during query deployment.", file=sys.stderr)
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

    print(
        f"OK: deployed a query to a live NebulaStream instance and observed the StatisticOptimizationRule "
        f"print the expected statistic value {printed_value:g}."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
