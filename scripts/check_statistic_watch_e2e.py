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
End-to-end check for the statistic *watch* flow against a real NebulaStream instance.

A query carrying GET_STATISTICS' sibling option WATCH_STATISTIC=TRUE makes the engine deploy a single combined
"watch" query (NO separate probe):

    Generator -> StatisticStoreWriter -> StatisticStoreReader -> FileSink -> FIFO -> coordinator

The writer puts the value into the store and forwards the record as an *impulse*; the reader reads the value back
out of the store and reports it. Each report fires a condition-trigger callback that prints:

    StatisticWatch: source <S> received data; statistic value 42

Because the generator emits continuously, the callback fires repeatedly, so we observe the print *periodically*.
This script deploys such a query, lets it run a few seconds, and asserts the print appears multiple times.

Usage:
    scripts/check_statistic_watch_e2e.py [--build-dir DIR] [--skip-build] [--repl-binary PATH]
                                         [--min-prints 3] [--timeout 120]

Exit code 0 on success, non-zero otherwise.
"""
import argparse
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BUILD_DIR = REPO_ROOT / "build_dir"
BUILD_TARGET = "nes-repl-embedded"
REPL_BINARY_SUBPATH = Path("nes-frontend") / "apps" / "nes-repl-embedded"

# A bounded user query carrying WATCH_STATISTIC=TRUE. The user query terminates after MAX_RUNTIME_MS; the separately
# deployed watch query keeps emitting impulses (and thus prints) until the instance stops queries on exit.
QUERY_SCRIPT = """\
CREATE LOGICAL SOURCE watched(val_1 UINT32, ts UINT64);
CREATE PHYSICAL SOURCE FOR watched TYPE Generator SET(
    'ALL' as "SOURCE".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES,
    'CSV' as INPUT_FORMATTER."TYPE",
    6000 as "SOURCE".MAX_RUNTIME_MS,
    'SEQUENCE UINT32 0 10000000 1, SEQUENCE UINT64 0 10000000 1' AS "SOURCE".GENERATOR_SCHEMA);
SELECT TS FROM WATCHED INTO Void('localhost:8080' AS "SINK"."HOST") SET(TRUE AS "QUERY"."WATCH_STATISTIC");
"""

PRINT_RE = re.compile(r"StatisticWatch: source \S+ received data; statistic value (\d+(?:\.\d+)?)")


def build_repl(build_dir: Path) -> int:
    cmd = ["cmake", "--build", str(build_dir), "--target", BUILD_TARGET, "-j"]
    print(f"Building: {' '.join(cmd)}")
    completed = subprocess.run(cmd, cwd=str(REPO_ROOT))
    if completed.returncode != 0:
        print(f"ERROR: build of {BUILD_TARGET} failed (exit {completed.returncode}).", file=sys.stderr)
    return completed.returncode


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--build-dir", type=Path, default=DEFAULT_BUILD_DIR)
    parser.add_argument("--skip-build", action="store_true", help="Do not build nes-repl-embedded before running.")
    parser.add_argument("--repl-binary", type=Path, default=None)
    parser.add_argument("--min-prints", type=int, default=3, help="Minimum watch prints to require (default: 3).")
    parser.add_argument("--expected-value", type=float, default=42.0)
    parser.add_argument("--observe-seconds", type=float, default=6.0, help="How long to let the watch query run (default: 6).")
    args = parser.parse_args()

    if not args.skip_build:
        rc = build_repl(args.build_dir)
        if rc != 0:
            return rc

    repl_binary = args.repl_binary if args.repl_binary is not None else args.build_dir / REPL_BINARY_SUBPATH
    if not repl_binary.is_file():
        print(f"ERROR: nes-repl-embedded binary not found: {repl_binary}", file=sys.stderr)
        return 2

    # The watch query is continuous, so we run with WAIT_FOR_QUERY_TERMINATION (which keeps the instance alive while
    # the query runs), let it emit impulses for a few seconds, then SIGTERM it (the REPL's signal handler stops the
    # wait and exits cleanly).
    cmd = [str(repl_binary), "--enable-statistics", "--on-exit", "WAIT_FOR_QUERY_TERMINATION"]
    print(f"Starting NebulaStream instance: {' '.join(cmd)}")
    proc = subprocess.Popen(
        cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, cwd=str(REPO_ROOT)
    )
    assert proc.stdin is not None
    proc.stdin.write(QUERY_SCRIPT)
    proc.stdin.flush()
    # Keep stdin open so the REPL blocks on the next read (instead of hitting EOF and tearing down) while the
    # continuous watch query keeps emitting impulses. communicate() below closes it.

    time.sleep(args.observe_seconds)
    proc.send_signal(signal.SIGTERM)
    try:
        output, _ = proc.communicate(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()
        output, _ = proc.communicate()

    matches = PRINT_RE.findall(output)

    if len(matches) < args.min_prints:
        print(
            f"ERROR: observed only {len(matches)} watch print(s); expected at least {args.min_prints}.",
            file=sys.stderr,
        )
        print("----- captured output -----", file=sys.stderr)
        print(output, file=sys.stderr)
        return 1

    bad = [v for v in matches if float(v) != args.expected_value]
    if bad:
        print(f"ERROR: some watch prints reported unexpected values {bad}, expected {args.expected_value}.", file=sys.stderr)
        return 1

    print(
        f"OK: deployed a WATCH_STATISTIC query and observed the condition-trigger print fire {len(matches)} times "
        f"(periodically), each reporting the expected value {args.expected_value:g}."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
