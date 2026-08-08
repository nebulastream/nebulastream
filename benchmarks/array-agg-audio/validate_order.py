#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Validate ARRAY_AGG_PAGE_SORTED ordering against ARRAY_AGG and the source data."""

from argparse import ArgumentParser
from base64 import b64decode
from bisect import bisect_left
from pathlib import Path
from struct import pack
from subprocess import run
from sys import executable
from tempfile import TemporaryDirectory


SAMPLE_RATE = 44_100
CSV_ROW_BYTES = 42
WINDOW_SIZE_US = 1_000_000
WINDOW_ADVANCE_US = 10_000


def make_query(input_path: Path, output_path: Path, aggregation: str) -> str:
    return f"""\
SELECT start AS window_start,
       end AS window_end,
       TO_BASE64({aggregation}(CASTTOTYPE(sample AS FLOAT32))) AS audio
FROM (
    SELECT timestamp / UINT64(1000) AS timestamp, sample
    FROM File(
        '{input_path}' AS \"SOURCE\".FILE_PATH,
        'localhost:8080' AS \"SOURCE\".\"HOST\",
        'CSV' AS INPUT_FORMATTER.\"TYPE\",
        SCHEMA(sample FLOAT64 NOT NULL, timestamp UINT64 NOT NULL) AS \"SOURCE\".\"SCHEMA\")
)
WINDOW SLIDING(timestamp, SIZE {WINDOW_SIZE_US} MS, ADVANCE BY {WINDOW_ADVANCE_US} MS)
INTO File(
    '{output_path}' AS \"SINK\".FILE_PATH,
    'localhost:8080' AS \"SINK\".\"HOST\",
    'CSV' AS \"SINK\".OUTPUT_FORMAT,
    FALSE AS \"SINK\".APPEND,
    FALSE AS \"OUTPUT_FORMATTER\".QUOTE_STRINGS);
"""


def execute_query(binary: Path, query: str, workers: int) -> None:
    command = [
        str(binary),
        "--on-exit",
        "WAIT_FOR_QUERY_TERMINATION",
        "--error-behaviour",
        "FAIL_FAST",
        "--",
        f"--worker.query_engine.number_of_worker_threads={workers}",
        "--worker.total_memory_in_bytes=2147483648",
        "--worker.default_query_execution.execution_mode=COMPILER",
    ]
    completed = run(command, input=query, text=True, capture_output=True, check=False)
    if completed.returncode != 0:
        raise RuntimeError(
            f"query failed with exit code {completed.returncode}\nstdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )


def read_results(path: Path) -> dict[tuple[int, int], bytes]:
    results: dict[tuple[int, int], bytes] = {}
    with path.open("rt", encoding="utf-8") as output:
        header = output.readline()
        if not header:
            raise RuntimeError(f"empty validation output: {path}")
        for line_number, line in enumerate(output, start=2):
            fields = line.rstrip("\n").split(",", maxsplit=2)
            if len(fields) != 3:
                raise RuntimeError(f"invalid output row at {path}:{line_number}")
            key = (int(fields[0]), int(fields[1]))
            if key in results:
                raise RuntimeError(f"duplicate window {key} in {path}")
            results[key] = b64decode(fields[2], validate=True)
    return results


def read_source(path: Path) -> tuple[list[int], bytes]:
    timestamps: list[int] = []
    samples = bytearray()
    with path.open("rt", encoding="utf-8") as source:
        for line in source:
            sample, timestamp_ns = line.rstrip("\n").split(",", maxsplit=1)
            timestamps.append(int(timestamp_ns) // 1000)
            samples.extend(pack("<f", float(sample)))
    if timestamps != sorted(timestamps):
        raise RuntimeError("validation source is not timestamp ordered")
    return timestamps, bytes(samples)


def validate_against_source(name: str, results: dict[tuple[int, int], bytes], timestamps: list[int], samples: bytes) -> None:
    for (window_start, window_end), actual in results.items():
        first = bisect_left(timestamps, window_start)
        last = bisect_left(timestamps, window_end)
        expected = samples[first * 4 : last * 4]
        if actual != expected:
            differing_byte = next((index for index, pair in enumerate(zip(actual, expected)) if pair[0] != pair[1]), None)
            raise RuntimeError(
                f"{name} differs from the timestamp-ordered source in window [{window_start}, {window_end}); "
                f"actual_bytes={len(actual)}, expected_bytes={len(expected)}, first_difference={differing_byte}"
            )


def main() -> None:
    benchmark_dir = Path(__file__).resolve().parent
    repository = benchmark_dir.parent.parent
    parser = ArgumentParser()
    parser.add_argument(
        "--binary",
        type=Path,
        default=repository / "cmake-build-release/nes-frontend/apps/nes-repl-embedded",
    )
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--seconds", type=int, default=2)
    args = parser.parse_args()

    if args.seconds < 1:
        raise ValueError("--seconds must be positive")
    if not args.binary.is_file():
        raise FileNotFoundError(args.binary)

    with TemporaryDirectory(prefix="array-agg-order-") as temporary_directory:
        temporary = Path(temporary_directory)
        input_path = temporary / "audio.csv"
        rows = args.seconds * SAMPLE_RATE
        run(
            [
                executable,
                str(benchmark_dir / "generate_audio_csv.py"),
                "--output",
                str(input_path),
                "--size-bytes",
                str(rows * CSV_ROW_BYTES),
            ],
            check=True,
        )

        outputs = {}
        for name, aggregation in (("baseline", "ARRAY_AGG"), ("page-sorted", "ARRAY_AGG_PAGE_SORTED")):
            output_path = temporary / f"{name}.csv"
            execute_query(args.binary, make_query(input_path, output_path, aggregation), args.workers)
            outputs[name] = read_results(output_path)

        baseline = outputs["baseline"]
        page_sorted = outputs["page-sorted"]
        if baseline.keys() != page_sorted.keys():
            missing = sorted(baseline.keys() - page_sorted.keys())
            extra = sorted(page_sorted.keys() - baseline.keys())
            raise RuntimeError(f"window sets differ: missing_from_page_sorted={missing}, extra_in_page_sorted={extra}")
        for window in baseline:
            if baseline[window] != page_sorted[window]:
                raise RuntimeError(f"aggregations produce different ordered bytes for window {window}")

        timestamps, samples = read_source(input_path)
        validate_against_source("ARRAY_AGG", baseline, timestamps, samples)
        validate_against_source("ARRAY_AGG_PAGE_SORTED", page_sorted, timestamps, samples)

        total_bytes = sum(map(len, baseline.values()))
        print(
            f"validated {len(baseline)} windows and {total_bytes} ordered array bytes per implementation "
            f"using {args.workers} workers"
        )


if __name__ == "__main__":
    main()
