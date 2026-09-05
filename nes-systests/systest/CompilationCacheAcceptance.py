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

import argparse
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys

REPORT_MARKER = "Nautilus compilation statistics for pipeline "
REPORT_END = "└"
FIELD_PATTERN = re.compile(
    r"^│\s+(eligible|object|mlir|tracingRan|fallback)\s*=\s*(\S+)\s*$",
    re.MULTILINE,
)
REQUIRED_FIELDS = {"eligible", "object", "mlir", "tracingRan", "fallback"}
CORPUS = (
    "tuples/OneTuple.test",
    "formatter/CSV_OUTPUT/InlinedOutputFormatConfig.test",
    "formatter/JSON_OUTPUT/BasicJSONInput.test",
    "operator/aggregation/WindowAggregationCount.test",
    "operator/join/JoinMultipleKeys.test",
    "operator/projection/Projection.test",
    "function/arithmetical/FunctionAdd.test",
)


class AcceptanceFailure(RuntimeError):
    pass


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--systest", type=Path, required=True)
    parser.add_argument("--suite", type=Path, required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--topology", type=Path, required=True)
    parser.add_argument("--run-root", type=Path, required=True)
    return parser.parse_args()


def require_path(path: Path, description: str, executable: bool = False) -> Path:
    resolved = path.resolve()
    if not resolved.exists():
        raise AcceptanceFailure(f"{description} does not exist: {resolved}")
    if executable and (not resolved.is_file() or not os.access(resolved, os.X_OK)):
        raise AcceptanceFailure(f"{description} is not executable: {resolved}")
    return resolved


def prepare_run_root(path: Path) -> Path:
    resolved = path.resolve()
    forbidden = {Path("/").resolve(), Path.home().resolve(), Path.cwd().resolve()}
    if resolved in forbidden:
        raise AcceptanceFailure(f"refusing to clear unsafe run root: {resolved}")
    shutil.rmtree(resolved, ignore_errors=True)
    resolved.mkdir(parents=True)
    (resolved / "cache").mkdir()
    return resolved


def prepare_corpus(suite: Path, run_root: Path) -> Path:
    corpus = run_root / "corpus"
    corpus.mkdir()
    for index, relative_path in enumerate(CORPUS):
        source = require_path(suite / relative_path, "compilation-cache systest")
        shutil.copy2(source, corpus / f"{index:02d}-{source.name}")
    return corpus


def module_events(log_path: Path) -> list[dict[str, str]]:
    text = log_path.read_text(encoding="utf-8", errors="replace")
    sections = text.split(REPORT_MARKER)[1:]
    events = []
    for index, section in enumerate(sections, start=1):
        if REPORT_END not in section:
            raise AcceptanceFailure(f"unterminated compilation report {index} in {log_path}")
        report = section.split(REPORT_END, 1)[0]
        fields = {}
        for key, value in FIELD_PATTERN.findall(report):
            if key in fields:
                raise AcceptanceFailure(f"duplicate field {key!r} in compilation report {index} at {log_path}")
            fields[key] = value
        missing = REQUIRED_FIELDS - fields.keys()
        if missing:
            raise AcceptanceFailure(f"compilation report {index} in {log_path} is missing {sorted(missing)}")
        events.append(fields)
    return events


def assert_field(phase: str, events: list[dict[str, str]], field: str, expected: str) -> None:
    failures = [f"module {index}: {field}={event[field]}" for index, event in enumerate(events, start=1) if event[field] != expected]
    if failures:
        raise AcceptanceFailure(
            f"{phase} has {len(failures)} module reports with {field} != {expected}:\n" + "\n".join(failures[:20])
        )


def validate_reports(phase: str, log_path: Path, cold_module_count: int | None = None) -> int:
    events = module_events(log_path)
    if not events:
        raise AcceptanceFailure(f"{phase} emitted no module reports")
    if cold_module_count is not None and len(events) != cold_module_count:
        raise AcceptanceFailure(f"{phase} emitted {len(events)} module reports; cold emitted {cold_module_count}")
    assert_field(phase, events, "eligible", "1")
    assert_field(phase, events, "fallback", "none")
    if phase == "cold":
        failures = []
        for index, event in enumerate(events, start=1):
            expected = ("written", "written", "1") if event["object"] == "written" else ("hit", "not_checked", "0")
            actual = (event["object"], event["mlir"], event["tracingRan"])
            if actual != expected:
                failures.append(f"module {index}: object={actual[0]} mlir={actual[1]} tracingRan={actual[2]}")
        if failures:
            raise AcceptanceFailure(f"cold has {len(failures)} invalid cache outcomes:\n" + "\n".join(failures[:20]))
        if not any(event["object"] == "written" for event in events):
            raise AcceptanceFailure("cold did not populate any cache entries")
    else:
        assert_field(phase, events, "object", "hit")
        assert_field(phase, events, "mlir", "not_checked")
        assert_field(phase, events, "tracingRan", "0")
    return len(events)


def validate_artifacts(cache: Path) -> None:
    counts = {
        extension: sum(1 for path in cache.glob(f"*.{extension}") if path.is_file())
        for extension in ("manifest", "mlirbc", "o")
    }
    if 0 in counts.values() or len(set(counts.values())) != 1:
        raise AcceptanceFailure(f"cache artifact counts do not match: {counts}")


def tail(path: Path, lines: int = 100) -> str:
    return "".join(path.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)[-lines:])


def run_phase(
    phase: str,
    seed: int,
    systest: Path,
    corpus: Path,
    data: Path,
    topology: Path,
    run_root: Path,
) -> tuple[Path, int]:
    log_path = run_root / f"{phase}.log"
    output_path = run_root / f"{phase}.stdout"
    command = [
        str(systest),
        "--ignoreDisableConfigFile",
        "--shuffle",
        "--shuffle-seed",
        str(seed),
        "--numberConcurrentQueries",
        "6",
        "--testLocations",
        str(corpus),
        "--clusterConfig",
        str(topology),
        "--data",
        str(data),
        "--workingDir",
        str(run_root / f"{phase}-working"),
        "--log-path",
        str(log_path),
        "--",
        "--worker.default_query_execution.execution_mode=COMPILER",
        "--worker.default_query_execution.slice_cache.enable_slice_cache=true",
        "--worker.enable_compilation_cache=true",
        f"--worker.compilation_cache_dir={run_root / 'cache'}",
    ]
    print(f"Running {phase} process: {shlex.join(command)}", flush=True)
    with output_path.open("w", encoding="utf-8") as output:
        result = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, check=False)
    if result.returncode != 0:
        raise AcceptanceFailure(
            f"{phase} systest process exited with {result.returncode}; output: {output_path}\n{tail(output_path)}"
        )
    output = output_path.read_text(encoding="utf-8", errors="replace")
    loaded = re.search(r"Loaded \d+/\d+ test files containing a total of (\d+) queries", output)
    if loaded is None or int(loaded.group(1)) <= 0:
        raise AcceptanceFailure(f"{phase} did not load any queries; output: {output_path}\n{tail(output_path)}")
    query_count = int(loaded.group(1))
    if output.count("PASSED") != query_count or "FAILED" in output:
        raise AcceptanceFailure(f"{phase} did not pass all {query_count} queries; output: {output_path}\n{tail(output_path)}")
    return log_path, query_count


def main() -> int:
    arguments = parse_arguments()

    systest = require_path(arguments.systest, "systest executable", executable=True)
    suite = require_path(arguments.suite, "systest suite")
    data = require_path(arguments.data, "systest data directory")
    topology = require_path(arguments.topology, "systest topology")
    run_root = prepare_run_root(arguments.run_root)
    corpus = prepare_corpus(suite, run_root)

    cold_log, cold_query_count = run_phase("cold", 1729, systest, corpus, data, topology, run_root)
    module_count = validate_reports("cold", cold_log)
    validate_artifacts(run_root / "cache")

    warm_log, warm_query_count = run_phase("warm", 8675309, systest, corpus, data, topology, run_root)
    if warm_query_count != cold_query_count:
        raise AcceptanceFailure(f"warm loaded {warm_query_count} queries; cold loaded {cold_query_count}")
    validate_reports("warm", warm_log, module_count)
    validate_artifacts(run_root / "cache")

    print(
        f"Compilation cache acceptance passed: {module_count}/{module_count} "
        "warm module hits with no tracing or fallback"
    )
    print(f"Evidence: {cold_log} and {warm_log}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except AcceptanceFailure as error:
        print(f"compilation cache acceptance failed: {error}", file=sys.stderr)
        sys.exit(1)
