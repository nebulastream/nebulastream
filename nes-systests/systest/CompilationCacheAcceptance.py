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
from collections import Counter, defaultdict
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys

REPORT_MARKER = "Nautilus compilation statistics for pipeline "
QUERY_MARKER = "Systest query result: "
PLAN_MARKER = "PipelinedQueryPlan for Query: "
REPORT_END = "└"
FIELD_PATTERN = re.compile(r"^│[ \t]+(key|eligible|object|mlir|tracingRan|fallback)[ \t]*=[ \t]*(\S+)[ \t]*$", re.MULTILINE)
REQUIRED_FIELDS = {"key", "eligible", "object", "mlir", "tracingRan", "fallback"}
LOG_RECORD_PATTERN = re.compile(r"(?=^\[\d{2}:\d{2}:\d{2}\.\d+\] )", re.MULTILINE)
QUERY_ID_PATTERN = re.compile(r"QueryId\(local=([^,]+), distributed=([^\)]+)\)")
PIPELINE_PATTERN = re.compile(r"Pipeline\(ID\((\d+)\), Provider\([^)]*\)\)")
OPERATOR_PATTERN = re.compile(r"PhysicalOperator\(([^)]+)\)")
ANSI_PATTERN = re.compile(r"\x1b\[[0-9;]*m")
RESULT_PATTERN = re.compile(r"^\s*(\d+)/(\d+) \([\d .]+%\) (.+?)\.*(PASSED|FAILED)[ \t]*$", re.MULTILINE)
CORPUS = (
    "tuples/OneTuple.test",
    "formatter/BasicTypeSupport.test",
    "formatter/CSV_OUTPUT/InlinedOutputFormatConfig.test",
    "formatter/CSV_OUTPUT/VarsizedDoesNotFitInOutputBuffer.test",
    "formatter/JSON/BasicJSON.test",
    "formatter/JSON/NestedJSON.test",
    "formatter/JSON_OUTPUT/BasicJSONInput.test",
    "formatter/JSON_OUTPUT/SpecialCharactersJSON.test",
    "formatter/JSON_OUTPUT/VarsizedJSON.test",
    "operator/aggregation/WindowAggregationCount.test",
    "operator/aggregation/WindowAggregationVarSizedMinimal.test",
    "operator/join/JoinMultipleKeys.test",
    "operator/join/JoinWithVarSized.test",
    "operator/join/ThetaJoinVarSized.test",
    "windows/SlidingWindowsWithGap.test",
    "operator/projection/Projection.test",
    "function/arithmetical/FunctionAdd.test",
    "datatype/RawStringAndBooleanLiterals.test",
    "datatype/DeeplyNestedVarsizedEquality.test",
    "function/Conditional.test",
    "function/varsized/CharLength.test",
    "function/varsized/OctetLength.test",
    "regression/VariableSizedLiteralBytes.test",
)
INFERENCE_CORPUS = ("inference/InferModel.test", "inference/InferModelTwoModels.test")


class AcceptanceFailure(RuntimeError):
    pass


@dataclass
class Coverage:
    queries: Counter
    modules: Counter
    planned_aborts: Counter
    events: list[dict[str, str]]


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Strict fresh-process object-cache acceptance; curated corpus by default.")
    parser.add_argument("--systest", type=Path, required=True)
    parser.add_argument("--suite", type=Path, required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--topology", type=Path, required=True)
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--inference", choices=("enabled", "disabled"), required=True,
                        help="Explicit dependency gate; enabled requires ovc and all selected inference queries to pass.")
    parser.add_argument("--full-suite", action="store_true",
                        help="Run every .test file, including large/compilation-intensive tests; requires their test data.")
    parser.add_argument("--seeds", type=int, nargs="+", default=(1729, 8675309, 42),
                        help="First seed is cold; all subsequent seeds run in fresh warm processes.")
    parser.add_argument("--join-strategies", nargs="+", choices=("HASH_JOIN", "NESTED_LOOP_JOIN"), default=("HASH_JOIN",))
    parser.add_argument("--slice-cache", nargs="+", choices=("true", "false"), default=("true", "false"))
    arguments = parser.parse_args(argv)
    if len(arguments.seeds) < 2 or len(set(arguments.seeds)) != len(arguments.seeds):
        parser.error("--seeds requires at least two distinct shuffle seeds")
    if any(seed < 0 or seed > 2**32 - 1 for seed in arguments.seeds):
        parser.error("--seeds must fit uint32")
    return arguments


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
    if any(resolved == path or resolved in path.parents for path in forbidden):
        raise AcceptanceFailure(f"refusing to clear unsafe run root: {resolved}")
    shutil.rmtree(resolved, ignore_errors=True)
    resolved.mkdir(parents=True)
    return resolved


def prepare_corpus(suite: Path, run_root: Path, full_suite: bool, inference: str) -> Path:
    if inference == "enabled" and shutil.which("ovc") is None:
        raise AcceptanceFailure("inference enabled but the OpenVINO converter ovc is unavailable")
    selected = tuple(sorted(path.relative_to(suite).as_posix() for path in suite.rglob("*.test"))) if full_suite else CORPUS
    if not full_suite and inference == "enabled":
        selected += INFERENCE_CORPUS
    corpus = run_root / "corpus"
    corpus.mkdir()
    included = []
    for relative_path in selected:
        source = require_path(suite / relative_path, "compilation-cache systest")
        groups = re.search(r"^# groups:\s*\[([^]]*)\]", source.read_text(encoding="utf-8"), re.MULTILINE)
        is_inference = relative_path.startswith("inference/") or (
            groups and "inference" in {group.strip().lower() for group in groups.group(1).split(",")}
        )
        if is_inference and inference == "disabled":
            print(f"Inference explicitly disabled: {relative_path}", flush=True)
            continue
        destination = corpus / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        included.append(relative_path)
    if not included:
        raise AcceptanceFailure("selected corpus is empty")
    (run_root / "corpus.json").write_text(json.dumps(included, indent=2) + "\n", encoding="utf-8")
    scope = "full .test suite" if full_suite else "curated CI corpus (not the full suite)"
    print(f"Selected {len(included)} files: {scope}; inference {inference}", flush=True)
    return corpus


def query_id(record: str, log_path: Path) -> tuple[str, str]:
    match = QUERY_ID_PATTERN.search(record)
    if match is None:
        raise AcceptanceFailure(f"missing query identity in telemetry at {log_path}: {record.splitlines()[0]}")
    return match.group(1), match.group(2)


def module_fields(record: str, log_path: Path) -> dict[str, str]:
    if REPORT_END not in record:
        raise AcceptanceFailure(f"unterminated compilation report in {log_path}")
    fields = {}
    for key, value in FIELD_PATTERN.findall(record.split(REPORT_END, 1)[0]):
        if key in fields:
            raise AcceptanceFailure(f"duplicate field {key!r} in compilation report at {log_path}")
        fields[key] = value
    missing = REQUIRED_FIELDS - fields.keys()
    if missing:
        raise AcceptanceFailure(f"compilation report in {log_path} is missing {sorted(missing)}")
    if re.fullmatch(r"[0-9a-f]{64}", fields["key"]) is None:
        raise AcceptanceFailure(f"invalid semantic cache key in {log_path}: {fields['key']!r}")
    return fields


def query_result(record: str, log_path: Path) -> dict:
    try:
        result = json.loads(record.split(QUERY_MARKER, 1)[1])
    except (ValueError, IndexError) as error:
        raise AcceptanceFailure(f"invalid query result telemetry in {log_path}: {error}") from error
    if (not isinstance(result, dict) or set(result) != {"query", "configuration", "passed", "kind", "differential"}
            or not isinstance(result["query"], str) or re.fullmatch(r".+:[1-9]\d*", result["query"]) is None
            or not isinstance(result["configuration"], dict)
            or any(not isinstance(key, str) or not isinstance(value, str) for key, value in result["configuration"].items())
            or type(result["passed"]) is not bool or type(result["differential"]) is not bool
            or result["kind"] not in ("execute", "expected_error", "explain")):
        raise AcceptanceFailure(f"incomplete query result telemetry in {log_path}: {result}")
    if not result["passed"]:
        raise AcceptanceFailure(f"query failed in {log_path}: {result['query']}")
    return result


def display_query(result: dict) -> str:
    name, number = result["query"].rsplit(":", 1)
    configuration = ", ".join(f"{key}={value}" for key, value in sorted(result["configuration"].items()))
    return f"{name}:{int(number):02d}" + (f" [{configuration}]" if configuration else "")


def parse_reports(log_path: Path, output_queries: Counter) -> Coverage:
    text = require_path(log_path, "module telemetry log").read_text(encoding="utf-8")
    queries, modules, planned_aborts, reported_queries = Counter(), Counter(), Counter(), Counter()
    plans = defaultdict(dict)
    completed_plans = {}
    observed = {}
    events = []
    plan_reports = 0
    for record in LOG_RECORD_PATTERN.split(text):
        if PLAN_MARKER in record:
            plan_reports += 1
            local, query = query_id(record, log_path)
            pipelines = list(PIPELINE_PATTERN.finditer(record))
            if not pipelines:
                raise AcceptanceFailure(f"empty pipeline plan for {query} in {log_path}")
            for index, pipeline in enumerate(pipelines):
                end = pipelines[index + 1].start() if index + 1 < len(pipelines) else len(record)
                operators = OPERATOR_PATTERN.findall(record[pipeline.end():end])
                if not operators:
                    raise AcceptanceFailure(f"missing operator chain for {query} in {log_path}")
                if operators[0] not in ("NES::SourceDescriptorPhysicalOperator", "NES::SinkPhysicalOperator"):
                    identity = (local, pipeline.group(1))
                    if identity in plans[query] and plans[query][identity] != tuple(operators):
                        raise AcceptanceFailure(f"conflicting pipeline identity for {query} in {log_path}")
                    plans[query][identity] = tuple(operators)
        elif REPORT_MARKER in record:
            local, query = query_id(record.splitlines()[0], log_path)
            pipeline = re.search(re.escape(REPORT_MARKER) + r"(\d+):", record)
            if pipeline is None:
                raise AcceptanceFailure(f"missing pipeline identity in {log_path}")
            identity = (local, pipeline.group(1))
            if identity not in plans.get(query, {}) and (query, identity) not in completed_plans:
                raise AcceptanceFailure(f"module without pipeline-plan telemetry for {query} in {log_path}")
            if (query, identity) in observed:
                raise AcceptanceFailure(f"duplicate module report for {query}, pipeline {identity} in {log_path}")
            fields = module_fields(record, log_path)
            observed[(query, identity)] = fields
            events.append(fields)
        elif QUERY_MARKER in record:
            result = query_result(record, log_path)
            query = result["query"]
            configuration = tuple(sorted(result["configuration"].items()))
            identity = (query, configuration, result["kind"], result["differential"])
            if identity in queries:
                raise AcceptanceFailure(f"duplicate query result for {query} in {log_path}")
            queries[identity] += 1
            reported_queries[display_query(result)] += 1
            branches = (query, query + "-differential") if result["differential"] else (query,)
            for branch in branches:
                expected = plans.pop(branch, {})
                if result["kind"] == "execute" and not expected:
                    raise AcceptanceFailure(f"missing module telemetry for executed query {branch} in {log_path}")
                if result["kind"] == "explain" and expected:
                    raise AcceptanceFailure(f"unexpected execution for EXPLAIN query {branch} in {log_path}")
                for pipeline, operators in expected.items():
                    if (branch, pipeline) in completed_plans:
                        raise AcceptanceFailure(f"pipeline associated with multiple query results for {branch} in {log_path}")
                    completed_plans[(branch, pipeline)] = (identity, operators)
    for (branch, pipeline), (identity, operators) in completed_plans.items():
        fields = observed.pop((branch, pipeline), None)
        if fields is None:
            if identity[2] != "expected_error":
                raise AcceptanceFailure(f"missing module report for {branch}, pipeline {pipeline} in {log_path}")
            planned_aborts[(identity, branch, operators)] += 1
        else:
            modules[(identity, branch, operators, fields["key"])] += 1
    if not events:
        raise AcceptanceFailure(f"{log_path} emitted no module reports")
    if plans or observed:
        raise AcceptanceFailure(f"module/plan telemetry without query results in {log_path}: {list(plans)}")
    if (text.count(REPORT_MARKER) != len(events) or text.count(QUERY_MARKER) != queries.total()
            or text.count(PLAN_MARKER) != plan_reports):
        raise AcceptanceFailure(f"malformed or unassociated telemetry in {log_path}")
    compare_coverage("query result telemetry", output_queries, reported_queries)
    return Coverage(queries, modules, planned_aborts, events)


def compare_coverage(description: str, expected: Counter, actual: Counter) -> None:
    if expected != actual:
        raise AcceptanceFailure(
            f"{description} coverage mismatch: expected {expected.total()}, observed {actual.total()}; "
            f"missing={list((expected - actual).items())[:10]}, unexpected={list((actual - expected).items())[:10]}"
        )


def assert_field(phase: str, events: list[dict[str, str]], field: str, expected: str) -> None:
    failures = [f"module {index}: {field}={event[field]} key={event['key']}"
                for index, event in enumerate(events, start=1) if event[field] != expected]
    if failures:
        raise AcceptanceFailure(
            f"{phase} has {len(failures)} module reports with {field} != {expected}:\n" + "\n".join(failures[:20])
        )


def validate_reports(phase: str, log_path: Path, output_queries: Counter, cold: Coverage | None = None) -> Coverage:
    coverage = parse_reports(log_path, output_queries)
    events = coverage.events
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
        written = {event["key"] for event in events if event["object"] == "written"}
        if not written or written != {event["key"] for event in events}:
            raise AcceptanceFailure("cold did not populate every observed cache entry")
    else:
        if cold is None:
            raise AcceptanceFailure(f"{phase} has no cold coverage baseline")
        assert_field(phase, events, "object", "hit")
        assert_field(phase, events, "mlir", "not_checked")
        assert_field(phase, events, "tracingRan", "0")
        compare_coverage(f"{phase} queries", cold.queries, coverage.queries)
        compare_coverage(f"{phase} per-query semantic modules", cold.modules, coverage.modules)
        compare_coverage(f"{phase} per-query planned-abort pipelines", cold.planned_aborts, coverage.planned_aborts)
    return coverage


def validate_artifacts(cache: Path, coverage: Coverage) -> None:
    keys = {event["key"] for event in coverage.events}
    for extension in ("manifest", "mlirbc", "o"):
        files = [path for path in cache.glob(f"*.{extension}") if path.is_file()]
        if {path.stem for path in files} != keys or any(path.stat().st_size == 0 for path in files):
            raise AcceptanceFailure(f"cache {extension} artifacts do not match observed module keys at {cache}")


def tail(path: Path, lines: int = 100) -> str:
    return "".join(path.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)[-lines:])


def validate_process(phase: str, returncode: int, output_path: Path, file_count: int) -> Counter:
    if returncode != 0:
        raise AcceptanceFailure(f"{phase} systest process exited with {returncode}; output: {output_path}\n{tail(output_path)}")
    output = ANSI_PATTERN.sub("", output_path.read_text(encoding="utf-8"))
    loaded = re.findall(r"Loaded (\d+)/(\d+) test files containing a total of (\d+) queries", output)
    if len(loaded) != 1 or tuple(map(int, loaded[0][:2])) != (file_count, file_count) or int(loaded[0][2]) <= 0:
        raise AcceptanceFailure(f"{phase} did not load all {file_count} files and nonempty queries; output: {output_path}")
    query_count = int(loaded[0][2])
    results = RESULT_PATTERN.findall(output)
    if (len(results) != query_count or {int(index) for index, _, _, _ in results} != set(range(1, query_count + 1))
            or any(int(total) != query_count or status != "PASSED" for _, total, _, status in results)
            or output.count("PASSED") != query_count or "FAILED" in output or output.count("All queries passed.") != 1):
        raise AcceptanceFailure(f"{phase} did not pass all {query_count} queries; output: {output_path}\n{tail(output_path)}")
    return Counter(name for _, _, name, _ in results)


def run_phase(
    phase: str,
    seed: int,
    systest: Path,
    corpus: Path,
    data: Path,
    topology: Path,
    run_root: Path,
    join_strategy: str,
    slice_cache: str,
) -> tuple[Path, Counter]:
    log_path = run_root / f"{phase}.log"
    output_path = run_root / f"{phase}.stdout"
    command = [
        str(systest), "--ignoreDisableConfigFile", "--shuffle", "--shuffle-seed", str(seed),
        "--numberConcurrentQueries", "6", "--testLocations", str(corpus),
        "--clusterConfig", str(topology), "--data", str(data),
        "--optimizer", f"join_strategy={join_strategy}",
        "--workingDir", str(run_root / f"{phase}-working"), "--log-path", str(log_path), "--",
        "--worker.default_query_execution.execution_mode=COMPILER",
        f"--worker.default_query_execution.slice_cache.enable_slice_cache={slice_cache}",
        "--worker.total_memory_in_bytes=805306368", "--worker.unpooled_memory_fraction=0.8333333",
        "--worker.enable_compilation_cache=true", f"--worker.compilation_cache_dir={run_root / 'cache'}",
    ]
    print(f"Running {phase} process: {shlex.join(command)}", flush=True)
    with output_path.open("w", encoding="utf-8") as output:
        result = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, check=False)
    return log_path, validate_process(phase, result.returncode, output_path, len(list(corpus.rglob("*.test"))))


def main() -> int:
    arguments = parse_arguments()
    systest = require_path(arguments.systest, "systest executable", executable=True)
    suite = require_path(arguments.suite, "systest suite")
    data = require_path(arguments.data, "systest data directory")
    topology = require_path(arguments.topology, "systest topology")
    root = arguments.run_root.resolve()
    if any(root == path or root in path.parents for path in (systest, suite, data, topology)):
        raise AcceptanceFailure(f"run root would remove acceptance inputs: {root}")
    run_root = prepare_run_root(root)
    corpus = prepare_corpus(suite, run_root, arguments.full_suite, arguments.inference)
    warm_modules = 0
    warm_planned_aborts = 0
    for strategy in arguments.join_strategies:
        for slice_cache in arguments.slice_cache:
            configuration_root = run_root / f"{strategy}-slice-{slice_cache}"
            (configuration_root / "cache").mkdir(parents=True)
            cold = None
            for index, seed in enumerate(arguments.seeds):
                phase = "cold" if index == 0 else f"warm-{index}"
                log_path, output_queries = run_phase(
                    phase, seed, systest, corpus, data, topology, configuration_root, strategy, slice_cache
                )
                coverage = validate_reports(phase, log_path, output_queries, cold)
                validate_artifacts(configuration_root / "cache", coverage)
                count = len(coverage.events)
                if cold is None:
                    cold = coverage
                    outcome = f"{count} observed cold modules"
                else:
                    warm_modules += count
                    warm_planned_aborts += coverage.planned_aborts.total()
                    outcome = f"{count}/{count} observed warm module hits"
                print(f"{strategy}, slice-cache={slice_cache}, seed={seed}: {coverage.queries.total()} passed queries; {outcome}; "
                      f"{coverage.planned_aborts.total()} planned-but-uncompiled expected-error pipelines (not module hits)", flush=True)
    print(f"Compilation cache acceptance passed: {warm_modules}/{warm_modules} observed warm module hits, no tracing/fallback")
    print(f"Planned-but-uncompiled expected-error pipelines across warm processes: {warm_planned_aborts} "
          "(matched cold coverage; not module hits)")
    print(f"Evidence: {run_root}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (AcceptanceFailure, OSError) as error:
        print(f"compilation cache acceptance failed: {error}", file=sys.stderr)
        sys.exit(1)
