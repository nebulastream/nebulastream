# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections import Counter
import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import CompilationCacheAcceptance as acceptance


KEY_A = "a" * 64
KEY_B = "b" * 64


def record(message, query="First:1", local="uuid-cold"):
    return f"[01:02:03.000001] [D] [localhost:8080] [WorkerThread-0] [Task:QueryId(local={local}, distributed={query})] {message}\n"


def plan(query="First:1", local="uuid-cold", pipelines=(2,)):
    body = f"PipelinedQueryPlan for Query: QueryId(local={local}, distributed={query})\n"
    body += "  Pipeline(ID(0), Provider(COMPILER))\n    PhysicalOperator(NES::SourceDescriptorPhysicalOperator)\n"
    for pipeline in pipelines:
        body += f"  Pipeline(ID({pipeline}), Provider(None))\n    PhysicalOperator(NES::ScanPhysicalOperator)\n"
        body += "      PhysicalOperator(NES::EmitPhysicalOperator)\n"
    body += "  Pipeline(ID(99999), Provider(None))\n    PhysicalOperator(NES::SinkPhysicalOperator)\n"
    return record(body, query, local)


def module(query="First:1", local="uuid-cold", pipeline=2, key=KEY_A, cold=True, **overrides):
    fields = {"key": key, "eligible": "1", "object": "written" if cold else "hit",
              "mlir": "written" if cold else "not_checked", "tracingRan": "1" if cold else "0", "fallback": "none"}
    fields.update(overrides)
    body = f"Nautilus compilation statistics for pipeline {pipeline}:\n┌─ nautilus compilation statistics\n│  cache\n"
    body += "".join(f"│    {field} = {value}\n" for field, value in fields.items() if value is not None)
    return record(body + "└─────────", query, local)


def result(query="First:1", configuration=None, passed=True, kind="execute", differential=False):
    return record(acceptance.QUERY_MARKER + json.dumps({
        "query": query, "configuration": configuration or {}, "passed": passed, "kind": kind, "differential": differential,
    }), query)


def execution(query="First:1", local="uuid-cold", pipelines=(2,), keys=(KEY_A,), cold=True, configuration=None):
    return plan(query, local, pipelines) + "".join(
        module(query, local, pipeline, key, cold) for pipeline, key in zip(pipelines, keys, strict=True)
    ) + result(query, configuration=configuration)


def stdout(queries=("First:01",), files=1):
    output = f"Loaded {files}/{files} test files containing a total of {len(queries)} queries\n"
    for index, query in enumerate(queries, start=1):
        output += f"{index}/{len(queries)} (100.0%) {query}........\x1b[1m\x1b[38;2;000;128;000mPASSED \n\x1b[0m"
    return output + "\nAll queries passed.\n"


class CompilationCacheAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.log = self.root / "phase.log"
        self.output = self.root / "phase.stdout"
        self.queries = Counter({"First:01": 1})

    def validate(self, text, cold=None, queries=None):
        self.log.write_text(text, encoding="utf-8")
        return acceptance.validate_reports("cold" if cold is None else "warm", self.log, queries or self.queries, cold)

    def cold(self):
        return self.validate(execution())

    def test_fresh_process_history_and_order_do_not_change_coverage(self):
        queries = Counter({"First:01": 1, "Second:01": 1})
        cold = self.validate(execution() + execution("Second:1", "uuid-second", (3,), (KEY_B,)), queries=queries)
        warm = self.validate(execution("Second:1", "other-uuid", (87,), (KEY_B,), False)
                             + execution("First:1", "fresh-uuid", (91,), (KEY_A,), False), cold, queries)
        self.assertEqual(len(warm.events), 2)
        self.assertEqual(cold.modules, warm.modules)

    def test_cold_allows_in_process_reuse_with_populated_key(self):
        text = plan(pipelines=(2, 3)) + module() + module(pipeline=3, cold=False) + result()
        coverage = self.validate(text)
        self.assertEqual(len(coverage.events), 2)
        self.assertEqual(coverage.modules.total(), 2)

    def test_module_multiplicity_is_preserved(self):
        cold = self.validate(execution(pipelines=(2, 3), keys=(KEY_A, KEY_A)))
        with self.assertRaisesRegex(acceptance.AcceptanceFailure, "coverage mismatch"):
            self.validate(execution(cold=False), cold)

    def test_same_count_different_module_keys_fails(self):
        cold = self.cold()
        with self.assertRaisesRegex(acceptance.AcceptanceFailure, "semantic modules.*coverage mismatch"):
            self.validate(execution(keys=(KEY_B,), cold=False), cold)

    def test_same_key_totals_assigned_to_different_queries_fail(self):
        queries = Counter({"First:01": 1, "Second:01": 1})
        cold = self.validate(execution() + execution("Second:1", "uuid-second", (3,), (KEY_B,)), queries=queries)
        with self.assertRaisesRegex(acceptance.AcceptanceFailure, "semantic modules.*coverage mismatch"):
            self.validate(execution(keys=(KEY_B,), cold=False)
                          + execution("Second:1", "uuid-second", (3,), (KEY_A,), False), cold, queries)

    def test_same_count_different_queries_fails(self):
        cold = self.cold()
        with self.assertRaisesRegex(acceptance.AcceptanceFailure, "queries coverage mismatch"):
            self.validate(execution("Other:1", cold=False), cold, Counter({"Other:01": 1}))

    def test_configuration_is_part_of_semantic_coverage(self):
        cold = self.validate(execution(configuration={"worker.option": "1"}), queries=Counter({"First:01 [worker.option=1]": 1}))
        with self.assertRaisesRegex(acceptance.AcceptanceFailure, "queries coverage mismatch"):
            self.validate(execution(cold=False, configuration={"worker.option": "2"}), cold,
                          Counter({"First:01 [worker.option=2]": 1}))

    def test_sequential_configuration_overrides_are_individually_accounted(self):
        text = execution(configuration={"worker.option": "1"})
        text += execution(pipelines=(3,), keys=(KEY_B,), configuration={"worker.option": "2"})
        coverage = self.validate(text, queries=Counter({"First:01 [worker.option=1]": 1, "First:01 [worker.option=2]": 1}))
        self.assertEqual(coverage.queries.total(), 2)
        self.assertEqual(coverage.modules.total(), 2)

    def test_empty_execution_fails(self):
        for text in ("", result(kind="expected_error"), result(kind="explain")):
            with self.subTest(text=text), self.assertRaises(acceptance.AcceptanceFailure):
                self.validate(text)

    def test_missing_log_fails(self):
        with self.assertRaisesRegex(acceptance.AcceptanceFailure, "telemetry log does not exist"):
            acceptance.validate_reports("cold", self.log, self.queries)

    def test_whole_query_missing_modules_fails_even_in_cold(self):
        with self.assertRaisesRegex(acceptance.AcceptanceFailure, "missing module telemetry"):
            self.validate(execution() + result("Second:1"), queries=Counter({"First:01": 1, "Second:01": 1}))

    def test_missing_module_report_fails_even_when_another_module_hits(self):
        with self.assertRaisesRegex(acceptance.AcceptanceFailure, "missing module report"):
            self.validate(plan(pipelines=(2, 3)) + module() + result())

    def test_duplicate_report_cannot_replace_missing_module(self):
        with self.assertRaisesRegex(acceptance.AcceptanceFailure, "duplicate module report"):
            self.validate(plan(pipelines=(2, 3)) + module() + module() + result())

    def test_report_without_plan_fails(self):
        with self.assertRaisesRegex(acceptance.AcceptanceFailure, "without pipeline-plan telemetry"):
            self.validate(module() + result())

    def test_missing_query_result_fails(self):
        with self.assertRaisesRegex(acceptance.AcceptanceFailure, "without query results"):
            self.validate(plan() + module())

    def test_duplicate_query_result_fails(self):
        with self.assertRaisesRegex(acceptance.AcceptanceFailure, "duplicate query result"):
            self.validate(execution() + result())

    def test_warm_requires_a_cold_baseline(self):
        self.log.write_text(execution(cold=False), encoding="utf-8")
        with self.assertRaisesRegex(acceptance.AcceptanceFailure, "no cold coverage baseline"):
            acceptance.validate_reports("warm", self.log, self.queries)

    def test_missing_fields_fail(self):
        for field in acceptance.REQUIRED_FIELDS:
            with self.subTest(field=field), self.assertRaisesRegex(acceptance.AcceptanceFailure, "missing"):
                self.validate(plan() + module(**{field: None}) + result())

    def test_invalid_key_fails(self):
        with self.assertRaisesRegex(acceptance.AcceptanceFailure, "invalid semantic cache key"):
            self.validate(plan() + module(key="none") + result())

    def test_duplicate_field_fails(self):
        with self.assertRaisesRegex(acceptance.AcceptanceFailure, "duplicate field"):
            self.validate(plan() + module().replace("└", "│    object = hit\n└") + result())

    def test_unterminated_report_fails(self):
        with self.assertRaisesRegex(acceptance.AcceptanceFailure, "unterminated"):
            self.validate(plan() + module().replace("└", "") + result())

    def test_missing_query_identity_fails(self):
        with self.assertRaisesRegex(acceptance.AcceptanceFailure, "missing query identity"):
            self.validate(plan() + module().replace("QueryId(", "Unidentified(") + result())

    def test_misses_bypasses_tracing_and_fallback_are_never_filtered(self):
        cold = self.cold()
        cases = ({"eligible": "0"}, {"object": "miss"}, {"object": "written"}, {"object": "bypass"},
                 {"tracingRan": "1"}, {"mlir": "hit"}, {"fallback": "invalid_object"},
                 {"fallback": "non_relocatable_pointer"})
        for fields in cases:
            with self.subTest(fields=fields), self.assertRaises(acceptance.AcceptanceFailure):
                self.validate(plan() + module(cold=False, **fields) + result(), cold)

    def test_invalid_cold_outcomes_are_rejected(self):
        cases = ({"eligible": "0"}, {"object": "miss"}, {"mlir": "not_checked"}, {"tracingRan": "0"},
                 {"fallback": "invalid_object"})
        for fields in cases:
            with self.subTest(fields=fields), self.assertRaises(acceptance.AcceptanceFailure):
                self.validate(plan() + module(**fields) + result())

    def test_cold_must_populate_every_key(self):
        for text in (execution(cold=False), plan(pipelines=(2, 3)) + module()
                     + module(pipeline=3, key=KEY_B, cold=False) + result()):
            with self.subTest(text=text), self.assertRaisesRegex(acceptance.AcceptanceFailure, "did not populate"):
                self.validate(text)

    def test_passed_expected_errors_and_explain_do_not_require_fake_modules(self):
        text = execution() + result("Error:1", kind="expected_error") + result("Explain:1", kind="explain")
        coverage = self.validate(text, queries=Counter({"First:01": 1, "Error:01": 1, "Explain:01": 1}))
        self.assertEqual(coverage.queries.total(), 3)
        self.assertEqual(len(coverage.events), 1)

    def test_expected_errors_do_not_filter_emitted_modules(self):
        text = plan() + module(fallback="invalid_object") + result(kind="expected_error")
        with self.assertRaisesRegex(acceptance.AcceptanceFailure, "fallback"):
            self.validate(text)

    def test_differential_requires_both_branches(self):
        text = plan() + module() + result(differential=True)
        with self.assertRaisesRegex(acceptance.AcceptanceFailure, "First:1-differential"):
            self.validate(text)
        text = plan() + module() + plan("First:1-differential", "other", (3,))
        text += module("First:1-differential", "other", 3, KEY_B) + result(differential=True)
        self.assertEqual(self.validate(text).modules.total(), 2)

    def test_failed_query_telemetry_fails(self):
        with self.assertRaisesRegex(acceptance.AcceptanceFailure, "query failed"):
            self.validate(plan() + module() + result(passed=False))

    def test_invalid_query_telemetry_fails(self):
        cases = ("{", json.dumps({"query": "First:1"}), json.dumps({"query": "First:1", "configuration": {},
                 "passed": "true", "kind": "execute", "differential": False}))
        for payload in cases:
            with self.subTest(payload=payload), self.assertRaises(acceptance.AcceptanceFailure):
                self.validate(plan() + module() + record(acceptance.QUERY_MARKER + payload))

    def test_query_stdout_and_log_coverage_must_match_not_just_count(self):
        with self.assertRaisesRegex(acceptance.AcceptanceFailure, "query result telemetry coverage mismatch"):
            self.validate(execution(), queries=Counter({"Other:01": 1}))

    def test_artifacts_must_match_keys_not_just_counts(self):
        coverage = self.cold()
        for extension in ("manifest", "mlirbc", "o"):
            (self.root / f"{KEY_A}.{extension}").write_bytes(b"artifact")
        acceptance.validate_artifacts(self.root, coverage)
        (self.root / f"{KEY_A}.o").rename(self.root / f"{KEY_B}.o")
        with self.assertRaisesRegex(acceptance.AcceptanceFailure, "artifacts do not match"):
            acceptance.validate_artifacts(self.root, coverage)

    def test_empty_artifact_fails(self):
        coverage = self.cold()
        for extension in ("manifest", "mlirbc", "o"):
            (self.root / f"{KEY_A}.{extension}").touch()
        with self.assertRaises(acceptance.AcceptanceFailure):
            acceptance.validate_artifacts(self.root, coverage)

    def test_successful_process_parses_query_identities(self):
        self.output.write_text(stdout(("First:01", "Second:02 [option=1]")), encoding="utf-8")
        self.assertEqual(acceptance.validate_process("cold", 0, self.output, 1), Counter({"First:01": 1, "Second:02 [option=1]": 1}))

    def test_failed_or_signalled_process_fails_despite_passed_output(self):
        self.output.write_text(stdout(), encoding="utf-8")
        for returncode in (1, -11):
            with self.subTest(returncode=returncode), self.assertRaisesRegex(acceptance.AcceptanceFailure, "process exited"):
                acceptance.validate_process("cold", returncode, self.output, 1)

    def test_failed_query_or_missing_completion_fails_despite_success_exit(self):
        cases = (stdout().replace("PASSED", "FAILED"), stdout().replace("All queries passed.", ""),
                 stdout().replace("First:01", "First:01\n"), stdout(("First:01", "Second:01")).replace("2/2", "1/2"))
        for text in cases:
            with self.subTest(text=text), self.assertRaisesRegex(acceptance.AcceptanceFailure, "did not pass all"):
                self.output.write_text(text, encoding="utf-8")
                acceptance.validate_process("cold", 0, self.output, 1)

    def test_missing_or_incomplete_file_loading_and_empty_queries_fail(self):
        cases = ("", stdout().replace("Loaded 1/1", "Loaded 1/2"), stdout(files=2), stdout(queries=()))
        for text in cases:
            with self.subTest(text=text), self.assertRaisesRegex(acceptance.AcceptanceFailure, "did not load all"):
                self.output.write_text(text, encoding="utf-8")
                acceptance.validate_process("cold", 0, self.output, 1)

    def test_run_phase_checks_process_failure(self):
        with patch.object(acceptance.subprocess, "run") as run:
            run.return_value.returncode = 7
            with contextlib.redirect_stdout(io.StringIO()), self.assertRaisesRegex(acceptance.AcceptanceFailure, "process exited with 7"):
                acceptance.run_phase("cold", 1729, Path("systest"), self.root, self.root, self.root, self.root, "HASH_JOIN", "true")
            command = run.call_args.args[0]
            self.assertIn("--shuffle-seed", command)
            self.assertIn("--worker.enable_compilation_cache=true", command)
            self.assertEqual(command[command.index("--numberConcurrentQueries") + 1], "6")

    def test_curated_corpus_contains_required_coverage_and_real_paths(self):
        suite = Path(__file__).resolve().parents[2]
        with contextlib.redirect_stdout(io.StringIO()):
            corpus = acceptance.prepare_corpus(suite, self.root, False, "disabled")
        self.assertEqual(len(list(corpus.rglob("*.test"))), len(acceptance.CORPUS))
        for required in ("formatter/JSON/BasicJSON.test", "formatter/CSV_OUTPUT/InlinedOutputFormatConfig.test",
                         "formatter/JSON_OUTPUT/BasicJSONInput.test", "datatype/DeeplyNestedVarsizedEquality.test",
                         "regression/VariableSizedLiteralBytes.test", "function/varsized/CharLength.test"):
            self.assertIn(required, acceptance.CORPUS)

    def test_full_suite_is_not_limited_to_curated_files(self):
        suite = self.root / "suite"
        (suite / "extra").mkdir(parents=True)
        (suite / "extra" / "Additional.test").write_text("SELECT 1;\n", encoding="utf-8")
        (suite / "extra" / "Disabled.test.disabled").write_text("SELECT 2;\n", encoding="utf-8")
        with contextlib.redirect_stdout(io.StringIO()):
            corpus = acceptance.prepare_corpus(suite, self.root, True, "disabled")
        self.assertTrue((corpus / "extra/Additional.test").is_file())
        self.assertEqual(len(list(corpus.rglob("*.test"))), 1)

    def test_enabled_inference_missing_dependency_fails_not_skips(self):
        with patch.object(acceptance.shutil, "which", return_value=None), self.assertRaisesRegex(acceptance.AcceptanceFailure, "ovc"):
            acceptance.prepare_corpus(self.root, self.root, False, "enabled")

    def test_inference_inclusion_requires_explicit_mode(self):
        suite = Path(__file__).resolve().parents[2]
        with patch.object(acceptance.shutil, "which", return_value="/usr/bin/ovc"), contextlib.redirect_stdout(io.StringIO()):
            corpus = acceptance.prepare_corpus(suite, self.root, False, "enabled")
        for path in acceptance.INFERENCE_CORPUS:
            self.assertTrue((corpus / path).is_file())

    def test_full_suite_inference_disable_is_explicit(self):
        suite = self.root / "suite"
        (suite / "inference").mkdir(parents=True)
        (suite / "Additional.test").write_text("SELECT 1;\n", encoding="utf-8")
        (suite / "inference" / "Model.test").write_text("SELECT 2;\n", encoding="utf-8")
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            corpus = acceptance.prepare_corpus(suite, self.root, True, "disabled")
        self.assertIn("Inference explicitly disabled: inference/Model.test", output.getvalue())
        self.assertFalse((corpus / "inference/Model.test").exists())

    def test_seed_validation_and_full_suite_option(self):
        argv = ["--systest", "systest", "--suite", ".", "--data", ".", "--topology", "topology.yaml", "--run-root", "run",
                "--inference", "disabled"]
        arguments = acceptance.parse_arguments(argv + ["--full-suite"])
        self.assertTrue(arguments.full_suite)
        self.assertGreater(len(arguments.seeds), 2)
        for seeds in (("1",), ("1", "1"), ("-1", "2"), (str(2**32), "2")):
            with self.subTest(seeds=seeds), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                acceptance.parse_arguments(argv + ["--seeds", *seeds])


if __name__ == "__main__":
    unittest.main()
