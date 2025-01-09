import json
import time
import os
import logging
from typing import List, Any

log = logging.getLogger(__name__)

from benchadapt import BenchmarkResult
from benchadapt.adapters import CallableAdapter, BenchmarkAdapter, GoogleBenchmarkAdapter
from benchrun import Benchmark, BenchmarkList, Iteration, CaseList

DEFAULT_RESULT_DIR = "/tmp/nebulastream-public/cmake-build-debug-nes/nes-systests/result"
DEFAULT_EXECUTABLE_PATH = "/tmp/nebulastream-public/cmake-build-debug-nes/nes-systests/systest/systest"
DEFAULT_TEST_LOCATION = "/tmp/nebulastream-public/nes-systests/systest/../"

class SystestAdapter(BenchmarkAdapter):
    systest_result_dir: str
    result_fields_override: dict[str, Any] = None,
    result_fields_append: dict[str, Any] = None,


    def __init__(
            self,
            systest_result_dir: str,
            result_fields_override: dict[str, Any] = None,
            result_fields_append: dict[str, Any] = None,
    ) -> None:
        super().__init__(
            command=[],
            result_fields_append=result_fields_append,
            result_fields_override=result_fields_override) # need to call super to set stuff like machine info etc.
        self.systest_result_dir=systest_result_dir
        self.results = self.transform_results()

    def _transform_results(self) -> List[BenchmarkResult]:
        with open(self.systest_result_dir + "/BenchmarkResults.json", "r") as f:
            raw_results = json.load(f)

        benchmarkResults = []

        for result in raw_results:
            benchmarkResults.append(BenchmarkResult(
                #run_tags={"name": result["query name"]},
                stats={
                    "data": [result["time"]],
                    "unit": "ns"
                },
                context={"benchmark_language": "systest"},
                tags={"name": result["query name"]},
            ))

        return benchmarkResults

#os.environ["CONBENCH_URL"] = "https://bench.nebula.stream"
#os.environ["CONBENCH_EMAIL"] = "user@example.com"
#os.environ["CONBENCH_PASSWORD"] = "password"
#os.environ["CONBENCH_PROJECT_REPOSITORY"] = "https://github.com/conbench/conbench"
#os.environ["CONBENCH_PROJECT_COMMIT"] = "d7009ab93f1f4b79e3f95692a87a002da0465dd0"
#os.environ["CONBENCH_RUN_REASON"] = "Testing"


systest_adapter = SystestAdapter(
    systest_result_dir="./build/nes-systests/result/",
    result_fields_override={"run_reason": os.getenv("CONBENCH_RUN_REASON")},
)

systest_adapter.post_results()
