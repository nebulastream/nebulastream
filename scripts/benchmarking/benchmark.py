import json
import subprocess
import time
import os
import logging
from typing import List, Any

log = logging.getLogger(__name__)

from benchadapt import BenchmarkResult
from benchadapt.adapters import CallableAdapter, BenchmarkAdapter, GoogleBenchmarkAdapter
from benchrun import Benchmark, BenchmarkList, Iteration, CaseList

import argparse

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


class CompileTimeBenchmark(BenchmarkAdapter):
    """
    Adapter that runs the build and times it
    """
    def __init__(
            self,
            result_fields_override: dict[str, Any] = None,
            result_fields_append: dict[str, Any] = None,
    ):
        super().__init__(
            command=[],
            result_fields_append=result_fields_append,
            result_fields_override=result_fields_override
        )

    def run(self, params: List[str] = None) -> List[BenchmarkResult]:
        if os.path.isdir("build"):
            os.rmdir("build")

        log.info("Creating build directory")
        #subprocess.run(["cmake", "-GNinja", "-B build"])
        log.info("Starting build")
        start_time = time.time()
        #subprocess.run(["cmake", "--build", "build", "-j", "--", "--quiet"])
        end_time = time.time()
        elapsed_time = end_time - start_time
        log.info(f"Build finished. Took: {elapsed_time:.2f} seconds")
        self.results = [BenchmarkResult(
            stats={
                "data": [elapsed_time],
                "unit": "s",
            },
            context={"benchmark_language": "CMake"}, # required else page won't load
            tags={"name": "NebulaStream compile time"},
        )]
        self.results = self.transform_results()
        return self.results
    def _transform_results(self) -> List[BenchmarkResult]:
        return self.results

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="NebulastreamBenchmarker",
        description="A Utility to benchmark Nebulastream using various tools / upload benchmark result"
    )
    parser.add_argument("--systest", action="store_true", help="upload systest results")
    parser.add_argument("--compile-time", action="store_true", help="Benchmark Nebulastream compile time and upload results")

    args = parser.parse_args()

    if args.systest:

        systest_adapter = SystestAdapter(
            systest_result_dir="./build/nes-systests/result/",
            result_fields_override={"run_reason": os.getenv("CONBENCH_RUN_REASON")},
        )

        systest_adapter.post_results()
    elif args.compile_time:
        compile_time_adapter = CompileTimeBenchmark(
            result_fields_override={"run_reason": os.getenv("CONBENCH_RUN_REASON")},
        )
        compile_time_adapter.run()
        compile_time_adapter.post_results()
    else:
        log.fatal("No operation specified!")