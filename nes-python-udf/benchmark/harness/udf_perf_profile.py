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
"""Profiles one UDFBench UDF as an INLINE UDF with perf: Codon or Numba, its query of benchmark/UdfBenchCodonNumba.test on the
x10 data, one worker thread, with nautilus_perf so that perf resolves the generated code (perf inject --jit).

Usage: udf_perf_profile.py --udf NAME --compiler codon|numba --out DIR. Writes DIR/<udf>-<compiler>.report.txt (the hottest
symbols of the worker thread by DSO) and DIR/<udf>-<compiler>.annotate.txt (the machine code of the hottest generated function), and keeps the perf
data. Numba needs NES_NUMBA_PYTHON.
"""

import argparse
import os
import re
import subprocess

HARNESS = os.path.dirname(os.path.abspath(__file__))
REPOSITORY = os.path.abspath(os.path.join(HARNESS, "../../.."))
SOURCE_TEST = os.path.join(REPOSITORY, "nes-systests/benchmark/UdfBenchCodonNumba.test")


def single_query_test(udf, compiler):
    """The sources, the UDF's CREATE FUNCTION and its x10 query of UdfBenchCodonNumba.test, as a test in the repository."""
    source = open(SOURCE_TEST).read()
    header = source[source.index("CREATE LOGICAL SOURCE"):source.index("CREATE FUNCTION")]
    create = re.search(rf"^CREATE FUNCTION {udf}_(inline|adapted) .*$", source, flags=re.MULTILINE).group(0)
    query = re.search(rf"^SELECT .*{udf}_(inline|adapted)\(.*FROM \w+_x10 .*$", source, flags=re.MULTILINE).group(0)
    if compiler == "numba":
        create = create.replace("EXECUTION INLINE ENTRYPOINT", "EXECUTION INLINE COMPILER NUMBA ENTRYPOINT")
    path = os.path.join(REPOSITORY, "nes-systests/benchmark", f"TmpProfile_{udf}_{compiler}.test")
    with open(path, "w") as test:
        test.write(f"# name: benchmark/TmpProfile_{udf}_{compiler}.test\n# description: temporary perf profile\n# groups: [benchmark]\n\n")
        test.write(header + create + "\n\n" + query + "\n----\n")
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--udf", required=True)
    parser.add_argument("--compiler", choices=["codon", "numba"], required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--build", default=os.path.join(REPOSITORY, "cmake-build-relwithdebinfo-libstdcxx-none"))
    arguments = parser.parse_args()
    os.makedirs(arguments.out, exist_ok=True)
    name = f"{arguments.udf}-{arguments.compiler}"
    work = os.path.join(arguments.out, name + "-work")
    data = os.path.join(arguments.out, name + ".perf.data")
    test = single_query_test(arguments.udf, arguments.compiler)
    try:
        subprocess.run(["perf", "record", "-k", "1", "-F", "2999", "-o", data, "--",
                        os.path.join(arguments.build, "nes-systests/systest/systest"), "-t", test, "--data", "/dev/shm/streamudfbench-data",
                        "--workingDir", work, "--query-timeout", "1200", "-c", os.path.join(HARNESS, "single-node.yaml"), "--",
                        "--worker.query_engine.number_of_worker_threads=1", "--worker.default_query_execution.nautilus_perf=true"],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    finally:
        os.remove(test)
    jitted = data + ".jit"
    subprocess.run(["perf", "inject", "--jit", "-i", data, "-o", jitted], check=True)
    # Only the worker thread: the frontend's compile at CREATE FUNCTION runs in the same process.
    report = subprocess.run(["perf", "report", "--stdio", "--no-children", "-i", jitted, "--comms", "WorkerThread-0", "--sort", "dso,sym",
                             "--percent-limit", "0.3"],
                            capture_output=True, text=True).stdout
    open(os.path.join(arguments.out, name + ".report.txt"), "w").write(report)
    hottest = next((line for line in report.splitlines() if "jitted-" in line), None)
    if hottest:
        symbol = hottest.split("] ")[-1].strip()
        annotate = subprocess.run(["perf", "annotate", "--stdio", "-i", jitted, "--comms", "WorkerThread-0", "-s", symbol],
                                  capture_output=True, text=True).stdout
        open(os.path.join(arguments.out, name + ".annotate.txt"), "w").write(annotate)
    print(f"{name}: {os.path.join(arguments.out, name + '.report.txt')}")


if __name__ == "__main__":
    main()
