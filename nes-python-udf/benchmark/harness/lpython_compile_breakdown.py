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
"""Compile times of the UDFs of the Codon compile breakdown with LPython (lpython_udfs/, ported with type annotations).

Usage: lpython_compile_breakdown.py <path to the lpython binary>. Compiles each UDF to an executable three times, without and
with --fast, and prints one JSON line per compile: the wall time of the whole command (including the link) and LPython's
--time-report phases. A UDF that LPython rejects is reported with its error.
"""

import json
import pathlib
import re
import subprocess
import sys
import tempfile
import time

lpython = sys.argv[1]
udfs = pathlib.Path(__file__).parent / "lpython_udfs"
for name in ["double", "title", "first_number", "score"]:
    for options in [[], ["--fast"]]:
        for run in range(3):
            with tempfile.TemporaryDirectory() as directory:
                start = time.perf_counter()
                result = subprocess.run(
                    [lpython, "--no-color", "--time-report", *options, "-o", f"{directory}/udf", str(udfs / f"{name}.py")],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                wall = (time.perf_counter() - start) * 1000
            output = result.stdout + result.stderr
            phases = {phase: float(ms) for phase, ms in re.findall(r"^([A-Za-z ]+): ([0-9.]+)ms", output, flags=re.MULTILINE)}
            error = next((line for line in output.splitlines() if "error" in line), None) if result.returncode != 0 else None
            print(json.dumps({"udf": name, "options": options, "run": run, "wall_ms": round(wall, 1), "phases_ms": phases, "error": error}))
