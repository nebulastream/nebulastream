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
"""Adapted versions of the UDFBench scalar UDFs that NES does not accept as they are for EXECUTION INLINE (Codon), and Numba
with nes_numba_ext: CREATE FUNCTION ... ENTRYPOINT 'StreamUDFBench.udfbench_scalar_inline_adapted.<name>'.

Each function gives the same result as the UDFBench file in scalar/ for the inputs of StreamUDFBenchScalarUdfs.test, with
these changes:

- addnoise: imports random at module level, not inside the function (NES rejects an import inside an INLINE function).

The arguments are declared NOT NULL, so None does not reach these functions (as in UDFBench, which runs them in DuckDB).
"""

import random


def addnoise(val):
    if not val:
        return None
    return float(val) + random.gauss(0, 2)
