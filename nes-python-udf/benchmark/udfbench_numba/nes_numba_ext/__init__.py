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
"""Numba extensions that let the unchanged UDFBench scalar UDFs compile in nopython mode, as the NES Codon plugins do for
Codon: json for arrays of strings, re for literal patterns, int(str), and collections.Counter with heapq.nlargest."""

from nes_numba_ext import builtins_ext, collections_ext, json_ext, json_value_ext, re_ext  # noqa: F401
from nes_numba_ext.frontend import njit  # noqa: F401
