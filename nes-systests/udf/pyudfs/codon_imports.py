# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""UDFs for CodonFunctionLocalImport.test. EXECUTION INLINE rejects a UDF that uses a function with an import statement in
its body. This module itself imports only at file level, and imports codon_imports_lazy, which has such a function."""

import math

import codon_imports_file_level
import codon_imports_lazy


def import_inside(x: int) -> int:
    import random

    return x + random.randint(0, 0)


def import_in_branch(x: int) -> int:
    if x > 100:
        import random

        return x + random.randint(0, 0)
    else:
        x = x + 1
    return x


def import_inside_and_at_file_level(x: int) -> int:
    import math

    return x + int(math.floor(0.5))


def file_level_import(x: int) -> int:
    return x + int(math.floor(0.5))


def helper_with_file_level_import(x: int) -> int:
    return codon_imports_file_level.plus_one(x)


def helper_with_import_inside(x: int) -> int:
    return codon_imports_lazy.plus_noise(x)


def no_import(s: str) -> int:
    return len(s.split(","))
