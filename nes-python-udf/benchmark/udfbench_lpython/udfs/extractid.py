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
"""UDFBench extractid for LPython. Changes: LPython types, no `self`, no try/except (an IndexError gives None, here an
explicit length check), None is "NULL"."""
from lpython import ccallable


def extractid(project: str) -> str:
    if project != "":
        parts: list[str] = project.split("::")
        if len(parts) > 2:
            return parts[2]
        return "NULL"
    else:
        return "NULL"


@ccallable
def udf_entry(project: str) -> str:
    copy: str = project  # LPython miscompiles a @ccallable function that passes its str argument on
    return extractid(copy)
