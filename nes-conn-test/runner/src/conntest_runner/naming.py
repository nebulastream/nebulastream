# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Per-test resource names, such as a topic or a table.

They must be unique across tests and across parallel workers that share one service,
so the framework builds them, not the loader.
"""

from __future__ import annotations

import hashlib
import re

_NON_ALNUM = re.compile(r"[^a-z0-9]+")


def unique_token(test_id: str, scenario: str, worker_id: str, *, max_len: int = 50) -> str:
    """A truncated token ends with a hash of the full input, so distinct inputs stay distinct.

    ``max_len`` stays below the 63-character limit of SQL identifiers.
    """
    raw = "_".join(part for part in (worker_id, scenario, test_id) if part)
    safe = _NON_ALNUM.sub("_", raw.lower()).strip("_")
    if len(safe) <= max_len:
        return safe
    digest = hashlib.sha1(raw.encode("utf-8"), usedforsecurity=False).hexdigest()[:8]
    return safe[: max_len - 9].rstrip("_") + "_" + digest
