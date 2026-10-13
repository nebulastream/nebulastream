# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""naming.py — central, collision-free target-name generation.

A per-test resource name (e.g., a broker topic or table name) must be unique
across both tests and parallel pytest-xdist workers sharing one service, or
two runs collide on the same broker topic / DB table. That uniqueness rule is
framework policy, not connector policy — so the conformance test builds the
target as ``"conntest_" + unique_token(...)`` itself; a loader never names its
own per-test resource.
"""

from __future__ import annotations

import hashlib
import re

_NON_ALNUM = re.compile(r"[^a-z0-9]+")


def unique_token(test_id: str, scenario: str, worker_id: str, *, max_len: int = 50) -> str:
    """A collision-free ``[a-z0-9_]`` token identifying one test run.

    Uniqueness across parallel workers comes from ``worker_id``; across tests
    from ``test_id`` (the pytest node id). The result is lowercased and reduced
    to ``[a-z0-9_]`. When the sanitized form would exceed ``max_len``
    (SQL identifiers cap at 63), it is truncated and a hash of the full input is
    appended so distinct inputs stay distinct.
    """
    raw = "_".join(part for part in (worker_id, scenario, test_id) if part)
    safe = _NON_ALNUM.sub("_", raw.lower()).strip("_")
    if len(safe) <= max_len:
        return safe
    digest = hashlib.sha1(raw.encode("utf-8"), usedforsecurity=False).hexdigest()[:8]
    return safe[: max_len - 9].rstrip("_") + "_" + digest
