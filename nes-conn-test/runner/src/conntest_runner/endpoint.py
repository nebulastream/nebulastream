# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""endpoint.py — parse the ``host:port`` endpoint the framework hands a loader.

The framework owns topology: it derives the connector endpoint from the plugin's
compose.yaml and passes it to the loader (in ``__init__`` and ``config_overrides``).
Splitting ``"host:port"`` into ``(host, port)`` is therefore *framework policy*,
not connector knowledge — every loader needs the identical parse + validation, so
it lives here once instead of being copied into each one.

This does not bend ``plugin-isolation`` (core_design_principles.md): that invariant forbids
shared *connector logic*, not a shared *framework helper a loader calls* — the
same status as ``SourceModel`` or ``unique_token``. Endpoint parsing is squarely
in the framework's "naming / topology / config" bucket, not in "what this
connector knows about its backend".
"""

from __future__ import annotations


def split_endpoint(endpoint: str) -> tuple[str, int]:
    """Split a framework-supplied ``"host:port"`` endpoint into ``(host, port)``.

    Splits on the last ``:`` so the rightmost colon is the port separator. Raises
    ``ValueError`` with the offending value if either side is empty or the port is
    not an integer — the message a connector author sees if a bad endpoint reaches
    their loader.
    """
    host, sep, port = endpoint.rpartition(":")
    if not sep or not host or not port:
        raise ValueError(f"endpoint must be 'host:port', got {endpoint!r}")
    try:
        return host, int(port)
    except ValueError:
        raise ValueError(f"endpoint port must be an integer, got {endpoint!r}") from None
