# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""protocol.py — the stepper command/reply wire protocol.

The harness runs a command/reply session over a unix-domain control
socket (see ``conntest_runner.harness``). Each command produces exactly
one JSON reply line:

    {"ok":true,"phase":"fill","count":1024,"bytes":1024,"sha":"…","eos":false}
    {"ok":false,"phase":"open","origin":"connector","code":4002,
     "name":"CannotOpenSource","message":"…"}

This module owns:

  * the **error-code table**, parsed from the single source of truth
    (``nes-common/include/ExceptionDefinitions.inc``) so it can never
    drift from the C++ enum — there is no hand-maintained mirror;
  * the **reply types** (`Got`) and the two error kinds raised when a
    command replies ``ok:false``: `ConnectorError` (origin=connector — a
    real connector outcome, which a scenario may *expect*) and
    `HarnessError` (origin=harness — infra failure / step timeout, which
    a scenario can never expect).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

# ── error-code table, derived from the single source of truth ──────────
#
# ExceptionDefinitions.inc holds ~75 clean ``EXCEPTION(Name, code, "msg")``
# entries. Parsing it (rather than mirroring a subset by hand) means the
# Python table can never drift from the C++ enum: a renamed or retired
# code simply stops resolving, failing scenario-parse time loudly.
_INC = Path(__file__).resolve().parents[4] / "nes-common" / "include" / "ExceptionDefinitions.inc"
_EXCEPTION_RE = re.compile(r"EXCEPTION\(\s*([A-Za-z_]\w*)\s*,\s*(\d+)\s*,")

NAME_TO_CODE: dict[str, int] = {
    m.group(1): int(m.group(2)) for m in _EXCEPTION_RE.finditer(_INC.read_text(encoding="utf-8"))
}
CODE_TO_NAME: dict[int, str] = {v: k for k, v in NAME_TO_CODE.items()}


class UnknownErrorCode(ValueError):  # noqa: N818  # intentional name; an "Error" suffix would read oddly
    """A scenario named an error code/name absent from the .inc table."""


def resolve_code(token: str | int) -> int:
    """Resolve a numeric or symbolic error token to its numeric code.

    Validates it against the .inc table so a typo or retired code fails at
    scenario-parse time rather than silently never matching. Accepts a
    numeric (``4002``) or symbolic (``CannotOpenSource``) token.
    """
    if isinstance(token, int) or token.isdigit():
        code = int(token)
        if code not in CODE_TO_NAME:
            raise UnknownErrorCode(f"error code {code} is not in ExceptionDefinitions.inc")
        return code
    name = str(token)
    if name not in NAME_TO_CODE:
        raise UnknownErrorCode(f"error name {name!r} is not in ExceptionDefinitions.inc")
    return NAME_TO_CODE[name]


def name_for(code: int) -> str:
    """Symbolic name for a code, or ``"<code>"`` if unknown (for messages)."""
    return CODE_TO_NAME.get(code, str(code))


def describe(code: int) -> str:
    """Human-readable ``4002 (CannotOpenSource)`` for failure messages."""
    return f"{code} ({name_for(code)})"


# ── reply types ────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Got:
    """An ``ok:true`` reply from a data step (FILL/DRAIN) or a bare step.

    ``count`` is in the source's native unit (bytes for byte sources,
    tuples for NATIVE-formatter sources); ``bytes`` is the observed byte
    count; ``sha`` is the SHA-256 over the observed bytes; ``eos`` is
    whether the connector signalled end-of-stream.
    """

    count: int = 0
    bytes: int = 0
    sha: str = ""
    eos: bool = False


class ConnectorError(Exception):
    """An ``ok:false`` reply with origin=connector — a real connector outcome.

    E.g. CannotOpenSource on a dead endpoint. A scenario may *expect* one of
    these by code.
    """

    def __init__(self, *, code: int, name: str, phase: str, message: str) -> None:
        self.code = code
        self.name = name
        self.phase = phase
        self.message = message
        super().__init__(f"{describe(code)}@{phase}: {message}")


class HarnessError(Exception):
    """An ``ok:false`` reply with origin=harness — an infra failure or timeout.

    Never an expected outcome: a scenario that hits one fails regardless of its
    declared expectation (you never "expect" the test infra to break, and a
    timeout is always a failure — design §5).
    """

    def __init__(self, *, name: str, phase: str, message: str, code: int = 0) -> None:
        self.name = name
        self.phase = phase
        self.message = message
        self.code = code
        super().__init__(f"harness {name}@{phase}: {message}")


def read_reply(line: str) -> Got:
    """Parse one JSON reply line.

    Returns `Got` on ``ok:true``; raises `ConnectorError` / `HarnessError`
    on ``ok:false`` by origin.
    """
    j = json.loads(line)
    if j.get("ok"):
        return Got(
            count=int(j.get("count", 0)),
            bytes=int(j.get("bytes", 0)),
            sha=str(j.get("sha", "")),
            eos=bool(j.get("eos", False)),
        )
    origin = j.get("origin")
    phase = str(j.get("phase", ""))
    message = str(j.get("message", ""))
    if origin == "connector":
        raise ConnectorError(
            code=int(j.get("code", 0)),
            name=str(j.get("name", "")),
            phase=phase,
            message=message,
        )
    raise HarnessError(
        name=str(j.get("name", "")),
        phase=phase,
        message=message,
        code=int(j.get("code", 0)),
    )
