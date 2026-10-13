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

    {"ok":true,"phase":"fill","eos":false}
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
from functools import cache
from pathlib import Path

# ── error-code table, derived from the single source of truth ──────────
#
# ExceptionDefinitions.inc holds ~75 clean ``EXCEPTION(Name, code, "msg")``
# entries. Parsing it (rather than mirroring a subset by hand) means the
# Python table can never drift from the C++ enum: a renamed or retired
# code simply stops resolving, failing at scenario-parse time loudly.
#
# Parsed lazily and cached, NOT at import: importing this module touches no
# filesystem, so it stays testable in isolation and a missing/moved .inc fails at
# the first code resolution — with the path that failed — instead of as an opaque
# ImportError. ``@cache`` keeps it to a single parse per process, exactly as the
# old module-level dict did.
_EXCEPTION_RE = re.compile(r"EXCEPTION\(\s*([A-Za-z_]\w*)\s*,\s*(\d+)\s*,")


def _inc_path() -> Path:
    """Path to ExceptionDefinitions.inc (the C++ enum — the single source of truth).

    protocol.py lives at ``nes-conn-test/runner/src/conntest_runner/protocol.py``,
    so the repo root is four parents up and the table sits under ``nes-common/``.
    """
    root = Path(__file__).resolve().parents[4]
    return root / "nes-common" / "include" / "ExceptionDefinitions.inc"


@cache
def _name_to_code() -> dict[str, int]:
    """``{exception name: numeric code}``, parsed once from the .inc table."""
    inc = _inc_path()
    try:
        text = inc.read_text(encoding="utf-8")
    except OSError as e:
        msg = (
            f"cannot read the exception-code table at {inc}; "
            f"has protocol.py moved relative to the repo root?"
        )
        raise RuntimeError(msg) from e
    table = {m.group(1): int(m.group(2)) for m in _EXCEPTION_RE.finditer(text)}
    if not table:
        raise RuntimeError(f"no EXCEPTION(...) entries parsed from {inc}")
    return table


@cache
def _code_to_name() -> dict[int, str]:
    """``{numeric code: exception name}`` — the inverse of :func:`_name_to_code`."""
    return {v: k for k, v in _name_to_code().items()}


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
        if code not in _code_to_name():
            raise UnknownErrorCode(f"error code {code} is not in ExceptionDefinitions.inc")
        return code
    name = str(token)
    name_to_code = _name_to_code()
    if name not in name_to_code:
        raise UnknownErrorCode(f"error name {name!r} is not in ExceptionDefinitions.inc")
    return name_to_code[name]


def name_for(code: int) -> str:
    """Symbolic name for a code, or ``"<code>"`` if unknown (for messages)."""
    return _code_to_name().get(code, str(code))


def describe(code: int) -> str:
    """Human-readable ``4002 (CannotOpenSource)`` for failure messages."""
    return f"{code} ({name_for(code)})"


# ── reply types ────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Got:
    """An ``ok:true`` reply from a data step (FILL) or a bare step.

    Source correctness is verified entirely from the ``--observed-path`` JSONL,
    not from reply counters, so a source FILL reply carries no row/byte/sha
    numbers — only ``eos`` (whether the source signalled end-of-stream).
    """

    eos: bool = False
    # Sink WRITE: records actually written in this step (buffer-aligned).
    units_written: int = 0
    # Sink START: number of buffers the harness materialized from the input.
    n_buffers: int = 0
    # BIND: the engine's native row width (fixed region incl. null flags). The
    # runner uses it to size "N buffers" worth of rows; 0 when not reported.
    row_width: int = 0


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
            eos=bool(j.get("eos", False)),
            units_written=int(j.get("units_written", 0)),
            n_buffers=int(j.get("n_buffers", 0)),
            row_width=int(j.get("row_width", 0)),
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
