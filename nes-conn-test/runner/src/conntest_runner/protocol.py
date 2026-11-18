# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The JSON reply protocol of the harness control socket, and the error-code table.

Every command gets one reply line, for example
``{"ok":false,"phase":"open","origin":"connector","code":4002,"name":"CannotOpenSource","message":"..."}``.
The error codes are parsed from ``nes-common/include/ExceptionDefinitions.inc``, so they cannot drift from the C++ enum.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from functools import cache
from pathlib import Path

# The table is parsed on first use, not at import, so importing this module touches no file,
# and a missing .inc fails with its path instead of as an ImportError.
_EXCEPTION_RE = re.compile(r"EXCEPTION\(\s*([A-Za-z_]\w*)\s*,\s*(\d+)\s*,")


def _inc_path() -> Path:
    root = Path(__file__).resolve().parents[4]
    return root / "nes-common" / "include" / "ExceptionDefinitions.inc"


@cache
def _name_to_code() -> dict[str, int]:
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
    return {v: k for k, v in _name_to_code().items()}


class UnknownErrorCode(ValueError):  # noqa: N818  # intentional name; an "Error" suffix would read oddly
    """A scenario named an error code/name absent from the .inc table."""


def resolve_code(token: str | int) -> int:
    """Accepts ``4002`` or ``CannotOpenSource``.

    A typo or a retired code fails here, when the scenarios are parsed, instead of never matching.
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
    return _code_to_name().get(code, str(code))


def describe(code: int) -> str:
    return f"{code} ({name_for(code)})"


@dataclass(frozen=True)
class Got:
    """An ``ok:true`` reply.

    A FILL reply has no row counts, because the data is checked from the ``--observed-path`` file.
    ``partial_read_ms`` lists how long each read that returned a partly filled buffer took during a FILL.
    """

    eos: bool = False
    partial_read_ms: tuple[int, ...] = ()


class ConnectorError(Exception):
    """An ``ok:false`` reply with origin connector, which a scenario may expect."""

    def __init__(self, *, code: int, name: str, phase: str, message: str) -> None:
        self.code = code
        self.name = name
        self.phase = phase
        self.message = message
        super().__init__(f"{describe(code)}@{phase}: {message}")


class HarnessError(Exception):
    """An ``ok:false`` reply with origin harness, from an infra failure or a step timeout.

    It always fails the scenario, whatever the declared expectation.
    """

    def __init__(self, *, name: str, phase: str, message: str, code: int = 0) -> None:
        self.name = name
        self.phase = phase
        self.message = message
        self.code = code
        super().__init__(f"harness {name}@{phase}: {message}")


def read_reply(line: str) -> Got:
    j = json.loads(line)
    if j.get("ok"):
        return Got(eos=bool(j.get("eos", False)), partial_read_ms=tuple(int(ms) for ms in j.get("partial_read_ms", [])))
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
