# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for the error-code table and the parsing of harness replies."""

from __future__ import annotations

import json

import pytest

from conntest_runner import protocol
from conntest_runner.protocol import (
    ConnectorError,
    Got,
    HarnessError,
    UnknownErrorCode,
    _code_to_name,
    _name_to_code,
    describe,
    read_reply,
    resolve_code,
)


def test_code_table_parsed_from_inc() -> None:
    assert _name_to_code()["CannotOpenSource"] == 4002
    assert _code_to_name()[4002] == "CannotOpenSource"
    assert _name_to_code()["InvalidConfigParameter"] == 1000
    assert len(_name_to_code()) > 20


def test_code_table_is_cached() -> None:
    assert _name_to_code() is _name_to_code()


def test_missing_inc_fails_at_use_with_clear_message(monkeypatch, tmp_path) -> None:
    # The table is read lazily, so a missing .inc must fail at first use with its path,
    # not as an ImportError when the module is imported.
    missing = tmp_path / "nope" / "ExceptionDefinitions.inc"
    monkeypatch.setattr(protocol, "_inc_path", lambda: missing)
    protocol._name_to_code.cache_clear()
    protocol._code_to_name.cache_clear()
    try:
        with pytest.raises(RuntimeError, match="cannot read the exception-code table"):
            resolve_code("CannotOpenSource")
    finally:
        # Later tests must parse the real file again.
        protocol._name_to_code.cache_clear()
        protocol._code_to_name.cache_clear()


def test_resolve_code_numeric_and_symbolic() -> None:
    assert resolve_code("4002") == 4002
    assert resolve_code(4002) == 4002
    assert resolve_code("CannotOpenSource") == 4002
    assert resolve_code("InvalidConfigParameter") == 1000


def test_resolve_code_rejects_unknown() -> None:
    with pytest.raises(UnknownErrorCode):
        resolve_code("Bogus")
    with pytest.raises(UnknownErrorCode):
        resolve_code("99999")


def test_describe() -> None:
    assert describe(4002) == "4002 (CannotOpenSource)"


def test_read_reply_ok_fill() -> None:
    # Unknown keys, here fields that the harness no longer emits, are ignored.
    got = read_reply(
        json.dumps(
            {
                "ok": True,
                "phase": "fill",
                "count": 1024,
                "bytes": 1024,
                "sha": "abc",
                "eos": False,
            }
        )
    )
    assert got == Got(eos=False)


def test_read_reply_ok_bind_defaults() -> None:
    got = read_reply(json.dumps({"ok": True, "phase": "bind"}))
    assert got == Got(eos=False)


def test_read_reply_connector_error() -> None:
    with pytest.raises(ConnectorError) as ei:
        read_reply(
            json.dumps(
                {
                    "ok": False,
                    "phase": "open",
                    "origin": "connector",
                    "code": 4002,
                    "name": "CannotOpenSource",
                    "message": "boom",
                }
            )
        )
    assert ei.value.code == 4002
    assert ei.value.name == "CannotOpenSource"
    assert ei.value.phase == "open"


def test_read_reply_harness_error() -> None:
    with pytest.raises(HarnessError) as ei:
        read_reply(
            json.dumps(
                {
                    "ok": False,
                    "phase": "fill",
                    "origin": "harness",
                    "code": 0,
                    "name": "StepTimeout",
                    "message": "timed out",
                }
            )
        )
    assert ei.value.name == "StepTimeout"
    assert ei.value.phase == "fill"
