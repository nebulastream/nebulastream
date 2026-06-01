# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Framework self-test for `conntest_runner.protocol` (no docker).

Covers the two load-bearing pieces of the wire protocol: the
.inc-derived error-code table round-trips, and reply parsing produces a
`Got` on ok:true / raises the right error kind by origin on ok:false.
"""

from __future__ import annotations

import json

import pytest

from conntest_runner.protocol import (
    CODE_TO_NAME,
    NAME_TO_CODE,
    ConnectorError,
    Got,
    HarnessError,
    UnknownErrorCode,
    describe,
    read_reply,
    resolve_code,
)


def test_code_table_parsed_from_inc() -> None:
    # A representative, stable pair must be present and round-trip.
    assert NAME_TO_CODE["CannotOpenSource"] == 4002
    assert CODE_TO_NAME[4002] == "CannotOpenSource"
    assert NAME_TO_CODE["InvalidConfigParameter"] == 1000
    # The table is non-trivial (the .inc has ~75 entries).
    assert len(NAME_TO_CODE) > 20


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
    assert got == Got(count=1024, bytes=1024, sha="abc", eos=False)


def test_read_reply_ok_validate_defaults() -> None:
    got = read_reply(json.dumps({"ok": True, "phase": "validate"}))
    assert got == Got(count=0, bytes=0, sha="", eos=False)


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
