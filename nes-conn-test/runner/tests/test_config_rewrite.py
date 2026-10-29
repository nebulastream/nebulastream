# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for replacing values in a config's SET clause, whatever literal it holds and however the key is quoted."""

from __future__ import annotations

import pytest

from conntest_runner.config_rewrite import apply_overrides

_CONFIG = (
    "CREATE PHYSICAL SOURCE FOR s TYPE TCP SET (\n"
    "    'localhost' AS \"SOURCE\".SOCKET_HOST,\n"
    "    9000        AS \"SOURCE\".\"SOCKET_PORT\",\n"
    "    100         AS SOURCE.FLUSH_INTERVAL_MS\n"
    ");\n"
)


def test_replaces_each_named_key_and_keeps_the_rest() -> None:
    out = apply_overrides(_CONFIG, {"SOCKET_HOST": "relay", "SOCKET_PORT": 9100})
    assert "'relay' AS \"SOURCE\".SOCKET_HOST" in out
    assert "9100        AS \"SOURCE\".\"SOCKET_PORT\"" in out
    assert "100         AS SOURCE.FLUSH_INTERVAL_MS" in out


def test_value_type_decides_the_literal() -> None:
    config = "SET ('localhost' AS \"SOURCE\".HOST, 9000 AS \"SOURCE\".PORT)"
    out = apply_overrides(config, {"HOST": "9100", "PORT": 9100})
    assert out == "SET ('9100' AS \"SOURCE\".HOST, 9100 AS \"SOURCE\".PORT)"


def test_key_does_not_match_a_longer_key_it_prefixes() -> None:
    config = "SET ('a' AS \"SOURCE\".PORT_RANGE, 'b' AS \"SOURCE\".PORT)"
    assert apply_overrides(config, {"PORT": "x"}) == "SET ('a' AS \"SOURCE\".PORT_RANGE, 'x' AS \"SOURCE\".PORT)"


def test_value_is_inserted_literally() -> None:
    out = apply_overrides(_CONFIG, {"SOCKET_HOST": r"a\1b"})
    assert r"'a\1b' AS" in out


def test_unknown_key_raises() -> None:
    with pytest.raises(KeyError, match="SOCKET_PATH"):
        apply_overrides(_CONFIG, {"SOCKET_PATH": "/tmp/x"})
