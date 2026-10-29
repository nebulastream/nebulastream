# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Rewrites a pristine ``.nesql`` config for one test run by replacing the values bound to keys that the loader lists.

A committed config holds placeholder values, such as `'localhost' AS "SOURCE".SOCKET_HOST` or `9000 AS "SOURCE".SOCKET_PORT`,
because the real ones are only known at run time: the compose service, a dead endpoint for ``bad_endpoint``,
or a topic or table of the test's own.
Overriding by key means the loader does not depend on the literal that the config holds,
and a key that the config lacks raises instead of leaving the config unchanged.
"""

from __future__ import annotations

import re


def _binding(key: str) -> str:
    # A SET entry looks like `'localhost' AS "SOURCE".SOCKET_HOST`.
    # The scope and the key may be double-quoted, because NebulaStream SQL quotes reserved words.
    # The lookahead stops a key from matching a longer key that it prefixes, such as `port` in `.port_range`.
    return r'\s+AS\s+"?\w+"?\."?' + re.escape(key) + r'"?(?![\w"])'


def _key_pattern(key: str) -> re.Pattern[str]:
    # The value is a quoted string or a bare literal, such as a number.
    return re.compile(r"(?:'[^']*'|[^\s,()']+)(" + _binding(key) + ")")


def _literal(value: str | int) -> str:
    # NebulaStream checks the literal's type, so a numeric field takes a bare number and a string field a quoted one.
    return str(value) if isinstance(value, int) else "'" + value + "'"


def apply_overrides(config: str, overrides: dict[str, str | int]) -> str:
    out = config
    for key, value in overrides.items():
        # A function replacement keeps backslashes in the value literal.
        # The default argument binds this iteration's value.
        def _replace(m: re.Match[str], literal: str = _literal(value)) -> str:
            return literal + m.group(1)

        out, count = _key_pattern(key).subn(_replace, out, count=1)
        if count == 0:
            raise KeyError(
                f"apply_overrides: key {key!r} not found in config — the loader's "
                f'override does not match any `AS "SCOPE".{key}` in the .nesql'
            )
    return out
