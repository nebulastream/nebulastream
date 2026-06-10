# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""config_rewrite.py — derive a runnable config from a pristine one by key.

The connector ships *one* pristine, hand-authored ``.nesql`` config (literal
values, no placeholders) that doubles as documentation and as the validation
battery's input. To run it against the live compose service (or a deliberately
dead endpoint), the framework swaps the *values* of a handful of keys the
loader names — the endpoint and the per-test target — leaving everything else
exactly as authored.

This is override-by-**key**, not by literal value: the loader says "set
``server_uri`` to X", and we find whatever literal is currently bound to that
key and replace it. So the loader never has to know (or stay in sync with) the
literal the ``.nesql`` happens to carry — and a key the loader names but the
config doesn't carry raises loudly rather than silently no-op'ing.
"""

from __future__ import annotations

import re


def _key_pattern(key: str) -> re.Pattern[str]:
    # A SET entry is ``'value' AS `SCOPE`.key`` — value first, then the
    # backtick-optional scope (SINK/SOURCE), then the backtick-optional key.
    # The trailing (?![\w`]) stops a key from matching a longer one it prefixes
    # (e.g. `port` must not match inside `.db_port`).
    return re.compile(r"'[^']*'(\s+AS\s+`?\w+`?\.`?" + re.escape(key) + r"`?(?![\w`]))")


def value_of(config: str, key: str) -> str | None:
    """Return the quoted value currently bound to ``key`` in a ``SET (…)`` clause.

    The read-only inverse of :func:`apply_overrides` (same grammar). Returns
    ``None`` if the key is absent. Used to learn a config's authored value — e.g.
    the ``query`` a source reads — without the loader having to know it.
    """
    match = re.search(r"'([^']*)'\s+AS\s+`?\w+`?\.`?" + re.escape(key) + r"`?(?![\w`])", config)
    return match.group(1) if match is not None else None


def single_table_of(query: str) -> str:
    """Return the single table a one-table ``SELECT`` reads, for repointing to the per-test target.

    Only single-table queries are supported here — a test-framework limitation,
    not the connector's, which runs whatever query it is handed.
    """
    froms = list(re.finditer(r"\bFROM\b", query, re.IGNORECASE))
    if len(froms) != 1:
        raise ValueError(f"single_table_of: need exactly one FROM, found {len(froms)}: {query!r}")
    if re.search(r"\bJOIN\b", query, re.IGNORECASE):
        raise ValueError(f"single_table_of: JOIN reads more than one table: {query!r}")
    rest = query[froms[0].end() :].lstrip()
    tokens = rest.split()
    if not tokens:
        raise ValueError(f"single_table_of: no table after FROM: {query!r}")
    table = tokens[0]
    if "," in table or rest[len(table) :].lstrip().startswith(","):
        raise ValueError(f"single_table_of: comma-separated tables are not supported: {query!r}")
    return table


def apply_overrides(config: str, overrides: dict[str, str]) -> str:
    """Replace the quoted value bound to each named key in a ``SET (…)`` clause.

    ``overrides`` maps a config key (e.g. ``server_uri``, ``table``,
    ``query``) to its new value. Raises ``KeyError`` if a named key is not
    present in ``config`` — that means the loader's override list has drifted
    from the ``.nesql`` and the live config would otherwise be silently wrong.
    """
    out = config
    for key, value in overrides.items():
        # A function replacement avoids treating backslashes / group refs in the
        # value as regex replacement syntax; the default arg binds this
        # iteration's value into the closure.
        def _replace(m: re.Match[str], value: str = value) -> str:
            return "'" + value + "'" + m.group(1)

        out, count = _key_pattern(key).subn(_replace, out, count=1)
        if count == 0:
            raise KeyError(
                f"apply_overrides: key {key!r} not found in config — the loader's "
                f"override does not match any `AS `SCOPE`.{key}` in the .nesql"
            )
    return out
