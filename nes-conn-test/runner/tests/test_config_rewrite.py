# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Framework self-test for `conntest_runner.config_rewrite` (no docker).

Focuses on `single_table_of`: the table extractor the battery uses to repoint a
source's authored query at the per-test target. It must accept a provably
single-table SELECT and fail loud on anything it cannot prove reads exactly one
table (the alternative is a silently-wrong rewrite).
"""

from __future__ import annotations

import pytest

from conntest_runner.config_rewrite import single_table_of, value_of


@pytest.mark.parametrize(
    ("query", "table"),
    [
        (
            "SELECT k, a, b, c FROM conntest_rows "
            "WHERE created_at > ? AND created_at <= ? ORDER BY k DESC",
            "conntest_rows",
        ),
        ("SELECT * FROM events", "events"),
        ("select id from readings where id > ?", "readings"),
        ("SELECT * FROM events e WHERE e.id > ?", "events"),
        ("SELECT * FROM events AS e ORDER BY e.id", "events"),
        ("SELECT * FROM public.events WHERE id > ?", "public.events"),
        ("SELECT id, count(*) FROM hits GROUP BY id", "hits"),
        ("SELECT * FROM hits LIMIT 10", "hits"),
    ],
)
def test_single_table_of_accepts_single_table(query: str, table: str) -> None:
    assert single_table_of(query) == table


@pytest.mark.parametrize(
    "query",
    [
        # Comma-separated (implicit join), with and without surrounding spaces.
        "SELECT * FROM a, b WHERE a.id = b.id",
        "SELECT * FROM a,b WHERE a.id = b.id",
        "SELECT * FROM a , b",
        # Explicit joins (all spellings carry the word JOIN).
        "SELECT * FROM a JOIN b ON a.id = b.id",
        "SELECT a.x, b.y FROM a INNER JOIN b ON a.id = b.id",
        "SELECT * FROM a LEFT OUTER JOIN b ON a.id = b.id WHERE a.id > ?",
        "SELECT * FROM a CROSS JOIN b",
        # Nested / correlated subqueries — the second FROM gives them away.
        "SELECT * FROM (SELECT * FROM inner_t) sub",
        "SELECT * FROM a WHERE a.id IN (SELECT id FROM b)",
        "SELECT (SELECT max(v) FROM b) AS m FROM a",
        # Set operations read from two tables.
        "SELECT * FROM a UNION SELECT * FROM b",
        "SELECT * FROM a EXCEPT SELECT * FROM b",
        # No FROM at all.
        "SELECT 1",
    ],
)
def test_single_table_of_rejects_non_single_table(query: str) -> None:
    with pytest.raises(ValueError, match="single_table_of"):
        single_table_of(query)


def test_value_of_reads_query_table_end_to_end() -> None:
    """The table the battery repoints is exactly the one authored in the query."""
    config = (
        "CREATE PHYSICAL SOURCE FOR s TYPE ODBC SET (\n"
        "    'SELECT k FROM conntest_rows WHERE created_at > ?' AS `SOURCE`.query\n"
        ");\n"
    )
    query = value_of(config, "query")
    assert query is not None
    assert single_table_of(query) == "conntest_rows"
