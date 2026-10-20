# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for the conn-test data model."""

from __future__ import annotations

import json

import pytest

from conntest_runner.datamodel import (
    SourceModel,
    generate_rows,
    jsonl_to_rows,
    parse_source_schema,
    rows_to_jsonl,
)

_NATIVE_SCHEMA = [
    ("num_records", "INT16", False),
    ("activity_sec", "INT32", False),
    ("application", "VARSIZED", False),
    ("device", "VARSIZED", False),
    ("subscribers", "INT16", False),
    ("volume_total_bytes", "FLOAT64", False),
]
_SRC_ROW_SCHEMA = [
    ("k", "INT64", False),
    ("a", "INT32", False),
    ("b", "INT64", False),
    ("c", "FLOAT64", False),
]
_NULL_SCHEMA = [
    ("id", "INT32", False),
    ("reading", "FLOAT64", True),
    ("name", "VARSIZED", False),
]



def test_parse_source_schema() -> None:
    sql = (
        "CREATE LOGICAL SOURCE conntest_rows (k INT64 NOT NULL, a INT32 NOT NULL, "
        "b INT64 NOT NULL, c FLOAT64 NOT NULL); CREATE PHYSICAL SOURCE ... TYPE TCP;"
    )
    assert parse_source_schema(sql) == _SRC_ROW_SCHEMA
    assert parse_source_schema("CREATE LOGICAL SOURCE smoke_input (data UINT8);") == [
        ("data", "UINT8", True)
    ]
    # Quoted identifiers lose their quotes; a column without NOT NULL is nullable.
    assert parse_source_schema('CREATE LOGICAL SOURCE s (`a` INT32 NOT NULL, "b" VARSIZED);') == [
        ("a", "INT32", False),
        ("b", "VARSIZED", True),
    ]
    with pytest.raises(ValueError, match="LOGICAL SOURCE"):
        parse_source_schema("SELECT 1")



def test_generate_rows_deterministic() -> None:
    a = generate_rows(_NATIVE_SCHEMA, seed=1, max_bytes=4096)
    assert a == generate_rows(_NATIVE_SCHEMA, seed=1, max_bytes=4096)
    assert a != generate_rows(_NATIVE_SCHEMA, seed=2, max_bytes=4096)


def test_generate_rows_fills_max_bytes() -> None:
    rows = generate_rows(_NATIVE_SCHEMA, seed=1, max_bytes=4096)
    assert len(rows_to_jsonl(_NATIVE_SCHEMA, rows)) <= 4096
    # One more row would not fit.
    more = generate_rows(_NATIVE_SCHEMA, seed=1, max_bytes=1 << 20)[: len(rows) + 1]
    assert len(rows_to_jsonl(_NATIVE_SCHEMA, more)) > 4096
    # A limit below one row still gives one row.
    assert len(generate_rows(_NATIVE_SCHEMA, seed=1, max_bytes=1)) == 1


def test_rows_to_jsonl_uint8_stream() -> None:
    schema = [("data", "UINT8", False)]
    rows = generate_rows(schema, seed=3, max_bytes=128)
    blob = rows_to_jsonl(schema, rows)
    lines = [line for line in blob.split(b"\n") if line.strip()]
    assert len(lines) == len(rows)
    parsed = [json.loads(line) for line in lines]
    assert [p["data"] for p in parsed] == [r[0] for r in rows]
    assert all(0 <= p["data"] < 256 for p in parsed)



def test_source_model_rows_roundtrip() -> None:
    m = SourceModel(_SRC_ROW_SCHEMA)
    rows = m.make_rows(n_buffers=2, seed=5, buffer_size=512)
    assert rows
    assert m.fill_counts(rows) == (len(rows), len(rows_to_jsonl(_SRC_ROW_SCHEMA, rows)))
    # A SELECT may reorder rows, so the compare ignores order.
    observed = rows_to_jsonl(_SRC_ROW_SCHEMA, list(reversed(rows)))
    m.compare_fill(rows, observed=observed)
    short = rows_to_jsonl(_SRC_ROW_SCHEMA, rows[:-1])
    with pytest.raises(AssertionError, match="rows-equal mismatch"):
        m.compare_fill(rows, observed=short)


def test_source_model_rows_accepts_nullable_scalar() -> None:
    m = SourceModel([("a", "INT32", True)])
    rows = [(7,), (None,)]
    observed = rows_to_jsonl([("a", "INT32", True)], rows)
    m.compare_fill(rows, observed=observed)


def test_source_model_rows_varsized_roundtrip() -> None:
    schema = [("id", "INT32", False), ("name", "VARSIZED", False), ("note", "VARSIZED", True)]
    m = SourceModel(schema)
    rows = [(1, "ab", "x"), (2, "cd", None)]
    observed = rows_to_jsonl(schema, list(reversed(rows)))
    m.compare_fill(rows, observed=observed)


def test_source_model_fill_counts() -> None:
    schema = [("data", "UINT8", True)]
    m = SourceModel(schema)
    rows = m.make_rows(n_buffers=2, seed=3, buffer_size=512)
    assert m.fill_counts(rows) == (len(rows), len(rows_to_jsonl(schema, rows)))
    m.compare_fill(rows, observed=rows_to_jsonl(schema, list(reversed(rows))))


def test_source_model_keeps_nullability() -> None:
    # Nullable columns are not forced to NOT NULL, so the generated rows contain NULLs.
    m = SourceModel(_NULL_SCHEMA)
    rows = m.make_rows(n_buffers=2, seed=1, buffer_size=512)
    assert any(r[1] is None for r in rows)
    m.compare_fill(rows, observed=rows_to_jsonl(_NULL_SCHEMA, rows))


def test_compare_fill_is_order_insensitive_and_counts_rows() -> None:
    m = SourceModel(_NATIVE_SCHEMA)
    rows = m.make_rows(n_buffers=2, seed=1, buffer_size=512)
    m.compare_fill(rows, observed=rows_to_jsonl(_NATIVE_SCHEMA, list(reversed(rows))))
    with pytest.raises(AssertionError, match="rows-equal mismatch"):
        m.compare_fill(rows, observed=rows_to_jsonl(_NATIVE_SCHEMA, rows[:-1]))  # one short


def test_compare_fill_detects_a_changed_null() -> None:
    m = SourceModel(_NULL_SCHEMA)
    rows = m.make_rows(n_buffers=2, seed=1, buffer_size=512)
    corrupt = [(r[0], None, r[2]) if r[1] is not None else r for r in rows]
    assert corrupt != rows
    with pytest.raises(AssertionError, match="rows-equal mismatch"):
        m.compare_fill(rows, observed=rows_to_jsonl(_NULL_SCHEMA, corrupt))


def test_jsonl_roundtrip_handles_special_chars() -> None:
    # A comma or newline in a VARSIZED would corrupt a delimited format.
    schema = [("text", "VARSIZED", False)]
    rows = [("a,b",), ("line1\nline2",), ('quote"here',)]
    payload = rows_to_jsonl(schema, rows)
    assert len([line for line in payload.split(b"\n") if line.strip()]) == 3  # newline escaped
    assert jsonl_to_rows(schema, payload) == rows
    SourceModel(schema).compare_fill(rows, observed=payload)
