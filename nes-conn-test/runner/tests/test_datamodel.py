# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Framework self-test for `conntest_runner.datamodel` (no docker).

The native tuple memory layout is owned by the engine (the C++ harness encodes/
decodes JSONL through the engine's own formatters), so these tests never assert
native bytes. ``row_width`` — the one engine-sourced number, reported by the
harness BIND reply at runtime — is supplied here as a plain integer; it only
sizes "N buffers" worth of rows, so any consistent value drives the row count.
"""

from __future__ import annotations

import json

import pytest

from conntest_runner.datamodel import (
    Generator,
    SinkModel,
    SourceModel,
    jsonl_to_rows,
    parse_sink_schema,
    parse_source_schema,
    rows_to_jsonl,
)

# A native schema mixing widths, two VARSIZED columns, and a FLOAT64 — the typed
# path the ODBC sink exercises. The widths below are illustrative row widths (the
# engine reports the real one at runtime); they only set rows-per-buffer.
_NATIVE_SCHEMA = [
    ("num_records", "INT16", False),
    ("activity_sec", "INT32", False),
    ("application", "VARSIZED", False),
    ("device", "VARSIZED", False),
    ("subscribers", "INT16", False),
    ("volume_total_bytes", "FLOAT64", False),
]
_NATIVE_WIDTH = 48  # 2 + 4 + 16 + 16 + 2 + 8
_SRC_ROW_SCHEMA = [
    ("k", "INT64", False),
    ("a", "INT32", False),
    ("b", "INT64", False),
    ("c", "FLOAT64", False),
]
_SRC_ROW_WIDTH = 28  # 8 + 4 + 8 + 8
_NULL_SCHEMA = [
    ("id", "INT32", False),
    ("reading", "FLOAT64", True),
    ("name", "VARSIZED", False),
]
_NULL_WIDTH = 29  # 4 + (1 + 8) + 16


# ── schema parsers ───────────────────────────────────────────────────────


def test_parse_sink_schema() -> None:
    sql = (
        "CREATE SINK conntest_sink (num_records INT16 NOT NULL, activity_sec INT32 NOT NULL, "
        "application VARSIZED NOT NULL, device VARSIZED NOT NULL, subscribers INT16 NOT NULL, "
        "volume_total_bytes FLOAT64 NOT NULL) TYPE ODBC SET ('localhost' AS `SINK`.db_host);"
    )
    assert parse_sink_schema(sql) == _NATIVE_SCHEMA


def test_parse_sink_schema_strips_quotes_and_reads_nullable() -> None:
    sql = 'CREATE SINK s (`a` INT32 NOT NULL, "b" VARSIZED, c FLOAT64 NOT NULL) TYPE ODBC SET ();'
    assert parse_sink_schema(sql) == [
        ("a", "INT32", False),
        ("b", "VARSIZED", True),
        ("c", "FLOAT64", False),
    ]


def test_parse_source_schema() -> None:
    sql = (
        "CREATE LOGICAL SOURCE conntest_rows (k INT64 NOT NULL, a INT32 NOT NULL, "
        "b INT64 NOT NULL, c FLOAT64 NOT NULL); CREATE PHYSICAL SOURCE ... TYPE ODBC;"
    )
    assert parse_source_schema(sql) == _SRC_ROW_SCHEMA
    assert parse_source_schema("CREATE LOGICAL SOURCE smoke_input (data UINT8);") == [
        ("data", "UINT8", True)
    ]
    with pytest.raises(ValueError, match="LOGICAL SOURCE"):
        parse_source_schema("SELECT 1")


# ── Generator / Dataset (buffer-quantized, the one authority) ────────────


def test_generator_deterministic() -> None:
    a = Generator(_NATIVE_SCHEMA, seed=1, row_width=_NATIVE_WIDTH).dataset_for_buffers(2, 4096)
    b = Generator(_NATIVE_SCHEMA, seed=1, row_width=_NATIVE_WIDTH).dataset_for_buffers(2, 4096)
    assert a.rows() == b.rows()
    # a different seed yields different rows
    c = Generator(_NATIVE_SCHEMA, seed=2, row_width=_NATIVE_WIDTH).dataset_for_buffers(2, 4096)
    assert c.rows() != a.rows()


def test_dataset_for_buffers_fills_exactly_n_buffers() -> None:
    gen = Generator(_NATIVE_SCHEMA, seed=1, row_width=_NATIVE_WIDTH)
    buffer_size = 512
    n = 3
    ds = gen.dataset_for_buffers(n, buffer_size)
    rpb = buffer_size // gen.row_width
    assert rpb >= 1
    assert ds.rows_per_buffer == rpb
    assert ds.row_count() == n * rpb
    bufs = ds.buffers()
    assert len(bufs) == n
    assert all(len(b) == rpb for b in bufs)


def test_row_width_drives_rows_per_buffer() -> None:
    # The injected (engine-sourced) row width is the only thing that sets how many
    # rows fill a buffer; a wider row packs fewer.
    narrow = Generator(_SRC_ROW_SCHEMA, seed=1, row_width=8).dataset_for_buffers(1, 64)
    wide = Generator(_SRC_ROW_SCHEMA, seed=1, row_width=32).dataset_for_buffers(1, 64)
    assert narrow.rows_per_buffer == 64 // 8
    assert wide.rows_per_buffer == 64 // 32
    # a row wider than the buffer still yields at least one row per buffer
    assert Generator(_SRC_ROW_SCHEMA, seed=1, row_width=999).rows_per_buffer(64) == 1


def test_seed_batches_match_old_head_tail_split() -> None:
    ds = Generator(_NATIVE_SCHEMA, seed=7, row_width=_NATIVE_WIDTH).dataset_for_buffers(2, 64)
    rows = ds.rows()
    head_len = (len(rows) + 1) // 2
    batches = ds.seed_batches(2)
    assert batches == [rows[:head_len], rows[head_len:]]
    assert ds.seed_batches(1) == [rows]


def test_rows_to_jsonl_uint8_stream() -> None:
    # The opaque (MQTT) source's wire form is JSONL the loader produces with
    # rows_to_jsonl: one schema-keyed object per line for a single-UINT8 schema.
    schema = [("data", "UINT8", False)]
    ds = Generator(schema, seed=3, row_width=1).dataset_for_buffers(2, 4)  # width 1 -> 8 rows
    blob = rows_to_jsonl(schema, ds.rows())
    lines = [line for line in blob.split(b"\n") if line.strip()]
    assert len(lines) == 8
    parsed = [json.loads(line) for line in lines]
    assert [p["data"] for p in parsed] == [r[0] for r in ds.rows()]
    assert all(0 <= p["data"] < 256 for p in parsed)


# ── SourceModel ──────────────────────────────────────────────────────────


def test_source_model_rows_roundtrip() -> None:
    m = SourceModel(_SRC_ROW_SCHEMA)
    rows = m.make_dataset(n_buffers=2, seed=5, buffer_size=512, row_width=_SRC_ROW_WIDTH).rows()
    assert rows
    # fill_counts reports both measures the harness picks from (row count, JSONL bytes)
    assert m.fill_counts(rows) == (len(rows), len(rows_to_jsonl(_SRC_ROW_SCHEMA, rows)))
    # the harness emits JSONL; a SELECT may reorder -> still passes (multiset)
    observed = rows_to_jsonl(_SRC_ROW_SCHEMA, list(reversed(rows)))
    m.compare_fill(rows, observed=observed)
    short = rows_to_jsonl(_SRC_ROW_SCHEMA, rows[:-1])
    with pytest.raises(AssertionError, match="rows-equal mismatch"):
        m.compare_fill(rows, observed=short)


def test_source_model_rows_accepts_nullable_scalar() -> None:
    # A nullable scalar source column: the engine emits `null` for a NULL, which the
    # runner parses back to None and the type-keyed compare matches.
    m = SourceModel([("a", "INT32", True)])
    rows = [(7,), (None,)]
    observed = rows_to_jsonl([("a", "INT32", True)], rows)
    m.compare_fill(rows, observed=observed)


def test_source_model_rows_varsized_roundtrip() -> None:
    # A non-null VARSIZED and a nullable VARSIZED: the engine renders them as JSON
    # strings (NULL as `null`); the model parses the observed JSONL back to rows.
    schema = [("id", "INT32", False), ("name", "VARSIZED", False), ("note", "VARSIZED", True)]
    m = SourceModel(schema)
    rows = [(1, "ab", "x"), (2, "cd", None)]
    observed = rows_to_jsonl(schema, list(reversed(rows)))  # a SELECT may reorder
    m.compare_fill(rows, observed=observed)


def test_source_model_fill_counts() -> None:
    # The model is kind-blind: fill_counts always returns both measures of the batch —
    # the row count and the JSONL byte length. The harness picks the one matching its
    # native unit (tuples for a NATIVE source, bytes for an opaque one); the runner does
    # not know or branch on the kind.
    schema = [("data", "UINT8", True)]
    m = SourceModel(schema)
    rows = m.make_dataset(n_buffers=2, seed=3, buffer_size=4, row_width=1).rows()  # 8 rows
    assert m.fill_counts(rows) == (len(rows), len(rows_to_jsonl(schema, rows)))
    m.compare_fill(rows, observed=rows_to_jsonl(schema, list(reversed(rows))))


def test_source_model_keeps_nullability() -> None:
    # No null-forcing (principle 4): a nullable column generates a deterministic mix of
    # NULLs (not suppressed), which round-trip through JSONL as `null`/None and compare.
    m = SourceModel(_NULL_SCHEMA)
    rows = m.make_dataset(n_buffers=2, seed=1, buffer_size=512, row_width=_NULL_WIDTH).rows()
    assert any(r[1] is None for r in rows)  # NULLs generated, not suppressed
    m.compare_fill(rows, observed=rows_to_jsonl(_NULL_SCHEMA, rows))


# ── SinkModel ──────────────────────────────────────────────────────────--


def test_sink_model_roundtrip() -> None:
    m = SinkModel(_NATIVE_SCHEMA)
    ds = m.make_dataset(n_buffers=2, seed=1, buffer_size=512, row_width=_NATIVE_WIDTH)
    assert m.input_record_count(ds) == ds.row_count()
    assert len(m.harness_input(ds)) > 0  # a real JSONL stream
    # the loader returns typed rows; a SELECT/broker may reorder -> still passes
    m.verify_readback(ds.rows(), list(reversed(ds.rows())))
    with pytest.raises(AssertionError, match="rows-equal mismatch"):
        m.verify_readback(ds.rows(), ds.rows()[:-1])  # one short


def test_sink_model_handles_nulls() -> None:
    m = SinkModel(_NULL_SCHEMA)
    ds = m.make_dataset(n_buffers=2, seed=1, buffer_size=512, row_width=_NULL_WIDTH)
    rows = ds.rows()
    nulls = sum(1 for r in rows if r[1] is None)
    assert 0 < nulls < len(rows)  # a deterministic mix of NULLs and values
    m.verify_readback(rows, list(reversed(rows)))  # NULLs compare equal, order-blind
    corrupt = [(r[0], None, r[2]) if r[1] is not None else r for r in rows]
    assert corrupt != rows
    with pytest.raises(AssertionError, match="rows-equal mismatch"):
        m.verify_readback(rows, corrupt)


def test_sink_input_is_jsonl() -> None:
    # The sink is fed JSONL (decoded to native tuples, or sliced into records, by the
    # harness) — one schema-keyed object per row.
    m = SinkModel(_NATIVE_SCHEMA)
    ds = m.make_dataset(n_buffers=2, seed=1, buffer_size=512, row_width=_NATIVE_WIDTH)
    payload = m.harness_input(ds)
    lines = [line for line in payload.split(b"\n") if line.strip()]
    assert len(lines) == ds.row_count()
    assert all("num_records" in json.loads(p) for p in lines)


def test_verify_readback_rejects_wrong_arity_row() -> None:
    # A read-back ROW whose column count differs from the schema is a malformed
    # result (a wrong projection / decode), distinct from a wrong row *count*.
    # The shared canonicalizer zips strict, so it fails loudly rather than being
    # silently compared on the shorter prefix.
    m = SinkModel(_SRC_ROW_SCHEMA)  # 4 columns
    expected = [(1, 2, 3, 4.0)]
    with pytest.raises(ValueError, match="argument"):  # zip(strict=True) arity guard
        m.verify_readback(expected, [(1, 2, 3)])  # dropped the 4th column


def test_jsonl_roundtrip_handles_special_chars() -> None:
    # The opaque sink loader parses its collected records with jsonl_to_rows; JSONL
    # escapes values that would have corrupted the old CSV form (a comma or newline in
    # a VARSIZED), so the parsed rows reproduce the originals exactly and compare equal.
    schema = [("text", "VARSIZED", False)]
    rows = [("a,b",), ("line1\nline2",), ('quote"here',)]
    payload = rows_to_jsonl(schema, rows)
    assert len([line for line in payload.split(b"\n") if line.strip()]) == 3  # newline escaped
    parsed = jsonl_to_rows(schema, payload)
    assert parsed == rows
    SinkModel(schema).verify_readback(rows, parsed)  # the model multiset-compares the parsed rows


def test_dump_is_jsonl() -> None:
    # The debug dump is the same JSONL the connectors carry: a `#` metadata header
    # then one object per row, round-tripping back to the oracle (NULL -> null).
    ds = Generator(_NULL_SCHEMA, seed=1, row_width=_NULL_WIDTH).dataset_for_buffers(2, 256)
    lines = ds.dump().splitlines()
    assert lines[0].startswith("# seed=")
    body = "\n".join(lines[1:]).encode("utf-8")
    assert jsonl_to_rows(_NULL_SCHEMA, body) == ds.rows()
