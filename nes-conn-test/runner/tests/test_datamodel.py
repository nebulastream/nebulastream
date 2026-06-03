# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Framework self-test for `conntest_runner.datamodel` (no docker)."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from conntest_runner.datamodel import (
    ByteModel,
    File,
    Generate,
    NativeModel,
    RowModel,
    assert_multiset_equal,
    parse_sink_schema,
)
from conntest_runner.protocol import Got

_ROW_SCHEMA = [("k", "INT64"), ("a", "INT32"), ("b", "INT64"), ("c", "FLOAT64")]

# A native sink schema mixing widths, two VARSIZED columns, and a FLOAT64 —
# the typed path the ODBC sink exercises.
_NATIVE_SQL = (
    "CREATE SINK conntest_sink (num_records INT16 NOT NULL, activity_sec INT32 NOT NULL, "
    "application VARSIZED NOT NULL, device VARSIZED NOT NULL, subscribers INT16 NOT NULL, "
    "volume_total_bytes FLOAT64 NOT NULL) TYPE ODBC SET ('localhost' AS `SINK`.db_host);"
)
_NATIVE_SCHEMA = [
    ("num_records", "INT16", False),
    ("activity_sec", "INT32", False),
    ("application", "VARSIZED", False),
    ("device", "VARSIZED", False),
    ("subscribers", "INT16", False),
    ("volume_total_bytes", "FLOAT64", False),
]


def test_byte_model_roundtrip_from_bin(tmp_path: Path) -> None:
    blob = bytes(range(256)) * 4
    p = tmp_path / "blob.bin"
    p.write_bytes(blob)
    m = ByteModel()
    d = m.load(File("blob.bin").bind(tmp_path))
    assert m.fill_quota(d) == len(blob)
    got = Got(count=len(blob), bytes=len(blob), sha=hashlib.sha256(blob).hexdigest())
    m.compare_fill(d, got=got, observed=b"")  # sha authoritative; no bytes needed


def test_byte_model_sha_mismatch_fails(tmp_path: Path) -> None:
    p = tmp_path / "blob.bin"
    p.write_bytes(b"hello world")
    m = ByteModel()
    d = m.load(File("blob.bin").bind(tmp_path))
    bad = Got(count=11, bytes=11, sha="0" * 64)
    with pytest.raises(AssertionError):
        m.compare_fill(d, got=bad, observed=b"")


def test_byte_model_sink_records(tmp_path: Path) -> None:
    p = tmp_path / "in.csv"
    p.write_text("0,0,0\n1,2,3\n2,4,6\n")
    m = ByteModel()
    d = m.load(File("in.csv").bind(tmp_path))
    assert m.write_quota(d) == 3
    # read_back returns the same records in any order -> multiset compare passes
    received = [b"2,4,6", b"0,0,0", b"1,2,3"]
    m.compare_readback(d, received)


def test_row_model_native_width() -> None:
    m = RowModel(_ROW_SCHEMA)
    assert m.native_row_width == 28  # q(8)+i(4)+q(8)+d(8)


def test_row_model_roundtrip_from_csv(tmp_path: Path) -> None:
    p = tmp_path / "rows.csv"
    p.write_text("3,33,333,3.5\n2,22,222,2.5\n1,11,111,1.5\n")
    m = RowModel(_ROW_SCHEMA)
    d = m.load(File("rows.csv").bind(tmp_path))
    assert m.fill_quota(d) == 3
    # Harness ships native bytes; a DB SELECT may reorder -> shuffle and the
    # multiset compare must still pass.
    reordered = [d.rows[2], d.rows[0], d.rows[1]]
    observed = m.encode(reordered)
    got = Got(count=3, bytes=3 * 28)
    m.compare_fill(d, got=got, observed=observed)


def test_row_model_detects_missing_row(tmp_path: Path) -> None:
    p = tmp_path / "rows.csv"
    p.write_text("3,33,333,3.5\n2,22,222,2.5\n1,11,111,1.5\n")
    m = RowModel(_ROW_SCHEMA)
    d = m.load(File("rows.csv").bind(tmp_path))
    observed = m.encode([d.rows[0], d.rows[1]])  # one short
    got = Got(count=2, bytes=2 * 28)
    with pytest.raises(AssertionError):
        m.compare_fill(d, got=got, observed=observed)


def test_generate_rows_deterministic() -> None:
    m = RowModel(_ROW_SCHEMA)
    a = m.load(Generate(count=10, seed=1).bind(Path()))
    b = m.load(Generate(count=10, seed=1).bind(Path()))
    assert a.rows == b.rows
    assert len(a.rows) == 10


def test_assert_multiset_equal_diff_message() -> None:
    with pytest.raises(AssertionError, match="multiset mismatch"):
        assert_multiset_equal([1, 2, 3], [1, 2, 4])


# ── native sink model ───────────────────────────────────────────────────


def test_parse_sink_schema() -> None:
    schema = parse_sink_schema(_NATIVE_SQL)
    assert schema == _NATIVE_SCHEMA


def test_parse_sink_schema_strips_quotes_and_reads_nullable() -> None:
    sql = 'CREATE SINK s (`a` INT32 NOT NULL, "b" VARSIZED, c FLOAT64 NOT NULL) TYPE ODBC SET ();'
    assert parse_sink_schema(sql) == [
        ("a", "INT32", False),
        ("b", "VARSIZED", True),
        ("c", "FLOAT64", False),
    ]


def test_native_model_generation_deterministic() -> None:
    m = NativeModel().bind(_NATIVE_SCHEMA)
    a = m.load(Generate(count=50))
    b = m.load(Generate(count=50))
    assert a.rows == b.rows
    assert m.record_count(a) == 50
    # Each row matches the schema arity and column kinds (str for VARSIZED).
    assert all(len(r) == len(_NATIVE_SCHEMA) for r in a.rows)
    assert all(isinstance(r[2], str) and isinstance(r[5], float) for r in a.rows)


def test_native_model_input_is_the_oracle() -> None:
    # The bytes handed to the harness and the read-back oracle come from the
    # same generated rows — the single source of truth (no drift).
    m = NativeModel().bind(_NATIVE_SCHEMA)
    gen = Generate(count=33)
    assert m.input_record_count(gen) == 33
    assert len(m.harness_input(gen)) > 0  # a real `.nes` container
    assert m.load(gen).rows == m.load(gen).rows


def test_native_model_verify_readback_is_order_insensitive() -> None:
    m = NativeModel().bind(_NATIVE_SCHEMA)
    d = m.load(Generate(count=20))
    received = list(reversed(d.rows))  # a SELECT may reorder
    m.verify_readback(d, received)  # passes despite the shuffle


def test_native_model_verify_readback_detects_missing_row() -> None:
    m = NativeModel().bind(_NATIVE_SCHEMA)
    d = m.load(Generate(count=20))
    with pytest.raises(AssertionError, match="read-back mismatch"):
        m.verify_readback(d, d.rows[:-1])  # one short


def test_native_model_prefix_and_combine() -> None:
    m = NativeModel().bind(_NATIVE_SCHEMA)
    d = m.load(Generate(count=20))
    assert m.record_count(m.prefix(d, 5)) == 5
    assert m.prefix(d, 5).rows == d.rows[:5]
    assert m.combine([m.prefix(d, 5), m.prefix(d, 3)]).rows == d.rows[:5] + d.rows[:3]


def test_native_model_rejects_nullable_schema() -> None:
    with pytest.raises(ValueError, match="NOT NULL"):
        NativeModel().bind([("a", "INT32", True)])
