# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""ODBC sink TestDataLoader for the conn-test harness.

Discovered by the runner via the per-plugin ``conn-test/loader.py`` glob.
Sink-side flow (inverse of the ODBC source): the harness loads the committed
native ``.nes`` fixture (``input/records.nes``) into NES *native* tuples under
the sink's schema (the sink config sets ``output_format=Native``) and drains
them through ODBCSink, which binds each column to a typed ODBC parameter and
INSERTs it. ``read_back`` then SELECTs the rows and re-renders them in the same
canonical form for an order-insensitive multiset compare.

The fixture is the input-formatter ``Food`` dataset (so the ``.nes`` is a real
engine artifact rather than a bespoke encoding): two VARSIZED string columns,
mixed INT16/INT32 widths, and a FLOAT64 — enough to exercise the typed/varsized
native path. ``harness_input`` hands the harness the ``.nes`` while ``load``
parses the paired CSV only to derive the expected read-back multiset.

The sink reads its endpoint from config (``db_host`` / ``db_port``); this loader
talks to the same Postgres directly over TCP via psycopg to create the per-test
table (``setup``) and read it back.

ODBCSink connects eagerly in ``start()``, so an unreachable endpoint surfaces as
CannotOpenSink (4004) at the open phase — the framework default for
``bad_endpoint`` — hence no ``BAD_ENDPOINT_ERROR_CODE`` override.
"""

from __future__ import annotations

import csv
import io
from typing import Any, ClassVar

import psycopg

from conntest_runner.datamodel import ByteData, ByteModel
from conntest_runner.naming import unique_token

# The sink schema, in NES MemoryLayout order: (name, NES type, nullable). Mirror
# of `configs/valid/basic.nesql` AND of the committed native fixture
# `input/records.nes` (the input-formatter `Food` dataset). All columns are
# NOT NULL — the harness loads `records.nes` byte-for-byte under this schema, and
# a nullable column would shift the row stride. Two VARSIZED columns keep the
# typed/varsized path under test.
_SCHEMA: tuple[tuple[str, str, bool], ...] = (
    ("num_records", "INT16", False),
    ("activity_sec", "INT32", False),
    ("application", "VARSIZED", False),
    ("device", "VARSIZED", False),
    ("subscribers", "INT16", False),
    ("volume_total_bytes", "FLOAT64", False),
)
_COLUMNS = tuple(name for name, _typ, _nullable in _SCHEMA)

# The Food fixture is pipe-delimited; the harness reads the sibling `.nes`, while
# this loader parses the CSV only to derive the expected read-back multiset.
_CSV_DELIMITER = "|"

# Canonical per-row encoding shared by the input side (``RowModel.load``) and
# the read-back side. A unit separator that never occurs in the fixture keeps it
# unambiguous regardless of embedded commas; NULL gets a sentinel distinct from
# an empty string. Both sides run it on typed Python values, so an embedded
# comma, a float, and a NULL all compare exactly.
_FIELD_SEP = b"\x1f"
_NULL = b"\x00NULL\x00"


def _canonical_field(typ: str, value: Any) -> bytes:
    if value is None:
        return _NULL
    if typ.startswith(("INT", "UINT")):
        return str(int(value)).encode("ascii")
    if typ.startswith("FLOAT"):
        return repr(float(value)).encode("ascii")
    return str(value).encode("utf-8")  # VARSIZED


def _canonical_row(values: tuple[Any, ...]) -> bytes:
    return _FIELD_SEP.join(
        _canonical_field(typ, value)
        for (_name, typ, _nullable), value in zip(_SCHEMA, values, strict=False)
    )


class OdbcSinkRowModel(ByteModel):
    """Native-tuple sink model over a typed schema.

    Reuses ByteModel's record-multiset machinery (``write_quota`` =
    record count, ``compare_readback`` = multiset compare) but loads the CSV
    fixture as *typed rows* and emits each in the canonical encoding, so the
    comparison is type-aware: a string column stays one column and floats
    compare by value. The harness reads the sibling native ``.nes`` (the same
    data) via ``--input-path`` and pushes those tuples through the sink, so the
    rows that land in the DB are exactly these.
    """

    def harness_input(self, bound: Any) -> bytes:
        """Native sink: hand the harness the committed `.nes`, not the CSV text.

        The `.nes` sits next to the CSV fixture (``records.csv`` ->
        ``records.nes``); the harness loads it directly into native tuples.
        """
        data: bytes = bound.path.with_suffix(".nes").read_bytes()
        return data

    def load(self, src: Any) -> ByteData:
        """Parse the bound CSV source into canonical typed rows (expected side)."""
        text = src.bytes().decode("utf-8")
        rows: list[bytes] = []
        for cells in csv.reader(io.StringIO(text), delimiter=_CSV_DELIMITER):
            if not cells:
                continue
            values: list[Any] = []
            for (_name, typ, _nullable), cell in zip(_SCHEMA, cells, strict=False):
                if typ == "VARSIZED":
                    values.append(cell)
                elif typ.startswith("FLOAT"):
                    values.append(float(cell))
                else:
                    values.append(int(cell))
            rows.append(_canonical_row(tuple(values)))
        return ByteData(b"\n".join(rows) + (b"\n" if rows else b""))


def _build_dsn(host: str, port: int | str, *, dbname: str, user: str, password: str) -> str:
    """Libpq DSN string from parts."""
    return f"host={host} port={port} dbname={dbname} user={user} password={password}"


def _execute(dsn_str: str, sql: str) -> None:
    """Run a single statement (DDL or DML) and commit."""
    with psycopg.connect(dsn_str) as cx, cx.cursor() as cur:
        cur.execute(sql)
        cx.commit()


def _select_all(dsn_str: str, table: str, columns: tuple[str, ...]) -> list[tuple[Any, ...]]:
    """Return every row of ``table`` projected to ``columns`` as a list of tuples.

    No ``ORDER BY`` — the sink round-trip compares an order-insensitive multiset,
    so row order is irrelevant.
    """
    projection = ", ".join(f'"{c}"' for c in columns)
    with psycopg.connect(dsn_str) as cx, cx.cursor() as cur:
        cur.execute(f'SELECT {projection} FROM "{table}"')  # noqa: S608  # trusted static test-data SQL
        return list(cur.fetchall())


class OdbcSinkLoader:
    """Per-plugin TestDataLoader for the ODBC sink over Postgres.

    The class name is conventional, not load-bearing: discovery picks the single
    duck-typed match in this module.
    """

    system: ClassVar[str] = "postgres"
    compose_service: ClassVar[str] = "db"
    compose_port: ClassVar[int] = 5432

    # The sink receives native typed tuples; the row model owns the input quota
    # and the order-insensitive canonical-record multiset read-back compare.
    data_model: ClassVar[OdbcSinkRowModel] = OdbcSinkRowModel()

    def __init__(self, endpoint: str) -> None:
        self._endpoint = endpoint
        host, _, port = endpoint.rpartition(":")
        if not host or not port:
            raise ValueError(f"OdbcSinkLoader: endpoint must be 'host:port', got {endpoint!r}")
        self._host, self._port = host, int(port)

    # ---- framework hooks --------------------------------------------------
    def make_target(self, *, test_id: str, scenario: str, worker_id: str) -> str:
        """Per-test Postgres TABLE name (``cts_`` prefix).

        Uniqueness from the central ``unique_token``.
        """
        return "cts_" + unique_token(test_id, scenario, worker_id)

    def config_overrides(self, *, target: str, endpoint: str) -> dict[str, str]:
        """Config keys the framework swaps in by value.

        Point it at ``endpoint`` (host:port) and the per-test ``target`` table.
        Credentials stay as the config authored them (they match compose).
        """
        host, _, port = endpoint.rpartition(":")
        return {"db_host": host, "db_port": port, "table": target}

    def _create_sql(self, target: str) -> str:
        # Column order matches _SCHEMA / the CREATE SINK; ODBCSink INSERTs
        # positionally, so only count + coercion-compatibility matter.
        return (
            f'CREATE TABLE IF NOT EXISTS "{target}" '
            "(num_records SMALLINT, activity_sec INTEGER, application TEXT, "
            "device TEXT, subscribers SMALLINT, volume_total_bytes DOUBLE PRECISION)"
        )

    def setup(self, *, target: str) -> None:
        """Create the per-test target table before START.

        ODBCSink's INSERTs (issued during the WRITE drain) need it to exist.
        """
        _execute(self._dsn(), self._create_sql(target))

    def read_back(
        self, *, target: str, expect_records: int | None = None, timeout_s: float = 30.0
    ) -> list[bytes]:
        """Return the rows the sink INSERTed, each in the canonical encoding.

        The battery can then multiset-compare against the input. ``expect_records``
        / ``timeout_s`` are unused (a Postgres SELECT is synchronous and complete).
        """
        del expect_records, timeout_s
        rows = _select_all(self._dsn(), target, _COLUMNS)
        return [_canonical_row(row) for row in rows]

    def _dsn(self) -> str:
        return _build_dsn(
            self._host,
            self._port,
            dbname="nebulastream",
            user="nes",
            password="nes",  # noqa: S106  # test credential
        )
