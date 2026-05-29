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
Sink-side flow (inverse of the ODBC source): the harness parses the CSV fixture
into NES *native* tuples under the sink's schema (the sink config sets
``output_format=Native``) and drains them through ODBCSink, which binds each
column to a typed ODBC parameter and INSERTs it. ``read_back`` then SELECTs the
rows and re-renders them in the same canonical form for an order-insensitive
multiset compare.

The schema mixes types on purpose — a VARSIZED column carrying embedded commas /
quotes, a FLOAT64, and a nullable VARSIZED — so the round-trip proves the typed
path is correct where the old CSV-text sink was not: a comma inside a quoted
field is one column (not two), and an empty field is SQL NULL (not ``''``).

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
# of `configs/valid/basic.nesql`. `note` is the only nullable column.
_SCHEMA: tuple[tuple[str, str, bool], ...] = (
    ("id", "INT32", False),
    ("label", "VARSIZED", False),
    ("score", "FLOAT64", False),
    ("note", "VARSIZED", True),
)
_COLUMNS = tuple(name for name, _typ, _nullable in _SCHEMA)

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
    comparison is type-aware: an empty field is NULL, a quoted ``"a,b"`` is one
    string column, and floats compare by value. The harness, fed the same raw
    fixture via ``--input-path``, builds the matching native tuples.
    """

    def load(self, src: Any) -> ByteData:
        """Parse the bound CSV source (RFC-4180) into canonical typed rows."""
        text = src.bytes().decode("utf-8")
        rows: list[bytes] = []
        for cells in csv.reader(io.StringIO(text)):
            if not cells:
                continue
            values: list[Any] = []
            for (_name, typ, nullable), cell in zip(_SCHEMA, cells, strict=False):
                if typ == "VARSIZED":
                    # An empty *unquoted* field is NULL; the fixture never uses a
                    # quoted empty string, so csv's lost quote/empty distinction
                    # does not matter (matches the harness's NULL convention).
                    values.append(None if (nullable and cell == "") else cell)
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
        return (
            f'CREATE TABLE IF NOT EXISTS "{target}" '
            "(id INTEGER, label TEXT, score DOUBLE PRECISION, note TEXT)"
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
