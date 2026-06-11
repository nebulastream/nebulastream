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

Sink-side flow (inverse of the ODBC source): the framework GENERATES typed rows from
the CREATE SINK schema and feeds them to the harness as JSONL; the harness decodes that
JSONL into native engine buffers (this is a NATIVE sink — ``OUTPUT_FORMAT 'Native'`` in
the config) and drains them through ODBCSink, which binds each column to a typed ODBC
parameter and INSERTs it. The same rows are the read-back oracle. There is no CSV
anywhere on this path, and the loader does no encoding of its own.

This loader is pure backend IO. It carries the only connector-specific knowledge
about the schema — how each NES type lands as a Postgres column (``_NES_TO_PG``)
— and:
  * ``setup`` creates the per-test table from the schema (the framework parses
    it from the CREATE SINK and hands it to us), and
  * ``read_back`` SELECTs the rows the sink INSERTed and returns them as typed
    Python tuples in schema-column order.
Generation, canonicalization, and the type-aware multiset compare all live in the
framework — so adding a new valid config (a new schema) works out of the box.

ODBCSink connects eagerly in ``start()``, so an unreachable endpoint surfaces as
CannotOpenSink (4004) at the open phase — the framework default for
``bad_endpoint`` — hence no ``BAD_ENDPOINT_ERROR_CODE`` override.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import psycopg

from conntest_runner.contracts import SinkLoader
from conntest_runner.endpoint import split_endpoint

if TYPE_CHECKING:
    from conntest_runner.datamodel import NativeColumn

# NES type -> Postgres column type. The sink INSERTs positionally and lets the
# database coerce, so only width/coercion-compatibility matters (e.g. unsigned
# NES types widen to the next signed SQL type that holds their range). This is
# the inverse of ODBCSink's `nesToSql` read table, expressed for DDL.
_NES_TO_PG: dict[str, str] = {
    "INT8": "SMALLINT",
    "UINT8": "SMALLINT",
    "INT16": "SMALLINT",
    "UINT16": "INTEGER",
    "INT32": "INTEGER",
    "UINT32": "BIGINT",
    "INT64": "BIGINT",
    "UINT64": "NUMERIC(20)",
    "FLOAT32": "REAL",
    "FLOAT64": "DOUBLE PRECISION",
    "BOOLEAN": "BOOLEAN",
    "CHAR": "CHAR(1)",
    "VARSIZED": "TEXT",
}


def _build_dsn(host: str, port: int | str, *, dbname: str, user: str, password: str) -> str:
    """Libpq DSN string from parts."""
    return f"host={host} port={port} dbname={dbname} user={user} password={password}"


def _execute(dsn_str: str, sql: str) -> None:
    """Run a single statement (DDL or DML) and commit."""
    with psycopg.connect(dsn_str) as cx, cx.cursor() as cur:
        cur.execute(sql)
        cx.commit()


class OdbcSinkLoader(SinkLoader):
    """Per-plugin TestDataLoader for the ODBC sink over Postgres.

    The class name is conventional, not load-bearing: discovery picks the single
    duck-typed match in this module.
    """

    def __init__(self, endpoint: str) -> None:
        self._endpoint = endpoint
        self._host, self._port = split_endpoint(endpoint)

    def config_overrides(self, *, target: str, endpoint: str) -> dict[str, str]:
        """Config keys the framework swaps in by value.

        Point it at ``endpoint`` (host:port) and the per-test ``target`` table.
        Credentials stay as the config authored them (they match compose). The
        sink's table is built from the schema in ``setup``; here only the per-test
        table name is swapped.
        """
        host, port = split_endpoint(endpoint)
        return {"db_host": host, "db_port": str(port), "table": target}

    def setup(self, *, target: str, schema: list[NativeColumn]) -> None:
        """(Re)create the per-test target table from the sink schema, idempotently.

        Columns mirror the CREATE SINK in order (ODBCSink INSERTs positionally),
        each mapped to the Postgres type that holds its NES type's range.
        ODBCSink's INSERTs (issued during the WRITE drain) need the table to
        exist. The DROP makes a re-run against a surviving Postgres container
        start from an empty table, so read_back sees only this run's rows.
        """
        columns = ", ".join(f'"{name}" {_NES_TO_PG[typ]}' for name, typ, _nullable in schema)
        _execute(self._dsn(), f'DROP TABLE IF EXISTS "{target}"')
        _execute(self._dsn(), f'CREATE TABLE "{target}" ({columns})')

    def read_back(
        self,
        *,
        target: str,
        schema: list[NativeColumn],
        expect_records: int | None = None,  # noqa: ARG002  # a SELECT is synchronous and complete
        timeout_s: float = 30.0,  # noqa: ARG002  # unused (synchronous read)
    ) -> list[tuple[Any, ...]]:
        """Return the rows the sink INSERTed as typed tuples, in schema order.

        No ``ORDER BY`` — the framework compares an order-insensitive multiset.
        Returns raw typed values (int/float/str/bool); the framework coerces and
        canonicalizes them against the generated oracle, so this loader does no
        encoding of its own.
        """
        projection = ", ".join(f'"{name}"' for name, _typ, _nullable in schema)
        with psycopg.connect(self._dsn()) as cx, cx.cursor() as cur:
            cur.execute(f'SELECT {projection} FROM "{target}"')  # noqa: S608  # trusted static test schema
            return list(cur.fetchall())

    def _dsn(self) -> str:
        return _build_dsn(
            self._host,
            self._port,
            dbname="nebulastream",
            user="nes",
            password="nes",  # noqa: S106  # test credential
        )
