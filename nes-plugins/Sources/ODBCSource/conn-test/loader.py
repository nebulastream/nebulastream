# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""ODBC TestDataLoader for the conn-test harness (timestamp-windowed source).

The ODBC source is a NATIVE-formatter, timestamp-windowed poller: it runs a
user ``query`` whose two ``?`` parameters bind a ``(watermark, now]`` window,
writes the projected columns into the buffer in NES MemoryLayout order, and
advances the watermark. The windowing column (``created_at``) is filtered on
but not projected, so the emitted tuple is deterministic.

The projected schema (``k INT64, a INT32, b INT64, c FLOAT64`` → ``<qiqd``,
28 bytes) is parsed from the config's ``CREATE LOGICAL SOURCE`` by the
framework now, not hardcoded here: the loader is pure backend
IO (create the per-test table from that schema, INSERT the generated rows, point
the config at the endpoint). The framework hands ``setup`` the parsed schema, so
the table DDL is derived from it (``_NES_TO_PG``) rather than written by hand.
The loader seeds Postgres directly over TCP via psycopg; the harness reaches the
same Postgres through the ``PostgreSQL Unicode`` ODBC driver.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import psycopg

from conntest_runner.contracts import SourceLoader
from conntest_runner.endpoint import split_endpoint

if TYPE_CHECKING:
    from collections.abc import Iterable

    from conntest_runner.datamodel import NativeColumn

# NES type -> Postgres column type, for the per-test DDL derived from the parsed
# schema. Unsigned NES types widen to the next signed SQL type that holds their
# range (mirrors ODBCSink's ``_NES_TO_PG``).
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


def _insert_rows(
    dsn_str: str,
    table: str,
    columns: tuple[str, ...],
    rows: Iterable[tuple[float | int, ...]],
) -> None:
    """``INSERT INTO <table> (<columns>) VALUES (...)`` for every row, then commit."""
    cols = ", ".join(f'"{c}"' for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f'INSERT INTO "{table}" ({cols}) VALUES ({placeholders})'  # noqa: S608  # trusted static test-data SQL
    with psycopg.connect(dsn_str) as cx, cx.cursor() as cur:
        cur.executemany(sql, list(rows))
        cx.commit()


class PostgresOdbcLoader(SourceLoader):
    """Per-plugin TestDataLoader for the ODBC source over Postgres.

    The class name is conventional, not load-bearing: discovery picks the
    single duck-typed match in this module.
    """

    def __init__(self, endpoint: str) -> None:
        self._endpoint = endpoint
        self._host, self._port = split_endpoint(endpoint)
        # The schema the framework parses from the config, cached by setup() so
        # seed_batch() can INSERT into the projected columns by name.
        self._schema: list[NativeColumn] = []

    def config_overrides(self, *, target: str, endpoint: str) -> dict[str, str]:  # noqa: ARG002  # `target` is part of the Loader contract; the ODBC source's table is repointed from the query, not a config key
        """Point the pristine config at ``endpoint``.

        The per-test ``target`` is not a config key: the framework repoints the
        table named in the authored ``query`` to it.
        """
        host, port = split_endpoint(endpoint)
        return {
            "db_host": host,
            "db_port": str(port),
        }

    def _create_sql(self, target: str, schema: list[NativeColumn]) -> str:
        """Per-test table DDL derived from the parsed schema, plus windowing.

        Each projected column maps to a Postgres type via ``_NES_TO_PG`` and is
        NOT NULL unless the schema marks it nullable. No PRIMARY KEY: the
        generated first-column values are not guaranteed unique for an arbitrary
        schema, and the source's windowed SELECT does not need one. The
        ``created_at`` column the source filters on is not projected (so not in
        ``schema``); it is appended as a literal and defaults to insert time, so
        seeded rows fall inside the first ``(epoch, now]`` poll window.
        """
        cols = [
            f'"{name}" {_NES_TO_PG[typ]}{"" if nullable else " NOT NULL"}'
            for name, typ, nullable in schema
        ]
        cols.append("created_at TIMESTAMP NOT NULL DEFAULT now()")
        return f'CREATE TABLE "{target}" ({", ".join(cols)})'

    def setup(self, *, target: str, schema: list[NativeColumn]) -> None:
        """(Re)create the per-test table from the schema before OPEN, idempotently.

        The source's open() SQLPrepares the SELECT, which needs the table to
        exist. The DROP makes a re-run against a surviving Postgres container
        start from an empty table (no duplicate rows from a prior seed). The
        schema is cached so seed_batch can INSERT by column name.
        """
        self._schema = schema
        _execute(self._dsn(), f'DROP TABLE IF EXISTS "{target}"')
        _execute(self._dsn(), self._create_sql(target, schema))

    def seed_batch(self, rows: Iterable[tuple[float | int, ...]], *, target: str) -> None:
        """Insert a batch of generated rows into ``target`` (created by ``setup``).

        ``created_at`` defaults to now(), inside the first poll window. The framework
        hands the generated typed rows in projected schema-column order; this NATIVE
        source INSERTs them directly (no wire encoding — the engine reads typed tuples).
        """
        columns = tuple(name for name, _typ, _nullable in self._schema)
        _insert_rows(self._dsn(), target, columns, rows)

    def _dsn(self) -> str:
        return _build_dsn(
            self._host,
            self._port,
            dbname="nebulastream",
            user="nes",
            password="nes",  # noqa: S106  # test credential
        )
