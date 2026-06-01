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

The native tuple layout (``k INT64, a INT32, b INT64, c FLOAT64`` →
``<qiqd``, 28 bytes) is owned by the framework's ``RowModel`` now, not this
loader: the loader is pure backend IO (create table, INSERT rows, point the
config at the endpoint). The loader seeds Postgres directly over TCP via
psycopg; the harness reaches the same Postgres through the ``PostgreSQL
Unicode`` ODBC driver.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

import psycopg

from conntest_runner.datamodel import RowModel
from conntest_runner.naming import unique_token

if TYPE_CHECKING:
    from collections.abc import Iterable

    from conntest_runner.datamodel import RowData

# The projected schema, in MemoryLayout order. The framework's RowModel turns
# this into the native <qiqd 28-byte layout (and back, decoding observed bytes).
_SCHEMA = [("k", "INT64"), ("a", "INT32"), ("b", "INT64"), ("c", "FLOAT64")]


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


class PostgresOdbcLoader:
    """Per-plugin TestDataLoader for the ODBC source over Postgres.

    The class name is conventional, not load-bearing: discovery picks the
    single duck-typed match in this module.
    """

    system: ClassVar[str] = "postgres"
    compose_service: ClassVar[str] = "db"
    compose_port: ClassVar[int] = 5432

    # The ODBC source is a NATIVE-tuple connector: RowModel owns the
    # rows↔native-bytes layout and the multiset round-trip compare.
    data_model: ClassVar[RowModel] = RowModel(_SCHEMA)

    def __init__(self, endpoint: str) -> None:
        self._endpoint = endpoint
        host, _, port = endpoint.rpartition(":")
        if not host or not port:
            raise ValueError(f"PostgresOdbcLoader: endpoint must be 'host:port', got {endpoint!r}")
        self._host, self._port = host, int(port)

    def make_target(self, *, test_id: str, scenario: str, worker_id: str) -> str:
        """Per-test Postgres TABLE name (``ct_`` prefix)."""
        return "ct_" + unique_token(test_id, scenario, worker_id)

    def config_overrides(self, *, target: str, endpoint: str) -> dict[str, str]:
        """Point the pristine config at ``endpoint`` and the per-test ``target``.

        The target appears in ``sync_table`` and (embedded) ``query``.
        """
        host, _, port = endpoint.rpartition(":")
        return {
            "host": host,
            "server": host,
            "port": port,
            "sync_table": target,
            "query": (
                f"SELECT k, a, b, c FROM {target} "  # noqa: S608  # trusted static test-data SQL
                "WHERE created_at > ? AND created_at <= ? ORDER BY k DESC"
            ),
        }

    def _create_sql(self, target: str) -> str:
        """Per-test table DDL: projected columns plus the windowing column.

        The ``created_at`` column the source filters on is defaulted to insert
        time, so seeded rows fall inside the first ``(epoch, now]`` poll window.
        """
        return (
            f'CREATE TABLE IF NOT EXISTS "{target}" ('
            "k BIGINT PRIMARY KEY, a INTEGER NOT NULL, b BIGINT NOT NULL, "
            "c DOUBLE PRECISION NOT NULL, "
            "created_at TIMESTAMP NOT NULL DEFAULT now())"
        )

    def setup(self, *, target: str) -> None:
        """Create the per-test table before OPEN.

        The source's open() SQLPrepares the SELECT, which needs the table to exist.
        """
        _execute(self._dsn(), self._create_sql(target))

    def seed(self, data: RowData, *, target: str) -> None:
        """Insert the dataset's rows into ``target``.

        ``created_at`` defaults to now(), inside the first poll window.
        """
        _execute(self._dsn(), self._create_sql(target))
        _insert_rows(self._dsn(), target, ("k", "a", "b", "c"), data.rows)

    def _dsn(self) -> str:
        return _build_dsn(
            self._host,
            self._port,
            dbname="nebulastream",
            user="nes",
            password="nes",  # noqa: S106  # test credential
        )
