# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""datamodel.py — the source⟂model layer.

Two orthogonal axes, each varying independently (design §9):

  * **Data source** — where bytes/rows come from: ``File(path)`` (default)
    or ``Generate(count, seed)`` (synthetic).
  * **Data model / shape** — ``ByteModel`` (TCP/MQTT… byte streams + CSV
    sink record streams) or ``RowModel(schema)`` (ODBC/JDBC native tuple
    layout), implemented ONCE here and *declared* by a connector
    (``loader.data_model = …``).

This is what kills the old anti-pattern where the rows/bytes distinction
was smeared across the loader (``make_fixture``/``ROW_WIDTH``), the
harness (``--native-row-width``), and the test (``_get_fixture``): the
native tuple layout now lives in exactly one place, ``RowModel``.

A model turns a bound data source into a *dataset*, says how big a FILL
(source) or WRITE (sink) quota the dataset implies, and compares an
observed result against it. ``ByteModel`` compares via the reply SHA
(authoritative for an order-preserving byte stream); ``RowModel`` decodes
the observed native bytes and compares the multiset of rows
(order-insensitive — a DB SELECT may reorder).
"""

from __future__ import annotations

import hashlib
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    import builtins

    from conntest_runner.protocol import Got

# ── schema → native struct layout ──────────────────────────────────────
#
# The NES native tuple layout is packed little-endian with no padding
# (``struct`` format prefix ``<``). This is the single owner of the
# rows↔native-bytes mapping
_TYPE_TO_STRUCT: dict[str, str] = {
    "INT8": "b",
    "UINT8": "B",
    "INT16": "h",
    "UINT16": "H",
    "INT32": "i",
    "UINT32": "I",
    "INT64": "q",
    "UINT64": "Q",
    "FLOAT32": "f",
    "FLOAT64": "d",
}

# Above this size a full byte-for-byte / row materialization is wasteful;
# byte streams fall back to SHA-only and row streams to an order-insensitive
# per-row digest. Lifted from the old test_conformance threshold.
_LARGE_THRESHOLD_BYTES = 1_000_000


def assert_multiset_equal(observed: list[Any], expected: list[Any]) -> None:
    """Assert two record collections are equal as multisets (order-blind).

    Raises AssertionError with a compact diff on mismatch.
    """
    obs = sorted(observed)
    exp = sorted(expected)
    if obs == exp:
        return
    only_obs = [x for x in obs if x not in exp]
    only_exp = [x for x in exp if x not in obs]
    raise AssertionError(
        f"multiset mismatch: {len(observed)} observed vs {len(expected)} "
        f"expected\n  only in observed: {only_obs[:8]}\n  "
        f"only in expected: {only_exp[:8]}"
    )


# ── bound data sources ─────────────────────────────────────────────────


class DataSource(Protocol):
    """The shape a model loads from: a bound source yields bytes or rows."""

    def bytes(self) -> bytes:
        """All source bytes, in order."""
        ...

    def rows(self, schema: list[tuple[str, str]]) -> list[tuple[float | int, ...]]:
        """Source rows decoded under ``schema``."""
        ...


class File:
    """A file-backed data source, relative to the plugin's ``conn-test/`` dir.

    Declared in scenarios.py (e.g. ``File("input/sample.csv")``); the runner
    ``bind()``s it to that dir before a model loads it.
    """

    def __init__(self, rel_path: str) -> None:
        self.rel_path = rel_path

    def bind(self, base_dir: Path) -> BoundFile:
        """Bind to ``base_dir``, yielding a `BoundFile` over the joined path."""
        return BoundFile(Path(base_dir) / self.rel_path)


class _Records:
    """A data source over an explicit list of whole records (bytes).

    Backs the head()/tail() splits a reconnect scenario seeds in two batches.
    """

    def __init__(self, records: list[bytes]) -> None:
        self._records = records

    def bytes(self) -> bytes:
        if not self._records:
            return b""
        return b"\n".join(self._records) + b"\n"

    def rows(self, schema: list[tuple[str, str]]) -> list[tuple[float | int, ...]]:
        return _parse_csv_rows(self.bytes().decode("utf-8"), schema)

    def records(self) -> list[builtins.bytes]:
        return list(self._records)

    def head(self) -> _Records:
        return _Records(self._records[: (len(self._records) + 1) // 2])

    def tail(self) -> _Records:
        return _Records(self._records[(len(self._records) + 1) // 2 :])


class BoundFile:
    """A `File` bound to an absolute path, ready for a model to load."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def bytes(self) -> bytes:
        """Whole file contents as bytes."""
        return self.path.read_bytes()

    def rows(self, schema: list[tuple[str, str]]) -> list[tuple[float | int, ...]]:
        """Parse the file as CSV into native tuples under ``schema``."""
        return _parse_csv_rows(self.path.read_text(encoding="utf-8"), schema)

    def records(self) -> list[builtins.bytes]:
        """Non-blank lines as whole records."""
        return [r for r in self.bytes().split(b"\n") if r.strip()]

    def head(self) -> _Records:
        """First half of the records (see `_Records.head`)."""
        return _Records(self.records()).head()

    def tail(self) -> _Records:
        """Second half of the records (see `_Records.tail`)."""
        return _Records(self.records()).tail()


class Generate:
    """A synthetic data source: deterministic for a given (count, seed)."""

    def __init__(self, count: int, seed: int = 0) -> None:
        self.count = count
        self.seed = seed

    def bind(self, _base_dir: Path) -> Generate:
        """Bind is a no-op: a synthetic source is base-independent."""
        return self  # base-independent

    def bytes(self) -> bytes:
        """Deterministic pseudo-random bytes for ``(count, seed)``."""
        # Deterministic pseudo-random bytes (no Generate-of-bytes connector
        # exercises this yet, but the axis is first-class by design).
        out = bytearray()
        x = (self.seed + 1) & 0xFFFFFFFF
        for _ in range(self.count):
            x = (1103515245 * x + 12345) & 0xFFFFFFFF
            out.append(x & 0xFF)
        return bytes(out)

    def rows(self, schema: list[tuple[str, str]]) -> list[tuple[float | int, ...]]:
        """Deterministic native tuples for ``(count, seed)`` under ``schema``."""
        rows: list[tuple[float | int, ...]] = []
        for i in range(self.count):
            row: list[float | int] = []
            for col, (_name, typ) in enumerate(schema):
                v = self.seed + i * (col + 1)
                row.append(float(v) if typ.startswith("FLOAT") else int(v))
            rows.append(tuple(row))
        return rows

    def head(self) -> Generate:
        """First half: the same seed, half the count."""
        return Generate(count=(self.count + 1) // 2, seed=self.seed)

    def tail(self) -> Generate:
        """Second half: a shifted seed disjoint from the head's data."""
        # The second half: shift the seed so head() and tail() cover disjoint
        # data (matters only for connectors that dedup on content).
        half = (self.count + 1) // 2
        return Generate(count=self.count - half, seed=self.seed + half)


def _parse_csv_rows(text: str, schema: list[tuple[str, str]]) -> list[tuple[float | int, ...]]:
    rows: list[tuple[float | int, ...]] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        cells = line.split(",")
        if len(cells) != len(schema):
            raise ValueError(f"CSV row has {len(cells)} cells, schema has {len(schema)}: {line!r}")
        row: list[float | int] = []
        for cell, (_name, typ) in zip(cells, schema, strict=False):
            row.append(float(cell) if typ.startswith("FLOAT") else int(cell))
        rows.append(tuple(row))
    return rows


# ── datasets ───────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ByteData:
    """A loaded byte-stream dataset."""

    data: bytes

    def records(self) -> list[builtins.bytes]:
        """Whole records (non-blank lines).

        The unit a sink WRITE counts and read_back compares.
        """
        return [r for r in self.data.split(b"\n") if r]


@dataclass(frozen=True)
class RowData:
    """A loaded native-tuple dataset and its expected native encoding."""

    rows: list[tuple[float | int, ...]]
    native: bytes  # expected native-encoded bytes


# ── data models ────────────────────────────────────────────────────────


class ByteModel:
    """Byte-stream shape: TCP/MQTT sources, CSV sink record streams.

    A source round-trip is verified by SHA over the observed bytes (the
    harness reply carries it) — authoritative for an order-preserving
    stream, so no observed bytes need shipping. A sink round-trip compares
    the multiset of records read back.
    """

    needs_observed = False
    native_row_width: int | None = None

    def load(self, src: DataSource) -> ByteData:
        """Load a bound source into a `ByteData` dataset."""
        return ByteData(src.bytes())

    def harness_input(self, bound: DataSource) -> bytes:
        """Bytes the harness reads via ``--input-path``.

        Default: the source bytes verbatim — a formatted sink packs these as
        records. A native sink overrides this to hand over a native ``.nes``
        container the harness loads directly (see ODBCSink's model).
        """
        return bound.bytes()

    def fill_quota(self, d: ByteData) -> int:
        """FILL quota in the native unit (bytes)."""
        return len(d.data)  # native unit = bytes

    def write_quota(self, d: ByteData) -> int:
        """WRITE quota in records."""
        return len(d.records())

    def compare_fill(self, d: ByteData, *, got: Got, observed: bytes) -> None:  # noqa: ARG002  # observed is part of the shared model interface; ByteModel verifies via SHA
        """Verify a source FILL by reply SHA (authoritative for byte order)."""
        expected_sha = hashlib.sha256(d.data).hexdigest()
        assert got.sha == expected_sha, (  # noqa: S101  # the assertion is the comparison check
            f"source round-trip SHA mismatch: got {got.sha} "
            f"expected {expected_sha} ({got.bytes}/{len(d.data)} bytes)"
        )
        assert got.bytes == len(d.data), f"observed {got.bytes} bytes, expected {len(d.data)}"  # noqa: S101  # the assertion is the comparison check

    def compare_readback(self, d: ByteData, received: list[bytes]) -> None:
        """Compare a sink read-back as a multiset of records."""
        assert_multiset_equal(received, d.records())


class RowModel:
    """Native-tuple shape: ODBC/JDBC/Arrow sources.

    Owns the NES native tuple layout (the single home of rows↔native-bytes).
    A source round-trip decodes the observed native bytes and compares the
    multiset of rows (order-insensitive — a SELECT may reorder).
    """

    needs_observed = True

    def __init__(self, schema: list[tuple[str, str]]) -> None:
        self.schema = schema
        fmt = "<" + "".join(_TYPE_TO_STRUCT[typ] for _name, typ in schema)
        self._struct = struct.Struct(fmt)

    @property
    def native_row_width(self) -> int:
        """Packed width of one native tuple, in bytes."""
        return self._struct.size

    def encode(self, rows: list[tuple[float | int, ...]]) -> bytes:
        """Pack rows into the NES native little-endian layout."""
        return b"".join(self._struct.pack(*row) for row in rows)

    def decode(self, buf: bytes) -> list[tuple[Any, ...]]:
        """Unpack native bytes back into tuples (one per row width)."""
        width = self._struct.size
        count = len(buf) // width
        return [self._struct.unpack_from(buf, i * width) for i in range(count)]

    def load(self, src: DataSource) -> RowData:
        """Load a bound source into a `RowData` dataset (rows + native bytes)."""
        rows = src.rows(self.schema)
        return RowData(rows=rows, native=self.encode(rows))

    def fill_quota(self, d: RowData) -> int:
        """FILL quota in the native unit (tuple count)."""
        return len(d.rows)  # native unit = tuple count

    def compare_fill(self, d: RowData, *, got: Got, observed: bytes) -> None:
        """Verify a source FILL by decoding native bytes and multiset compare."""
        assert got.count == len(d.rows), f"observed {got.count} tuples, expected {len(d.rows)}"  # noqa: S101  # the assertion is the comparison check
        observed_rows = self.decode(observed)
        # tuple vs tuple, normalized to lists for a stable sort/compare
        assert_multiset_equal([list(r) for r in observed_rows], [list(r) for r in d.rows])
