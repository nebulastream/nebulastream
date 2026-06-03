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
import re
import struct
from collections import Counter
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
    # A formatted (byte-stream) sink: read_back returns raw record bytes and the
    # framework verifies them as a record multiset. NativeModel flips this.
    is_native = False

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

    # -- shape-blind sink-catalog hooks -------------------------------------
    # The sink scenarios drive datasets through these so the catalog stays
    # model-blind: a byte stream measures/slices/compares in records;
    # NativeModel does the same in typed rows.
    def input_record_count(self, bound: Any) -> int:
        """Records the harness will materialize from ``bound`` (for buffer sizing)."""
        return len(bound.records())

    def record_count(self, d: ByteData) -> int:
        """Records in a loaded dataset."""
        return len(d.records())

    def prefix(self, d: ByteData, n: int) -> ByteData:
        """The first ``n`` records of ``d`` as a dataset (outage_loss's kept batch)."""
        kept = d.records()[:n]
        return ByteData(b"\n".join(kept) + (b"\n" if kept else b""))

    def combine(self, datasets: list[ByteData]) -> ByteData:
        """Concatenate the records of every WRITE into one expected dataset."""
        records = [r for d in datasets for r in d.records()]
        return ByteData(b"\n".join(records) + (b"\n" if records else b""))

    def verify_readback(self, expected: ByteData, received: list[bytes]) -> None:
        r"""Multiset-compare a sink read-back against ``expected``.

        A sink publishes a TupleBuffer batch as one payload, so split each
        read-back payload on ``\n`` into the whole records the compare counts.
        """
        flat = [r for p in received for r in p.split(b"\n") if r.strip()]
        self.compare_readback(expected, flat)


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


# ── native sinks: schema-driven generator + `.nes` writer + typed compare ──
#
# A native sink (e.g. ODBCSink, OUTPUT_FORMAT=Native) consumes the engine's
# native TupleBuffer layout. The single source of truth is `NativeModel`: from
# a schema (parsed out of the CREATE SINK) plus a size, it GENERATES typed rows
# once, then serializes them two ways — a `.nes` container the harness loads and
# drains through the sink, and the same rows kept in memory as the read-back
# oracle. Both serializations derive from one in-memory list, so the oracle
# cannot drift from what the sink was fed. The connector loader stays out of it:
# it only maps the schema to its backend's DDL and SELECTs the rows back as
# typed tuples — the framework owns generation, encoding, and the compare.

# `(name, NES type, nullable)`, in MemoryLayout (declaration) order.
NativeColumn = tuple[str, str, bool]

# NES fixed-width field → little-endian struct code. VARSIZED is special (a
# 16-byte VariableSizedAccess slot + an appended child payload); CHAR is a raw
# byte we pack/unpack by hand.
_FIXED_STRUCT: dict[str, str] = {
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
    "BOOLEAN": "?",
    "CHAR": "c",
}
# A VARSIZED field occupies 16 bytes in the row: a `VariableSizedAccess`
# (offset:u32, index:u32, size:u64). On load the engine rebuilds offset/index
# from the child buffer it allocates, so only `size` is load-bearing on disk.
_VARSIZED_WIDTH = 16
_VARSIZED_SLOT = struct.Struct("<IIQ")

# `.nes` container headers — verbatim from FileUtil.hpp / NativeFixture.hpp
# (the writer/loader of the committed fixtures). Little-endian, packed.
_NES_FILE_HEADER = struct.Struct("<QQ")  # numberOfBuffers, sizeOfBuffersInBytes
_NES_BUFFER_HEADER = struct.Struct("<QQQQ")  # numTuples, numChildBuffers, seqNo, chunkNo
# The writer bounds each buffer's fixed region to this many bytes (one `.nes`
# "buffer" == one chunk the harness steps as a WRITE unit). Matching the C++
# writer's 4096 keeps a generated dataset multi-buffer, which the outage/
# reconnect scenarios need (write one buffer, cut the link, write the rest).
_NES_BUFFER_BYTES = 4096
# SequenceNumber::INITIAL == ChunkNumber::INITIAL == 1 (Identifiers.hpp). The
# sink ignores both; we set them faithfully so the `.nes` is a real artifact.
_SEQ_INITIAL = 1
_CHUNK_INITIAL = 1

_CREATE_SINK_RE = re.compile(
    r"CREATE\s+SINK\s+\S+\s*\((?P<cols>[^)]*)\)\s*TYPE\b", re.IGNORECASE | re.DOTALL
)


def _value_width(typ: str) -> int:
    """Bytes the value of ``typ`` occupies (without any null flag)."""
    return _VARSIZED_WIDTH if typ == "VARSIZED" else struct.calcsize(_FIXED_STRUCT[typ])


def _field_width(typ: str, nullable: bool) -> int:
    """Bytes a field of ``typ`` occupies in the packed row layout.

    A nullable field carries a leading null-flag byte (engine row layout: the
    flag precedes the value, matching ODBCSink's ``value = field + nullable``),
    so it is one byte wider than its NOT NULL counterpart.
    """
    return _value_width(typ) + (1 if nullable else 0)


def parse_sink_schema(nesql: str) -> list[NativeColumn]:
    """Parse the column list of a ``CREATE SINK <name> (...) TYPE ...`` statement.

    Returns ``(name, NES type, nullable)`` per column, in declaration order — the
    same order the engine lays out a native tuple and the sink INSERTs
    positionally. Names are unquoted (backticks/quotes stripped); types are
    upper-cased. Raises ``ValueError`` if no CREATE SINK schema is found or a
    column is malformed.
    """
    m = _CREATE_SINK_RE.search(nesql)
    if not m:
        raise ValueError("no `CREATE SINK <name> (...) TYPE ...` schema found in config")
    columns: list[NativeColumn] = []
    for raw in m.group("cols").split(","):
        cell = raw.strip()
        if not cell:
            continue
        tokens = cell.split()
        if len(tokens) < 2:  # noqa: PLR2004  # a column is at least `<name> <TYPE>`
            raise ValueError(f"malformed sink column {cell!r}: expected `<name> <TYPE> [NOT NULL]`")
        name = tokens[0].strip('`"')
        typ = tokens[1].upper()
        nullable = "NOT NULL" not in " ".join(tokens[2:]).upper()
        columns.append((name, typ, nullable))
    if not columns:
        raise ValueError("CREATE SINK schema has no columns")
    return columns


@dataclass(frozen=True)
class NativeData:
    """A loaded native dataset: the generated typed rows (the read-back oracle)."""

    rows: list[tuple[Any, ...]]


def _gen_value(name: str, typ: str, i: int, seed: int, col: int, nullable: bool) -> Any:
    """Deterministic value for row ``i``, column ``col`` of ``typ`` under ``seed``.

    Small, type-correct, and round-trip-safe by construction: ints stay in
    [0, 1000) so they fit every integer width and its SQL column; floats stay
    in [0, 1000) (and FLOAT32 is pre-rounded to single precision so the `.nes`
    payload and the oracle hold the exact value a REAL column returns); VARSIZED
    is a short ASCII string; CHAR a single 'A'-'Z'. A nullable column is NULL on
    a deterministic ~1-in-5 of its rows (so both the NULL and the value path are
    exercised), returned as ``None``.
    """
    n = (seed * 1_000_003 + i * 97 + col * 7) & 0x7FFFFFFF
    if nullable and n % 5 == 2:  # noqa: PLR2004  # ~20% NULL, deterministic
        return None
    if typ == "VARSIZED":
        return f"{name[:12]}-{i}"
    if typ == "CHAR":
        return chr(ord("A") + (n % 26))
    if typ == "BOOLEAN":
        return (n % 2) == 0
    if typ == "FLOAT32":
        v = (n % 100_000) / 100.0
        return struct.unpack("<f", struct.pack("<f", v))[0]  # round-trip to single precision
    if typ == "FLOAT64":
        return (n % 100_000) / 100.0
    return n % 1000  # any integer type


class NativeModel:
    """Native-tuple sink shape: generate typed rows, write a `.nes`, compare rows.

    Declared schema-less by the connector (``data_model = NativeModel()``); the
    framework parses the schema from the CREATE SINK under test and ``bind``s a
    concrete instance per case. The same generated rows are encoded to the `.nes`
    the harness drains (``harness_input``) and kept as the oracle (``load``), so a
    new valid config "just works": its schema drives both sides.
    """

    needs_observed = False
    native_row_width: int | None = None
    is_native = True

    def __init__(self, schema: list[NativeColumn] | None = None, *, seed: int = 1) -> None:
        self.schema = schema
        self.seed = seed
        # A nullable VARSIZED is unsupported: the native loader reads each
        # VariableSizedAccess at the field's start offset (its prefix-sum walk
        # uses getSizeInBytesWithNull), so a leading null-flag byte would shift
        # the access by one. Nullable scalars are fine (their flag is just one
        # more byte the loader copies verbatim). Fail loudly rather than emit a
        # silently-misaligned `.nes`.
        bad = [n for n, t, nullable in (schema or []) if nullable and t == "VARSIZED"]
        if bad:
            raise ValueError(f"NativeModel: nullable VARSIZED columns are unsupported: {bad}")

    def bind(self, schema: list[NativeColumn]) -> NativeModel:
        """Return a schema-bound model for one config case."""
        return NativeModel(schema, seed=self.seed)

    # -- generation (the single source of truth) ----------------------------
    def _rows(self, count: int, seed: int) -> list[tuple[Any, ...]]:
        assert self.schema is not None, "NativeModel used before bind(schema)"  # noqa: S101
        return [
            tuple(
                _gen_value(name, typ, i, seed, col, nullable)
                for col, (name, typ, nullable) in enumerate(self.schema)
            )
            for i in range(count)
        ]

    def _seed_of(self, bound: Any) -> int:
        return int(getattr(bound, "seed", self.seed))

    def load(self, src: Any) -> NativeData:
        """Generate the dataset a sink WRITE will deliver (== the `.nes` rows)."""
        return NativeData(self._rows(int(src.count), self._seed_of(src)))

    def harness_input(self, bound: Any) -> bytes:
        """Encode the generated rows into a `.nes` container for ``--input-path``."""
        return self.encode(self._rows(int(bound.count), self._seed_of(bound)))

    def input_record_count(self, bound: Any) -> int:
        """Tuples the harness will materialize (for buffer-count sizing)."""
        return int(bound.count)

    # -- `.nes` writer -------------------------------------------------------
    def encode(self, rows: list[tuple[Any, ...]]) -> bytes:
        """Serialize ``rows`` into the native `.nes` TupleBuffer container.

        Mirrors FileUtil.hpp's writer: a file header, then one buffer per chunk
        (its fixed region bounded to ``_NES_BUFFER_BYTES``) holding the packed
        fixed-width tuples followed by the VARSIZED child payloads in
        tuple-major, field order. The on-disk VARSIZED slot carries only its
        ``size``; the loader rebuilds the child index/offset.
        """
        assert self.schema is not None, "NativeModel used before bind(schema)"  # noqa: S101
        stride = sum(_field_width(t, nullable) for _n, t, nullable in self.schema)
        num_varsized = sum(1 for _n, t, _x in self.schema if t == "VARSIZED")
        per_chunk = max(1, _NES_BUFFER_BYTES // stride)
        chunks = [rows[k : k + per_chunk] for k in range(0, len(rows), per_chunk)] or [[]]

        out = bytearray(_NES_FILE_HEADER.pack(len(chunks), _NES_BUFFER_BYTES))
        for ci, chunk in enumerate(chunks):
            fixed = bytearray()
            payloads = bytearray()
            for row in chunk:
                for (_name, typ, nullable), value in zip(self.schema, row, strict=True):
                    if nullable:
                        # Leading null-flag byte (1 = NULL), then the value region
                        # — which is zeroed (and contributes no payload) when NULL.
                        fixed += b"\x01" if value is None else b"\x00"
                        if value is None:
                            fixed += b"\x00" * _value_width(typ)
                            continue
                    if typ == "VARSIZED":
                        blob = str(value).encode("utf-8")
                        fixed += _VARSIZED_SLOT.pack(0, 0, len(blob))
                        payloads += blob
                    elif typ == "CHAR":
                        fixed += str(value).encode("ascii")[:1].ljust(1, b"\x00")
                    elif typ == "BOOLEAN":
                        fixed += struct.pack("<?", bool(value))
                    elif typ.startswith("FLOAT"):
                        fixed += struct.pack("<" + _FIXED_STRUCT[typ], float(value))
                    else:
                        fixed += struct.pack("<" + _FIXED_STRUCT[typ], int(value))
            out += _NES_BUFFER_HEADER.pack(
                len(chunk), num_varsized * len(chunk), _SEQ_INITIAL + ci, _CHUNK_INITIAL
            )
            out += fixed
            out += payloads
        return bytes(out)

    # -- shape-blind sink-catalog hooks -------------------------------------
    def record_count(self, d: NativeData) -> int:
        """Rows in a loaded dataset."""
        return len(d.rows)

    def prefix(self, d: NativeData, n: int) -> NativeData:
        """The first ``n`` rows of ``d`` (outage_loss's kept batch)."""
        return NativeData(d.rows[:n])

    def combine(self, datasets: list[NativeData]) -> NativeData:
        """Concatenate the rows of every WRITE into one expected dataset."""
        return NativeData([r for d in datasets for r in d.rows])

    # -- typed read-back compare --------------------------------------------
    def _canon_field(self, typ: str, value: Any) -> tuple[str, Any]:
        """Exact, type-keyed canonical token for one field (raw bytes for floats)."""
        if value is None:
            return ("null", None)  # NULL — matches a DB NULL read back as None
        if typ in ("VARSIZED", "CHAR"):
            return ("s", str(value))
        if typ == "BOOLEAN":
            return ("i", int(bool(value)))
        if typ == "FLOAT32":
            return ("f", struct.pack("<f", float(value)))
        if typ == "FLOAT64":
            return ("f", struct.pack("<d", float(value)))
        return ("i", int(value))  # any integer type, coercing a DB Decimal/etc.

    def _canon_row(self, row: tuple[Any, ...]) -> tuple[tuple[str, Any], ...]:
        assert self.schema is not None, "NativeModel used before bind(schema)"  # noqa: S101
        return tuple(
            self._canon_field(typ, v) for (_n, typ, _x), v in zip(self.schema, row, strict=False)
        )

    def verify_readback(self, expected: NativeData, received: list[tuple[Any, ...]]) -> None:
        """Multiset-compare the DB read-back (typed rows) against the oracle.

        Coercion + canonicalization happen here (the one place), keyed by the
        schema type, so the connector loader returns raw typed tuples and never
        canonicalizes. On mismatch the diff shows the offending typed rows.
        """
        exp_by_canon = {self._canon_row(r): r for r in expected.rows}
        obs_by_canon = {self._canon_row(tuple(r)): tuple(r) for r in received}
        exp_counts = Counter(self._canon_row(r) for r in expected.rows)
        obs_counts = Counter(self._canon_row(tuple(r)) for r in received)
        if exp_counts == obs_counts:
            return
        only_exp = [exp_by_canon[c] for c in (exp_counts - obs_counts)]
        only_obs = [obs_by_canon[c] for c in (obs_counts - exp_counts)]
        raise AssertionError(
            f"native sink read-back mismatch: {len(received)} rows observed vs "
            f"{len(expected.rows)} expected\n  only in expected: {only_exp[:8]}\n"
            f"  only in observed: {only_obs[:8]}"
        )
