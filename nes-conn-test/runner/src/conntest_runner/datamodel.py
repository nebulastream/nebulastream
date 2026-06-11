# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""datamodel.py — the generator authority + data models.

One :class:`Generator` ``(schema, seed, row_width)`` is the single deterministic
data authority for both source and sink testing: from a requested buffer count it
derives the exact rows that fill them (design §2, §6.1) and yields a
:class:`Dataset` — the flat oracle plus its buffer/seed partitionings. The native
tuple memory layout is **not** mirrored here: the C++ harness encodes/decodes JSONL
through the engine's own formatters, so Python speaks only JSONL and keeps a single
engine-sourced number — ``row_width`` from the harness BIND reply — to size
"N buffers" worth of rows (``rows_per_buffer = buffer_size // row_width``).

The framework builds one schema-bound model per test (the schema is parsed from the
config under test) that turns a Dataset into a FILL/WRITE quota, the harness input, and
the read-back compare — both kinds are connector-agnostic:

  * :class:`SourceModel` — a source. Generates rows, reports both measures of a FILL
    batch (its row count and its JSONL byte length) so the harness can pick the one
    matching how it counts, and multiset-compares the observed JSONL parsed back to rows.
  * :class:`SinkModel` — a sink. Generates rows, feeds them to the harness as JSONL,
    and multiset-compares the typed rows the loader reads back.

Both compare order-insensitively (a SELECT or a broker may reorder). The native vs
opaque distinction is the engine's, owned entirely by the C++ harness (re-encode + FILL
counting) — the runner is fully kind-blind. Loaders deal in typed
rows on both ends and do their own backend IO (a JSON-wire connector serializes via
:func:`rows_to_jsonl` / parses via :func:`jsonl_to_rows`).

The human-readable wire form is JSONL — one schema-keyed JSON object per
newline-terminated line (:func:`rows_to_jsonl` / :func:`jsonl_to_rows`), the single
serializer shared by every path and :meth:`Dataset.dump`.

The schema is parsed from the config under test (:func:`parse_sink_schema` /
:func:`parse_source_schema`), so a new valid config "just works".
"""

from __future__ import annotations

import json
import re
import struct
from collections import Counter
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable

# ── schema model ────────────────────────────────────────────────────────
#
# The native tuple memory layout is no longer mirrored in Python: the C++ harness
# decodes/encodes JSONL through the engine's *own* formatters, so the layout is
# authored exactly once (by the engine). Python only ever speaks JSONL and keeps a
# single engine-sourced layout number — the row width from the harness BIND
# reply — to size "N buffers" worth of rows (`rows_per_buffer = buffer_size // row_width`).

# `(name, NES type, nullable)`, in MemoryLayout (declaration) order.
NativeColumn = tuple[str, str, bool]

_CREATE_SINK_RE = re.compile(
    r"CREATE\s+SINK\s+\S+\s*\((?P<cols>[^)]*)\)\s*TYPE\b", re.IGNORECASE | re.DOTALL
)
# A logical source declares its projected schema with no trailing TYPE keyword
# (the CREATE PHYSICAL SOURCE ... TYPE is a separate statement), so terminate on
# the closing paren of the column list.
_CREATE_SOURCE_RE = re.compile(
    r"CREATE\s+LOGICAL\s+SOURCE\s+\S+\s*\((?P<cols>[^)]*)\)", re.IGNORECASE | re.DOTALL
)


def _parse_columns(cols_text: str, *, kind: str) -> list[NativeColumn]:
    """Parse a ``<name> <TYPE> [NOT NULL], ...`` column list into NativeColumns.

    Returns ``(name, NES type, nullable)`` per column, in declaration order — the
    same order the engine lays out a native tuple. Names are unquoted
    (backticks/quotes stripped); types are upper-cased. Shared by the sink and
    source schema parsers (``kind`` only colours the error message).
    """
    columns: list[NativeColumn] = []
    for raw in cols_text.split(","):
        cell = raw.strip()
        if not cell:
            continue
        tokens = cell.split()
        if len(tokens) < 2:  # noqa: PLR2004  # a column is at least `<name> <TYPE>`
            raise ValueError(
                f"malformed {kind} column {cell!r}: expected `<name> <TYPE> [NOT NULL]`"
            )
        name = tokens[0].strip('`"')
        typ = tokens[1].upper()
        nullable = "NOT NULL" not in " ".join(tokens[2:]).upper()
        columns.append((name, typ, nullable))
    if not columns:
        raise ValueError(f"CREATE {kind.upper()} schema has no columns")
    return columns


def parse_sink_schema(nesql: str) -> list[NativeColumn]:
    """Parse the column list of a ``CREATE SINK <name> (...) TYPE ...`` statement.

    Returns ``(name, NES type, nullable)`` per column, in declaration order — the
    same order the engine lays out a native tuple and the sink INSERTs
    positionally. Raises ``ValueError`` if no CREATE SINK schema is found or a
    column is malformed.
    """
    m = _CREATE_SINK_RE.search(nesql)
    if not m:
        raise ValueError("no `CREATE SINK <name> (...) TYPE ...` schema found in config")
    return _parse_columns(m.group("cols"), kind="sink")


def parse_source_schema(nesql: str) -> list[NativeColumn]:
    """Parse the projected schema of a ``CREATE LOGICAL SOURCE <name> (...)`` statement.

    The source mirror of :func:`parse_sink_schema`: returns ``(name, NES type,
    nullable)`` per column, in projection order — the order the source writes
    into the native tuple and §2's width arithmetic keys off. Raises
    ``ValueError`` if no CREATE LOGICAL SOURCE schema is found or a column is
    malformed.
    """
    m = _CREATE_SOURCE_RE.search(nesql)
    if not m:
        raise ValueError("no `CREATE LOGICAL SOURCE <name> (...)` schema found in config")
    return _parse_columns(m.group("cols"), kind="source")


# Integer types whose declared width is narrower than [0, 1000): the value is
# taken modulo this so it fits the column (e.g. an opaque source's UINT8 framing,
# or a signed byte). Any integer type absent here keeps the full [0, 1000) range.
_INT_MODULUS: dict[str, int] = {"UINT8": 256, "INT8": 128}


def _gen_value(name: str, typ: str, i: int, seed: int, col: int, *, nullable: bool) -> Any:
    """Deterministic value for row ``i``, column ``col`` of ``typ`` under ``seed``.

    Small, type-correct, and round-trip-safe by construction: ints stay in
    [0, 1000) so they fit every integer width and its SQL column; floats stay
    in [0, 1000) (and FLOAT32 is pre-rounded to single precision so the JSONL value
    and the oracle hold the exact value a REAL column returns); VARSIZED
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
    if typ in ("FLOAT32", "FLOAT64"):
        v = (n % 100_000) / 100.0
        # FLOAT32 is round-tripped to single precision so the JSONL value and the
        # oracle hold the exact value a REAL column returns; FLOAT64 is exact.
        return struct.unpack("<f", struct.pack("<f", v))[0] if typ == "FLOAT32" else v
    # Integer types: full [0, 1000), clamped to the declared width for the narrow
    # 8-bit columns (INT8 stays positive, fitting a signed byte).
    return n % _INT_MODULUS.get(typ, 1000)


# ── typed-row canonicalization & multiset compare (shared) ───────────────
#
# Shared by the sink read-back and the source rows-equal compare, so both key off
# one canonical form: keyed by the schema type so a DB Decimal/None/float round-trips
# to the same token as the oracle value it must match.


def _canon_field(typ: str, value: Any) -> tuple[str, Any]:
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


def _canon_row(schema: list[NativeColumn], row: tuple[Any, ...]) -> tuple[tuple[str, Any], ...]:
    # strict=True (matching rows_to_jsonl): every caller feeds schema-width rows
    # (generator oracle, a DB SELECT of the schema projection, jsonl_to_rows), so a
    # row whose arity differs from the schema is a malformed read-back/decode — a
    # real defect that must fail loudly, not be silently compared on the shorter
    # prefix (which could let a wrong-arity read-back pass on a matching prefix).
    return tuple(_canon_field(typ, v) for (_n, typ, _x), v in zip(schema, row, strict=True))


def _verify_rows(
    schema: list[NativeColumn],
    expected_rows: list[tuple[Any, ...]],
    received: list[tuple[Any, ...]],
) -> None:
    """Multiset-compare typed rows against an oracle, keyed by schema type.

    Coercion + canonicalization happen here (the one place); callers pass raw typed
    tuples. The compare is an order-insensitive multiset — a DB SELECT or a broker may
    reorder, and validation does not depend on order. (If a connector ever needs an
    order guarantee, assert it explicitly rather than threading it through this compare.)
    On mismatch the diff names the offending typed rows.
    """
    # Canonicalize each side once and reuse (was re-canonicalised four times).
    exp_canon = [_canon_row(schema, r) for r in expected_rows]
    obs_canon = [_canon_row(schema, tuple(r)) for r in received]
    exp_counts = Counter(exp_canon)
    obs_counts = Counter(obs_canon)
    if exp_counts == obs_counts:
        return
    # Build the canon→row maps only on mismatch, to name the offending rows.
    exp_by_canon = dict(zip(exp_canon, expected_rows, strict=True))
    obs_by_canon = {c: tuple(r) for c, r in zip(obs_canon, received, strict=True)}
    only_exp = [exp_by_canon[c] for c in (exp_counts - obs_counts)]
    only_obs = [obs_by_canon[c] for c in (obs_counts - exp_counts)]
    raise AssertionError(
        f"rows-equal mismatch: {len(received)} rows observed vs "
        f"{len(expected_rows)} expected\n  only in expected: {only_exp[:8]}\n"
        f"  only in observed: {only_obs[:8]}"
    )


# ── JSONL: the one human-readable wire form ──────────────────────────────
#
# One schema-keyed JSON object per `\n`-terminated line (JSON Lines). This is the
# single serializer for every human-readable path: the formatted sink wire, the
# byte-source wire, and `Dataset.dump`. The generator already produces JSON-native
# Python values (int/float/bool/str/None), so no per-type coercion is needed in
# either direction — `_canon_field` does the type-keyed comparison. JSON's native
# typing and escaping make it safe for values holding `,` or `\n` (which the old
# CSV form silently corrupted) and ready for nested datatypes.


def rows_to_jsonl(schema: list[NativeColumn], rows: Iterable[tuple[Any, ...]]) -> bytes:
    """Render ``rows`` to JSONL — one schema-keyed object per newline-terminated line.

    Keys are lowercased: the SQL binder case-folds unquoted identifiers, so the
    engine's JSON formatters emit/expect a canonical lowercased key (the harness
    `Formatter` lowercases to match). Lowercasing both sides keeps the native sink
    (engine decode) and rows source (engine encode) aligned with the runner whatever
    the `.nesql` casing/quoting was; it is a no-op for the all-lowercase configs.
    """
    out = bytearray()
    for row in rows:
        obj = {name.lower(): value for (name, _typ, _x), value in zip(schema, row, strict=True)}
        out += json.dumps(obj, separators=(",", ":")).encode("utf-8")
        out += b"\n"
    return bytes(out)


def jsonl_to_rows(schema: list[NativeColumn], payload: bytes) -> list[tuple[Any, ...]]:
    """Parse a JSONL ``payload`` back into typed rows, pulling fields by schema name.

    Blank lines are skipped. Values come back as JSON natives (int/float/bool/str/
    None); the type-keyed canonicalization in :func:`_verify_rows` reconciles them
    with the oracle (e.g. a FLOAT32 compares by its single-precision bytes).
    """
    rows: list[tuple[Any, ...]] = []
    for line in payload.split(b"\n"):
        if not line.strip():
            continue
        obj = json.loads(line)
        # Keys are the lowercased field names (see rows_to_jsonl): the engine's JSON
        # formatters emit a canonical lowercased key, so pull by it on both paths.
        rows.append(tuple(obj[name.lower()] for name, _typ, _x in schema))
    return rows


# ── the one generator authority ──────────────────────────────────────────
#
# `Generator(schema, seed, row_width)` is the single deterministic data source for
# both kinds (design §6.1). Its primary entry point is buffer-count-driven: a
# scenario asks for N buffers, and the generator derives the exact row count that
# fills them from `rows_per_buffer = buffer_size // row_width`. The row width is the
# engine's own (reported by the harness BIND reply) — Python no longer mirrors
# the native layout. The byte-stream case is the degenerate single-`UINT8` schema,
# so even byte streams flow through `_gen_value` — there is one generator, not three.


class Generator:
    """Buffer-quantized deterministic data authority, parameterized by (schema, seed, row_width)."""

    def __init__(self, schema: list[NativeColumn], *, seed: int = 1, row_width: int) -> None:
        self.schema = schema
        self.seed = seed
        # The engine's native row width (fixed region incl. null flags), reported by
        # the harness BIND reply. Used only to size "N buffers" worth of rows; the
        # native byte layout itself is owned by the engine, never re-derived here.
        self._row_width = row_width

    @property
    def row_width(self) -> int:
        """The engine's native row width (fixed region incl. null flags)."""
        return self._row_width

    def rows_per_buffer(self, buffer_size: int) -> int:
        """Whole rows whose fixed region fits one ``buffer_size``-bounded buffer."""
        return max(1, buffer_size // max(1, self._row_width))

    def _gen_rows(self, count: int) -> list[tuple[Any, ...]]:
        return [
            tuple(
                _gen_value(name, typ, i, self.seed, col, nullable=nullable)
                for col, (name, typ, nullable) in enumerate(self.schema)
            )
            for i in range(count)
        ]

    def dataset_for_buffers(self, n_buffers: int, buffer_size: int) -> Dataset:
        """Derive the exact row count that fills N buffers, then generate it (§2)."""
        rpb = self.rows_per_buffer(buffer_size)
        return Dataset(
            schema=self.schema,
            seed=self.seed,
            buffer_size=buffer_size,
            n_buffers=n_buffers,
            rows_per_buffer=rpb,
            _rows=self._gen_rows(n_buffers * rpb),
            _gen=self,
        )


@dataclass(frozen=True)
class Dataset:
    """A generated dataset: the flat oracle plus its buffer/seed partitionings."""

    schema: list[NativeColumn]
    seed: int
    buffer_size: int
    n_buffers: int
    rows_per_buffer: int
    _rows: list[tuple[Any, ...]]
    _gen: Generator

    def rows(self) -> list[tuple[Any, ...]]:
        """The flat oracle, exactly filling ``n_buffers``."""
        return list(self._rows)

    def buffers(self) -> list[list[tuple[Any, ...]]]:
        """``n_buffers`` groups, each a full buffer's worth of rows (sink alignment)."""
        rpb = self.rows_per_buffer
        return [self._rows[i : i + rpb] for i in range(0, len(self._rows), rpb)] or [[]]

    def seed_batches(self, k: int) -> list[list[tuple[Any, ...]]]:
        """Split the flat rows into ``k`` contiguous seed batches (the outage A/B split).

        Distinct from ``buffers()``: this is *when* data is written relative to a
        link cut, not how a source/sink chunks its output. ``seed_batches(2)``
        reproduces the old ``head()``/``tail()`` split.
        """
        if k <= 1:
            return [list(self._rows)]
        n = len(self._rows)
        size = (n + k - 1) // k
        return [self._rows[i : i + size] for i in range(0, n, size)] or [[]]

    def row_count(self) -> int:
        """Flat oracle row count."""
        return len(self._rows)

    def dump(self) -> str:
        """Human-readable rows for a reviewer/debugger (design §7) — JSONL.

        A ``#`` metadata header line followed by the same JSONL the connectors
        carry, so the debug dump and the wire form are one representation.
        """
        head = (
            f"# seed={self.seed} N={self.n_buffers} rows_per_buffer={self.rows_per_buffer} "
            f"rows={len(self._rows)} row_width={self._gen.row_width}\n"
        )
        return head + rows_to_jsonl(self.schema, self._rows).decode("utf-8")


class SourceModel:
    """Generator-driven source model — schema-bound, connector-agnostic (§3).

    Generates rows, reports the two measures of a FILL batch (its row count and its JSONL
    byte length), and multiset-compares the observed JSONL parsed back to typed rows
    (order-insensitive — a DB SELECT or a broker may reorder). The observed wire form is
    always JSONL: a NATIVE source's engine tuples re-encoded through the engine's own JSON
    output formatter, or an opaque source's JSONL carried through unchanged.

    The model is fully **kind-blind** — it never knows whether the connector is native or
    opaque. FILL sends both measures and the harness compares against the one matching how
    it counts (tuples for a NATIVE source, bytes for an opaque one), so the
    native-vs-opaque distinction lives only in the harness. The loader seeds its backend
    from the typed rows directly (INSERT, or serialize-and-publish); this model owns
    generation sizing and the compare.
    """

    def __init__(self, schema: list[NativeColumn]) -> None:
        self.schema = schema

    def make_dataset(self, n_buffers: int, seed: int, buffer_size: int, row_width: int) -> Dataset:
        """Build the exact dataset that fills ``n_buffers`` — the seed and the oracle.

        ``row_width`` is the engine's native row width (from the harness BIND
        reply); it sizes the row count, never the wire encoding.
        """
        return Generator(self.schema, seed=seed, row_width=row_width).dataset_for_buffers(
            n_buffers, buffer_size
        )

    def fill_counts(self, rows: list[tuple[Any, ...]]) -> tuple[int, int]:
        """The two measures of ``rows`` the harness picks its FILL target from.

        Returns ``(row_count, jsonl_byte_length)`` — both computed the same way for every
        connector. The harness compares its observed count against whichever matches its
        native unit (the tuple count for a NATIVE source, the JSONL byte length for an
        opaque one), so the runner never needs to know the connector kind.
        """
        return len(rows), len(rows_to_jsonl(self.schema, rows))

    def compare_fill(self, rows: list[tuple[Any, ...]], *, observed: bytes) -> None:
        """Verify a source FILL against the generated ``rows`` (the oracle).

        The observed wire form is always JSONL — a NATIVE source's tuples re-encoded
        through the engine's JSON output formatter, or an opaque source's JSONL carried
        through. Parse it and multiset-compare typed rows (a SELECT or a broker may reorder).
        """
        _verify_rows(self.schema, rows, jsonl_to_rows(self.schema, observed))


class SinkModel:
    """Schema-driven sink model — connector-agnostic (§3).

    Generates typed rows from the CREATE SINK schema, feeds them to the harness as
    JSONL, and multiset-compares the typed rows the loader reads back (order-insensitive
    — a broker or a SELECT may reorder). The generated rows are both the harness input
    and the read-back oracle, so the oracle cannot drift from what the sink was fed.

    How the harness consumes the JSONL is the engine's concern, decided from the bound
    descriptor in the C++ harness: a NATIVE sink decodes it into native row-layout
    buffers through the engine's own JSON input formatter; an opaque sink slices it into
    record batches and carries the bytes through. The runner is JSONL-only either way and
    never branches on it — the loader returns typed rows (a NATIVE loader SELECTs them; an
    opaque loader parses the records it collected via :func:`jsonl_to_rows`).
    """

    def __init__(self, schema: list[NativeColumn]) -> None:
        self.schema = schema

    def make_dataset(self, n_buffers: int, seed: int, buffer_size: int, row_width: int) -> Dataset:
        """Build the dataset the sink will be fed (and the read-back oracle).

        ``row_width`` is the engine's native row width (from the harness BIND
        reply); it sizes the row count so that, fed back as JSONL with a matching
        ``buffer_size``, a native sink decodes into exactly ``n_buffers`` buffers.
        """
        return Generator(self.schema, seed=seed, row_width=row_width).dataset_for_buffers(
            n_buffers, buffer_size
        )

    def harness_input(self, dataset: Dataset) -> bytes:
        """Bytes the harness reads via ``--input-path`` — always JSONL.

        A native sink decodes this JSONL into native row-layout buffers through the
        engine's own JSON input formatter; an opaque sink slices it into record
        batches and carries the bytes through. The runner is JSONL-only either way.
        """
        return rows_to_jsonl(self.schema, dataset.rows())

    def input_record_count(self, dataset: Dataset) -> int:
        """Tuples/records the harness will materialize (for buffer-count sizing)."""
        return dataset.row_count()

    def verify_readback(self, expected_rows: list[tuple[Any, ...]], received: list[Any]) -> None:
        """Multiset-compare the loader's typed-row read-back against the generated oracle.

        ``received`` is typed rows: a NATIVE loader SELECTs them; an opaque loader parses
        the records it collected (via :func:`jsonl_to_rows`) before returning. Compares
        via the shared :func:`_verify_rows` (order-insensitive — a broker may reorder).
        """
        _verify_rows(self.schema, expected_rows, received)
