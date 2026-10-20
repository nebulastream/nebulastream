# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Test data generated from the schema of the config under test, and the JSONL codec and compare that check it.

The amount of data is measured in its JSONL encoding, so Python does not mirror the engine's native tuple layout.
"""

from __future__ import annotations

import json
import random
import re
import struct
from collections import Counter
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable

# `(name, NES type, nullable)`, in declaration order.
NativeColumn = tuple[str, str, bool]

# The TYPE of a source is in a separate CREATE PHYSICAL SOURCE statement,
# so the logical source's column list ends at its closing paren.
_CREATE_SOURCE_RE = re.compile(
    r"CREATE\s+LOGICAL\s+SOURCE\s+\S+\s*\((?P<cols>[^)]*)\)", re.IGNORECASE | re.DOTALL
)


def _parse_columns(cols_text: str) -> list[NativeColumn]:
    columns: list[NativeColumn] = []
    for raw in cols_text.split(","):
        cell = raw.strip()
        if not cell:
            continue
        tokens = cell.split()
        if len(tokens) < 2:  # noqa: PLR2004  # a column is at least `<name> <TYPE>`
            raise ValueError(
                f"malformed column {cell!r}: expected `<name> <TYPE> [NOT NULL]`"
            )
        name = tokens[0].strip('`"')
        typ = tokens[1].upper()
        nullable = "NOT NULL" not in " ".join(tokens[2:]).upper()
        columns.append((name, typ, nullable))
    if not columns:
        raise ValueError("CREATE LOGICAL SOURCE schema has no columns")
    return columns


def parse_source_schema(nesql: str) -> list[NativeColumn]:
    m = _CREATE_SOURCE_RE.search(nesql)
    if not m:
        raise ValueError("no `CREATE LOGICAL SOURCE <name> (...)` schema found in config")
    return _parse_columns(m.group("cols"))


# Integer types too narrow for [0, 1000).
# INT8 stays positive.
_INT_MODULUS: dict[str, int] = {"UINT8": 256, "INT8": 128}


# About one row in five, so both the NULL path and the value path are exercised.
_NULL_SHARE = 0.2


def _gen_value(name: str, typ: str, i: int, rng: random.Random, *, nullable: bool) -> Any:
    """Values stay small, so they fit the declared width and the backend's SQL column."""
    if nullable and rng.random() < _NULL_SHARE:
        return None
    if typ == "VARSIZED":
        return f"{name[:12]}-{i}"
    if typ == "CHAR":
        return chr(ord("A") + rng.randrange(26))
    if typ == "BOOLEAN":
        return rng.choice((True, False))
    if typ in ("FLOAT32", "FLOAT64"):
        v = rng.randrange(100_000) / 100.0
        # FLOAT32 is rounded to single precision so the oracle holds the exact value that a REAL column returns.
        return struct.unpack("<f", struct.pack("<f", v))[0] if typ == "FLOAT32" else v
    return rng.randrange(_INT_MODULUS.get(typ, 1000))


def _canon_field(typ: str, value: Any) -> tuple[str, Any]:
    """Floats compare by their raw bytes, so a decoded value matches only if it is bit-identical to the oracle."""
    if value is None:
        return ("null", None)
    if typ in ("VARSIZED", "CHAR"):
        return ("s", str(value))
    if typ == "BOOLEAN":
        return ("i", int(bool(value)))
    if typ == "FLOAT32":
        return ("f", struct.pack("<f", float(value)))
    if typ == "FLOAT64":
        return ("f", struct.pack("<d", float(value)))
    return ("i", int(value))  # also coerces a DB Decimal


def _canon_row(schema: list[NativeColumn], row: tuple[Any, ...]) -> tuple[tuple[str, Any], ...]:
    # A row with the wrong arity is a malformed read-back.
    # It must fail, not pass on a matching prefix.
    return tuple(_canon_field(typ, v) for (_n, typ, _x), v in zip(schema, row, strict=True))


def _verify_rows(
    schema: list[NativeColumn],
    expected_rows: list[tuple[Any, ...]],
    received: list[tuple[Any, ...]],
) -> None:
    """Compare typed rows against the oracle as a multiset, because a DB SELECT or a broker may reorder them.

    Values compare in a canonical form, so `1` decoded from JSON matches `Decimal('1')` from a DB.
    On a mismatch, the error shows up to eight rows that are only expected and up to eight that are only observed.
    """
    exp_canon = [_canon_row(schema, r) for r in expected_rows]
    obs_canon = [_canon_row(schema, tuple(r)) for r in received]
    exp_counts = Counter(exp_canon)
    obs_counts = Counter(obs_canon)
    if exp_counts == obs_counts:
        return
    exp_by_canon = dict(zip(exp_canon, expected_rows, strict=True))
    obs_by_canon = {c: tuple(r) for c, r in zip(obs_canon, received, strict=True)}
    only_exp = [exp_by_canon[c] for c in (exp_counts - obs_counts)]
    only_obs = [obs_by_canon[c] for c in (obs_counts - exp_counts)]
    raise AssertionError(
        f"rows-equal mismatch: {len(received)} rows observed vs "
        f"{len(expected_rows)} expected\n  only in expected: {only_exp[:8]}\n"
        f"  only in observed: {only_obs[:8]}"
    )


def rows_to_jsonl(schema: list[NativeColumn], rows: Iterable[tuple[Any, ...]]) -> bytes:
    """Keys are lowercased, so the wire form does not depend on how the `.nesql` cased or quoted the field names."""
    out = bytearray()
    for row in rows:
        obj = {name.lower(): value for (name, _typ, _x), value in zip(schema, row, strict=True)}
        out += json.dumps(obj, separators=(",", ":")).encode("utf-8")
        out += b"\n"
    return bytes(out)


def jsonl_to_rows(schema: list[NativeColumn], payload: bytes) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for line in payload.split(b"\n"):
        if not line.strip():
            continue
        obj = json.loads(line)
        rows.append(tuple(obj[name.lower()] for name, _typ, _x in schema))
    return rows


def generate_rows(schema: list[NativeColumn], *, seed: int, max_bytes: int) -> list[tuple[Any, ...]]:
    """The rows whose JSONL encoding fits in ``max_bytes``, and at least one.

    One generator yields the rows in order, so the same seed always gives the same rows.
    """
    rng = random.Random(seed)  # noqa: S311  # test data, not cryptography
    rows: list[tuple[Any, ...]] = []
    size = 0
    while True:
        row = tuple(_gen_value(name, typ, len(rows), rng, nullable=nullable) for name, typ, nullable in schema)
        size += len(rows_to_jsonl(schema, [row]))
        if size > max_bytes and rows:
            return rows
        rows.append(row)


class SourceModel:
    """Generates and checks the data of one source test.

    The loader only does the backend IO.
    """

    def __init__(self, schema: list[NativeColumn]) -> None:
        self.schema = schema

    def make_rows(self, n_buffers: int, seed: int, buffer_size: int) -> list[tuple[Any, ...]]:
        """As many rows as fill ``n_buffers`` in the JSONL encoding that a byte source such as TCP receives."""
        return generate_rows(self.schema, seed=seed, max_bytes=n_buffers * buffer_size)

    def fill_counts(self, rows: list[tuple[Any, ...]]) -> tuple[int, int]:
        return len(rows), len(rows_to_jsonl(self.schema, rows))

    def compare_fill(self, rows: list[tuple[Any, ...]], *, observed: bytes) -> None:
        _verify_rows(self.schema, rows, jsonl_to_rows(self.schema, observed))
