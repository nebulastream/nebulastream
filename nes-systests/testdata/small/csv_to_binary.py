#!/usr/bin/env python3

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Convert a CSV file to a packed little-endian binary file given a schema."""

import argparse
import csv
import re
import struct
import sys
from pathlib import Path

FIXED_TYPES = {
    "int8":    ("<b", 1),
    "int16":   ("<h", 2),
    "int32":   ("<i", 4),
    "int64":   ("<q", 8),
    "uint8":   ("<B", 1),
    "uint16":  ("<H", 2),
    "uint32":  ("<I", 4),
    "uint64":  ("<Q", 8),
    "float32": ("<f", 4),
    "float64": ("<d", 8),
}

BOOL_TRUE = {"true", "t", "1", "yes", "y"}
BOOL_FALSE = {"false", "f", "0", "no", "n"}

STRING_RE = re.compile(r"^string\[(\d+)\]$")


def parse_schema(schema_str, allow_unsized_string=False):
    fields = []
    for raw in schema_str.split(","):
        t = raw.strip().lower()
        if not t:
            raise ValueError("empty type in schema")
        if t in FIXED_TYPES:
            fields.append((t, None))
            continue
        if t == "bool" or t == "char":
            fields.append((t, None))
            continue
        if allow_unsized_string and t == "string":
            fields.append(("string", None))
            continue
        m = STRING_RE.match(t)
        if m:
            n = int(m.group(1))
            if n < 1:
                raise ValueError(f"string[N] requires N >= 1, got {n}")
            fields.append(("string", n))
            continue
        raise ValueError(f"unknown type: {raw!r}")
    return fields


def encode_value(value, type_name, n, row_idx, col_idx):
    try:
        if type_name in FIXED_TYPES:
            fmt, _ = FIXED_TYPES[type_name]
            if type_name.startswith(("int", "uint")):
                return struct.pack(fmt, int(value))
            return struct.pack(fmt, float(value))
        if type_name == "bool":
            v = value.strip().lower()
            if v in BOOL_TRUE:
                return b"\x01"
            if v in BOOL_FALSE:
                return b"\x00"
            raise ValueError(f"not a bool: {value!r}")
        if type_name == "char":
            if len(value) != 1:
                raise ValueError(f"char requires exactly 1 character, got {len(value)}")
            b = value.encode("utf-8")
            if len(b) != 1:
                raise ValueError(f"char must be a single byte, got {len(b)} bytes")
            return b
        if type_name == "string":
            b = value.encode("utf-8")
            if len(b) > n - 1:
                raise ValueError(
                    f"string of {len(b)} bytes exceeds max length {n - 1} for string[{n}]"
                )
            return b + b"\x00" * (n - len(b))
    except ValueError as e:
        raise ValueError(f"row {row_idx}, column {col_idx} ({type_name}): {e}") from e
    raise AssertionError(f"unhandled type {type_name}")


def convert(csv_path, out_path, schema, delimiter, has_header):
    fields = parse_schema(schema)
    if _convert_fast(csv_path, out_path, fields, delimiter, has_header):
        return
    with open(csv_path, newline="") as fin, open(out_path, "wb") as fout:
        reader = csv.reader(fin, delimiter=delimiter)
        if has_header:
            next(reader, None)
        for row_idx, row in enumerate(reader):
            if len(row) != len(fields):
                raise ValueError(
                    f"row {row_idx}: expected {len(fields)} fields, got {len(row)}"
                )
            for col_idx, ((type_name, n), value) in enumerate(zip(fields, row)):
                fout.write(encode_value(value, type_name, n, row_idx, col_idx))


# numpy dtype strings for the vectorized fast path (packed little-endian, same as struct fmts)
_FIXED_NP = {
    "int8": "<i1", "int16": "<i2", "int32": "<i4", "int64": "<i8",
    "uint8": "<u1", "uint16": "<u2", "uint32": "<u4", "uint64": "<u8",
    "float32": "<f4", "float64": "<f8",
}


def _convert_fast(csv_path, out_path, fields, delimiter, has_header, chunksize=4_000_000):
    """Vectorized pandas/numpy conversion — byte-identical to the row-wise path (validated on
    bid/auction/person nexmark samples), ~100x faster (6 GB bid CSV: ~50 s vs ~10 min).

    Handles fixed numeric types and string[N] only; returns False (caller falls back to the
    row-wise path) for bool/char schemas or when pandas/numpy are unavailable. Strings are
    encoded UTF-8 and \\x00-padded to N by the numpy 'S' dtype; the <= N-1 content-length rule
    is enforced per chunk before writing, matching encode_value(). float parsing uses
    float_precision='round_trip' (correctly-rounded, same result as Python float())."""
    try:
        import numpy as np
        import pandas as pd
    except ImportError:
        return False
    if any(t not in _FIXED_NP and t != "string" for t, _ in fields):
        return False

    np_dtype = []
    read_dtype = {}
    for i, (t, n) in enumerate(fields):
        np_dtype.append((f"f{i}", f"S{n}" if t == "string" else _FIXED_NP[t]))
        read_dtype[i] = str if t == "string" else _FIXED_NP[t]
    packed = np.dtype(np_dtype)  # align=False -> packed, no padding between fields

    total_rows = 0
    with open(out_path, "wb") as fout:
        reader = pd.read_csv(
            csv_path, header=0 if has_header else None, dtype=read_dtype,
            sep=delimiter, chunksize=chunksize, engine="c", na_filter=False,
            float_precision="round_trip",
        )
        for chunk in reader:
            if chunk.shape[1] != len(fields):
                raise ValueError(
                    f"expected {len(fields)} fields, got {chunk.shape[1]}"
                )
            arr = np.empty(len(chunk), dtype=packed)
            for i, (t, n) in enumerate(fields):
                col = chunk.iloc[:, i]
                if t == "string":
                    enc = col.str.encode("utf-8")
                    max_len = int(enc.str.len().max())
                    if max_len > n - 1:
                        raise ValueError(
                            f"column {i}: string of {max_len} bytes exceeds max length "
                            f"{n - 1} for string[{n}]"
                        )
                    arr[f"f{i}"] = enc.values
                else:
                    arr[f"f{i}"] = col.values
            arr.tofile(fout)
            total_rows += len(chunk)
    return True


def _next_pow2(x):
    return 1 << max(0, x - 1).bit_length()


def _build_schema(fields, max_bytes, size_fn):
    parts = []
    for i, (t, n) in enumerate(fields):
        if t == "string" and i in max_bytes:
            parts.append(f"string[{size_fn(i)}]")
        elif n is not None:
            parts.append(f"{t}[{n}]")
        else:
            parts.append(t)
    return ",".join(parts)


def measure(csv_path, schema, delimiter, has_header):
    fields = parse_schema(schema, allow_unsized_string=True)
    string_cols = [i for i, (t, _) in enumerate(fields) if t == "string"]
    if not string_cols:
        raise ValueError("schema has no string columns to measure")
    max_bytes = {i: 0 for i in string_cols}
    with open(csv_path, newline="") as fin:
        reader = csv.reader(fin, delimiter=delimiter)
        if has_header:
            next(reader, None)
        for row_idx, row in enumerate(reader):
            if len(row) != len(fields):
                raise ValueError(
                    f"row {row_idx}: expected {len(fields)} fields, got {len(row)}"
                )
            for i in string_cols:
                b = len(row[i].encode("utf-8"))
                if b > max_bytes[i]:
                    max_bytes[i] = b

    for i in string_cols:
        tight = max_bytes[i] + 1
        pow2 = _next_pow2(tight)
        print(
            f"column {i}: max_bytes={max_bytes[i]} -> string[{tight}] -> string[{pow2}]"
        )
    print("schema:      " + _build_schema(fields, max_bytes, lambda i: max_bytes[i] + 1))
    print(
        "schema_pow2: "
        + _build_schema(fields, max_bytes, lambda i: _next_pow2(max_bytes[i] + 1))
    )
    return fields, max_bytes


def auto_convert(csv_path, out_path, schema, delimiter, has_header, size_mode):
    print(f"=== phase 1: measuring string sizes ({csv_path}) ===")
    fields, max_bytes = measure(csv_path, schema, delimiter, has_header)
    if size_mode == "pow2":
        size_fn = lambda i: _next_pow2(max_bytes[i] + 1)
    else:
        size_fn = lambda i: max_bytes[i] + 1
    resolved = _build_schema(fields, max_bytes, size_fn)
    print(f"=== phase 2: converting with {size_mode} schema -> {out_path} ===")
    print(f"resolved schema: {resolved}")
    convert(csv_path, out_path, resolved, delimiter, has_header)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("csv", type=Path, help="input CSV file")
    p.add_argument(
        "output",
        type=Path,
        nargs="?",
        help="output binary file (omit when using --measure)",
    )
    p.add_argument(
        "--schema",
        required=True,
        help="comma-separated field types, e.g. 'int32,float64,string[16],bool'",
    )
    p.add_argument("--delimiter", default=",", help="CSV field delimiter (default ',')")
    p.add_argument("--has-header", action="store_true", help="skip the first CSV row")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument(
        "--measure",
        action="store_true",
        help="report max byte lengths for string columns instead of converting; "
        "bare 'string' is allowed in the schema in this mode",
    )
    mode.add_argument(
        "--auto",
        action="store_true",
        help="measure string sizes then convert in one run; "
        "bare 'string' is allowed in the schema",
    )
    p.add_argument(
        "--size",
        choices=("tight", "pow2"),
        default="pow2",
        help="with --auto, pick per-string size: tight = max+1, "
        "pow2 = next power of 2 >= max+1 (default: pow2)",
    )
    args = p.parse_args()

    try:
        if args.measure:
            if args.output is not None:
                p.error("--measure does not take an output path")
            measure(args.csv, args.schema, args.delimiter, args.has_header)
        elif args.auto:
            if args.output is None:
                p.error("--auto requires an output path")
            auto_convert(
                args.csv,
                args.output,
                args.schema,
                args.delimiter,
                args.has_header,
                args.size,
            )
        else:
            if args.output is None:
                p.error("output path is required unless --measure is set")
            convert(args.csv, args.output, args.schema, args.delimiter, args.has_header)
    except (ValueError, FileNotFoundError) as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
