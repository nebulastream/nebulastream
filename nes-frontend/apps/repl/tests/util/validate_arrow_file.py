#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["pyarrow>=15.0"]
# ///
"""
Validates an Arrow IPC stream file produced by the ArrowFileSink.

Usage:
    ./validate_arrow_file.py <arrow_file>

Reads the file with PyArrow, validates schema, row counts, data correctness
for non-nullable columns, nullable columns, and string columns.
Prints a JSON summary and exits 0 on success, 1 on failure.

Generator sequences (applied after filter (id/1000)%2==0):
  vi32 == id, vi8 == id % 101, vu16 == id % 60001,
  vf32 ≈ id, vf64 ≈ id * 0.5
  Nullable columns mirror the non-nullable sequences.
  rstr: random base64 strings, length 10-10000 — validated for presence, type,
        length bounds, and character set. Large strings exercise multi-child-buffer overflow.
"""

import json
import sys

import pyarrow as pa
import pyarrow.ipc as ipc


def validate(path: str) -> dict:
    with pa.OSFile(path, "rb") as f:
        reader = ipc.open_stream(f)
        schema = reader.schema
        batches = []
        while True:
            try:
                batch = reader.read_next_batch()
                batches.append(batch)
            except StopIteration:
                break

    table = pa.concat_tables([pa.Table.from_batches([b]) for b in batches])

    results = {
        "file": path,
        "num_batches": len(batches),
        "total_rows": len(table),
        "num_columns": len(schema),
        "schema": [
            {"name": f.name, "type": str(f.type), "nullable": f.nullable}
            for f in schema
        ],
    }

    if len(table) == 0:
        results["error"] = "no rows"
        print(json.dumps(results, indent=2))
        return results

    ids = table.column("SENSOR$ID").to_pylist()

    # 1. ID column: no duplicates, all satisfy filter (id/1000)%2==0
    unique_ids = set(ids)
    bad_filter = [i for i in ids if (i // 1000) % 2 != 0]
    results["id_count"] = len(ids)
    results["id_unique"] = len(unique_ids)
    results["id_no_duplicates"] = len(unique_ids) == len(ids)
    results["id_filter_valid"] = len(bad_filter) == 0
    if bad_filter:
        results["id_bad_filter_sample"] = bad_filter[:5]

    # 2. Non-nullable fixed-width columns
    non_nullable_checks = {}

    vi32 = table.column("SENSOR$VI32").to_pylist()
    if vi32:
        m = sum(
            1
            for a, b in zip(ids, vi32)
            if int(a) % (2**31) != int(b) % (2**31)
        )
        non_nullable_checks["vi32_mismatches"] = m

    vi8 = table.column("SENSOR$VI8").to_pylist()
    if vi8:
        m = sum(
            1
            for a, b in zip(ids, vi8)
            if int(a) % 101 != ((int(b) % 101) + 101) % 101
        )
        non_nullable_checks["vi8_mismatches"] = m

    vu16 = table.column("SENSOR$VU16").to_pylist()
    if vu16:
        m = sum(1 for a, b in zip(ids, vu16) if int(a) % 60001 != int(b))
        non_nullable_checks["vu16_mismatches"] = m

    vf32 = table.column("SENSOR$VF32").to_pylist()
    if vf32:
        m = sum(
            1 for a, b in zip(ids, vf32) if abs(float(a) - float(b)) > 1.0
        )
        non_nullable_checks["vf32_mismatches"] = m

    vf64 = table.column("SENSOR$VF64").to_pylist()
    if vf64:
        m = sum(
            1
            for a, b in zip(ids, vf64)
            if abs(float(a) * 0.5 - float(b)) > 0.01
        )
        non_nullable_checks["vf64_mismatches"] = m

    results["non_nullable"] = non_nullable_checks
    results["non_nullable_valid"] = all(
        v == 0 for v in non_nullable_checks.values()
    )

    # 3. Nullable columns
    nullable_checks = {}

    ni64 = table.column("SENSOR$NI64").to_pylist()
    if ni64:
        m = sum(
            1
            for a, b in zip(ids, ni64)
            if b is not None and int(a) != int(b)
        )
        nullable_checks["ni64_mismatches"] = m

    ni32 = table.column("SENSOR$NI32").to_pylist()
    if ni32:
        m = sum(
            1
            for a, b in zip(ids, ni32)
            if b is not None and int(a) % (2**31) != int(b) % (2**31)
        )
        nullable_checks["ni32_mismatches"] = m

    nf32 = table.column("SENSOR$NF32").to_pylist()
    if nf32:
        m = sum(
            1
            for a, b in zip(ids, nf32)
            if b is not None and abs(float(a) - float(b)) > 1.0
        )
        nullable_checks["nf32_mismatches"] = m

    nf64 = table.column("SENSOR$NF64").to_pylist()
    if nf64:
        m = sum(
            1
            for a, b in zip(ids, nf64)
            if b is not None and abs(float(a) * 0.5 - float(b)) > 0.01
        )
        nullable_checks["nf64_mismatches"] = m

    nullable_columns_present = sum(
        1 for name in table.column_names if name.startswith("SENSOR$N")
    )
    results["nullable"] = nullable_checks
    results["nullable_valid"] = all(v == 0 for v in nullable_checks.values())
    results["nullable_columns_present"] = nullable_columns_present

    # 4. String column (RANDOMSTR): validate presence, type, and length bounds (optional)
    string_checks = {}
    base64_chars = set(
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
    )

    if "SENSOR$RSTR" not in table.column_names:
        string_checks["column_present"] = False
        string_checks["valid"] = True  # String column is optional
        results["string"] = string_checks
        results["all_valid"] = (
            results["id_no_duplicates"]
            and results["id_filter_valid"]
            and results["non_nullable_valid"]
            and results["nullable_valid"]
        )
        print(json.dumps(results, indent=2))
        return results

    rstr_col = table.column("SENSOR$RSTR")
    string_checks["column_present"] = rstr_col is not None
    string_checks["type_is_string"] = pa.types.is_string(rstr_col.type) or pa.types.is_large_string(rstr_col.type)

    rstr_values = rstr_col.to_pylist()
    null_count = sum(1 for v in rstr_values if v is None)
    string_checks["null_count"] = null_count

    lengths = [len(v) for v in rstr_values if v is not None]
    if lengths:
        string_checks["min_length"] = min(lengths)
        string_checks["max_length"] = max(lengths)
        string_checks["length_valid"] = min(lengths) >= 10 and max(lengths) <= 10000

        # Verify all characters are from the base64 alphabet
        bad_chars = sum(
            1
            for v in rstr_values
            if v is not None and not all(c in base64_chars for c in v)
        )
        string_checks["bad_char_rows"] = bad_chars
        string_checks["chars_valid"] = bad_chars == 0
    else:
        string_checks["length_valid"] = False
        string_checks["chars_valid"] = False

    string_checks["valid"] = (
        string_checks["column_present"]
        and string_checks["type_is_string"]
        and string_checks.get("length_valid", False)
        and string_checks.get("chars_valid", False)
    )

    results["string"] = string_checks

    # Overall
    results["all_valid"] = (
        results["id_no_duplicates"]
        and results["id_filter_valid"]
        and results["non_nullable_valid"]
        and results["nullable_valid"]
        and string_checks["valid"]
    )

    print(json.dumps(results, indent=2))
    return results


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <arrow_file>", file=sys.stderr)
        sys.exit(1)
    result = validate(sys.argv[1])
    sys.exit(0 if result.get("all_valid", False) else 1)
