#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["pyarrow>=15.0"]
# ///
"""
Validate that an Arrow IPC file produced by NES matches the expected values
from generate_arrow_testdata.py after a roundtrip through ArrowFileSource.

Usage:
    ./validate_arrow_roundtrip.py <arrow_output_file>

Reads the file with PyArrow, checks every column against the expected
deterministic values.  Prints JSON results and exits 0 on success, 1 on failure.
"""

import json
import math
import sys

import pyarrow as pa
import pyarrow.ipc as ipc

NUM_ROWS = 1000


def validate(path: str) -> dict:
    with pa.OSFile(path, "rb") as f:
        reader = ipc.open_stream(f)
        table = reader.read_all()

    results = {
        "file": path,
        "total_rows": len(table),
        "num_columns": table.num_columns,
        "schema": [
            {"name": f.name, "type": str(f.type), "nullable": f.nullable}
            for f in table.schema
        ],
        "checks": {},
    }

    if len(table) != NUM_ROWS:
        results["checks"]["row_count"] = {
            "expected": NUM_ROWS,
            "actual": len(table),
            "pass": False,
        }
        results["all_valid"] = False
        print(json.dumps(results, indent=2))
        return results

    results["checks"]["row_count"] = {"expected": NUM_ROWS, "actual": NUM_ROWS, "pass": True}

    # NES prefixes column names with SOURCE_NAME$ — detect the prefix from the first column
    first_col = table.schema.field(0).name
    prefix = first_col[:first_col.rfind("$") + 1] if "$" in first_col else ""

    def col(name):
        """Resolve column name with optional NES prefix."""
        return prefix + name

    def check_int_col(col_name, expected_fn):
        vals = table.column(col(col_name)).to_pylist()
        mismatches = sum(1 for i, v in enumerate(vals) if v != expected_fn(i))
        results["checks"][col_name] = {"mismatches": mismatches, "pass": mismatches == 0}
        if mismatches > 0:
            # Show first mismatch for debugging
            for i, v in enumerate(vals):
                if v != expected_fn(i):
                    results["checks"][col_name]["first_mismatch"] = {
                        "row": i, "expected": expected_fn(i), "actual": v
                    }
                    break

    def check_float_col(col_name, expected_fn, tolerance=0.01):
        vals = table.column(col(col_name)).to_pylist()
        mismatches = sum(
            1 for i, v in enumerate(vals) if abs(float(v) - expected_fn(i)) > tolerance
        )
        results["checks"][col_name] = {"mismatches": mismatches, "pass": mismatches == 0}
        if mismatches > 0:
            for i, v in enumerate(vals):
                if abs(float(v) - expected_fn(i)) > tolerance:
                    results["checks"][col_name]["first_mismatch"] = {
                        "row": i, "expected": expected_fn(i), "actual": float(v)
                    }
                    break

    def check_bool_col(col_name, expected_fn):
        vals = table.column(col(col_name)).to_pylist()
        mismatches = sum(1 for i, v in enumerate(vals) if v != expected_fn(i))
        results["checks"][col_name] = {"mismatches": mismatches, "pass": mismatches == 0}
        if mismatches > 0:
            for i, v in enumerate(vals):
                if v != expected_fn(i):
                    results["checks"][col_name]["first_mismatch"] = {
                        "row": i, "expected": expected_fn(i), "actual": v
                    }
                    break

    def check_nullable_int_col(col_name, expected_fn, null_fn):
        vals = table.column(col(col_name)).to_pylist()
        mismatches = 0
        for i, v in enumerate(vals):
            if null_fn(i):
                if v is not None:
                    mismatches += 1
            else:
                if v is None or v != expected_fn(i):
                    mismatches += 1
        results["checks"][col_name] = {"mismatches": mismatches, "pass": mismatches == 0}
        null_count = sum(1 for v in vals if v is None)
        expected_nulls = sum(1 for i in range(NUM_ROWS) if null_fn(i))
        results["checks"][col_name]["null_count"] = null_count
        results["checks"][col_name]["expected_nulls"] = expected_nulls

    def check_nullable_float_col(col_name, expected_fn, null_fn, tolerance=0.01):
        vals = table.column(col(col_name)).to_pylist()
        mismatches = 0
        for i, v in enumerate(vals):
            if null_fn(i):
                if v is not None:
                    mismatches += 1
            else:
                if v is None or abs(float(v) - expected_fn(i)) > tolerance:
                    mismatches += 1
        results["checks"][col_name] = {"mismatches": mismatches, "pass": mismatches == 0}
        null_count = sum(1 for v in vals if v is None)
        expected_nulls = sum(1 for i in range(NUM_ROWS) if null_fn(i))
        results["checks"][col_name]["null_count"] = null_count
        results["checks"][col_name]["expected_nulls"] = expected_nulls

    # Non-nullable integer columns
    check_int_col("ID", lambda i: i)
    check_int_col("VI8", lambda i: i % 127)
    check_int_col("VI16", lambda i: i % 32000)
    check_int_col("VI32", lambda i: i * 10)
    check_int_col("VI64", lambda i: i * 100)
    check_int_col("VU8", lambda i: i % 255)
    check_int_col("VU16", lambda i: i % 60000)
    check_int_col("VU32", lambda i: i * 1000)
    check_int_col("VU64", lambda i: i * 10000)

    # Float columns (use tolerance for float32)
    check_float_col("VF32", lambda i: float(i) * 0.5, tolerance=0.01)
    check_float_col("VF64", lambda i: float(i) * 1.5, tolerance=0.001)

    # Boolean column
    check_bool_col("VBOOL", lambda i: i % 2 == 0)

    # Nullable columns (every 5th row is null)
    check_nullable_int_col("NI32", lambda i: i * 10, lambda i: i % 5 == 0)
    check_nullable_float_col("NF64", lambda i: float(i) * 1.5, lambda i: i % 5 == 0)

    results["all_valid"] = all(c["pass"] for c in results["checks"].values())

    print(json.dumps(results, indent=2))
    return results


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <arrow_output_file>", file=sys.stderr)
        sys.exit(1)
    result = validate(sys.argv[1])
    sys.exit(0 if result.get("all_valid", False) else 1)
