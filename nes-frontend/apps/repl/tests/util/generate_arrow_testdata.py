#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["pyarrow>=15.0"]
# ///
"""
Generate an Arrow IPC stream file with 1000 rows covering all NES-supported types.

Usage:
    ./generate_arrow_testdata.py <output_arrow_file>

Types covered (matching NES DataType → Arrow mapping):
  INT8, INT16, INT32, INT64, UINT8, UINT16, UINT32, UINT64,
  FLOAT32, FLOAT64, BOOLEAN
  (plus nullable variants of INT32 and FLOAT64)

Each column has deterministic values derived from the row index so the
consumer can verify correctness.
"""

import sys

import pyarrow as pa
import pyarrow.ipc as ipc

NUM_ROWS = 1000


def generate(path: str) -> None:
    ids = list(range(NUM_ROWS))

    schema = pa.schema([
        pa.field("ID", pa.uint64(), nullable=False),
        pa.field("VI8", pa.int8(), nullable=False),
        pa.field("VI16", pa.int16(), nullable=False),
        pa.field("VI32", pa.int32(), nullable=False),
        pa.field("VI64", pa.int64(), nullable=False),
        pa.field("VU8", pa.uint8(), nullable=False),
        pa.field("VU16", pa.uint16(), nullable=False),
        pa.field("VU32", pa.uint32(), nullable=False),
        pa.field("VU64", pa.uint64(), nullable=False),
        pa.field("VF32", pa.float32(), nullable=False),
        pa.field("VF64", pa.float64(), nullable=False),
        pa.field("VBOOL", pa.bool_(), nullable=False),
        pa.field("NI32", pa.int32(), nullable=True),
        pa.field("NF64", pa.float64(), nullable=True),
    ])

    # Deterministic values derived from row index
    col_id = pa.array(ids, type=pa.uint64())
    col_vi8 = pa.array([i % 127 for i in ids], type=pa.int8())
    col_vi16 = pa.array([i % 32000 for i in ids], type=pa.int16())
    col_vi32 = pa.array([i * 10 for i in ids], type=pa.int32())
    col_vi64 = pa.array([i * 100 for i in ids], type=pa.int64())
    col_vu8 = pa.array([i % 255 for i in ids], type=pa.uint8())
    col_vu16 = pa.array([i % 60000 for i in ids], type=pa.uint16())
    col_vu32 = pa.array([i * 1000 for i in ids], type=pa.uint32())
    col_vu64 = pa.array([i * 10000 for i in ids], type=pa.uint64())
    col_vf32 = pa.array([float(i) * 0.5 for i in ids], type=pa.float32())
    col_vf64 = pa.array([float(i) * 1.5 for i in ids], type=pa.float64())
    col_vbool = pa.array([i % 2 == 0 for i in ids], type=pa.bool_())

    # Nullable columns: every 5th row is null
    ni32_vals = [None if i % 5 == 0 else i * 10 for i in ids]
    nf64_vals = [None if i % 5 == 0 else float(i) * 1.5 for i in ids]
    col_ni32 = pa.array(ni32_vals, type=pa.int32())
    col_nf64 = pa.array(nf64_vals, type=pa.float64())

    batch = pa.record_batch(
        [col_id, col_vi8, col_vi16, col_vi32, col_vi64,
         col_vu8, col_vu16, col_vu32, col_vu64,
         col_vf32, col_vf64, col_vbool,
         col_ni32, col_nf64],
        schema=schema,
    )

    with pa.OSFile(path, "wb") as f:
        writer = ipc.new_stream(f, schema)
        writer.write_batch(batch)
        writer.close()

    print(f"Wrote {NUM_ROWS} rows to {path}", file=sys.stderr)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <output_arrow_file>", file=sys.stderr)
        sys.exit(1)
    generate(sys.argv[1])
