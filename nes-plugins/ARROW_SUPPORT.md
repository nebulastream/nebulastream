# Arrow Support in NebulaStream

## Overview

NebulaStream supports Apache Arrow as both an internal buffer format and an external I/O format.
Internally, `ArrowBufferRef` provides a columnar Arrow-compatible memory layout for TupleBuffers.
Externally, Arrow IPC streaming format files can be read (ArrowFileSource) and written
(ArrowFileSink), and data can be streamed over the network via Arrow Flight (ArrowFlightSink).

## Components

### ArrowBufferRef (Internal Buffer Layout)

**Files**: `nes-nautilus/include/Nautilus/Interface/BufferRef/ArrowBufferRef.hpp`,
`nes-nautilus/src/Nautilus/Interface/BufferRef/ArrowBufferRef.cpp`

Implements a columnar Arrow-compatible memory layout within a single TupleBuffer, with no
dependency on Apache Arrow headers. The scan and emit operators use this layout when the parser
type is `ARROW`.

**Buffer layout**: `[bitmap_col0][bitmap_col1]...[data_col0][data_col1]...`

- Each region is 8-byte aligned.
- Validity bitmaps use 1 bit per record (Arrow convention: 1 = valid, 0 = null).
- BOOLEAN data is bit-packed (1 bit per record), matching Arrow's native boolean format.
- VARSIZED (string) columns store int32 offset arrays in the main buffer; actual string bytes
  live in child buffers attached to the TupleBuffer, interleaved with stride = numVarSizedColumns.
- Fixed-width columns (integers, floats) are stored contiguously.

**Layout computation** lives in `LowerSchemaProvider::lowerSchemaWithOutputFormat()`. The source,
scan operator, emit operator, and sink all use the same computation to ensure offset agreement.

### ArrowFileSource (Read Arrow IPC Files)

**Files**: `nes-plugins/Sources/ArrowFileSource/`

Reads Arrow IPC streaming format files (`.arrows`) and produces TupleBuffers in ArrowBufferRef
layout. Avoids columnar-to-row-to-columnar conversion by writing directly into the Arrow columnar
layout that the scan operator expects.

**SQL usage**:
```sql
CREATE LOGICAL SOURCE data(id UINT64 NOT NULL, name VARSIZED);
CREATE PHYSICAL SOURCE FOR data TYPE ArrowFile SET(
    '/path/to/data.arrows' AS `SOURCE`.FILE_PATH,
    'ARROW' AS PARSER.`TYPE`);
```

**Supported types**: INT8, INT16, INT32, INT64, UINT8, UINT16, UINT32, UINT64, FLOAT32, FLOAT64,
BOOLEAN, VARSIZED. All types support nullable variants.

### ArrowFileSink (Write Arrow IPC Files)

**Files**: `nes-plugins/Sinks/ArrowFileSink/`

Writes TupleBuffers as Arrow RecordBatches to a file in Arrow IPC streaming format. The output
files are readable by any Arrow-compatible tool (PyArrow, DuckDB, Polars, Spark, etc.).

The sink performs zero-copy wrapping of TupleBuffer memory as Arrow arrays for fixed-width and
BOOLEAN columns. VARSIZED columns with multiple child buffers require a concatenation copy.

**SQL usage**:
```sql
CREATE SINK out(SRC.ID UINT64 NOT NULL, SRC.NAME VARSIZED) TYPE ArrowFile SET(
    '/path/to/output.arrows' AS `SINK`.FILE_PATH);
```

### ArrowFlightSink (Stream via Arrow Flight)

**Files**: `nes-plugins/Sinks/ArrowFlightSink/`

Streams Arrow RecordBatches to an external Arrow Flight server via gRPC DoPut. Same zero-copy
RecordBatch wrapping as ArrowFileSink.

**SQL usage**:
```sql
CREATE SINK out(...) TYPE ArrowFlight SET(
    'grpc://localhost:5005' AS `SINK`.ENDPOINT,
    'my-stream' AS `SINK`.STREAM_NAME);
```

### ArrowOutputFormatter (Plugin Registry Shim)

**Files**: `nes-plugins/OutputFormatters/ArrowOutputFormatter/`

Routes the "Arrow" output format to ArrowBufferRef in the plugin registry. Does not perform
actual formatting — `writeFormattedValue()` throws if called.

### PipeliningPhase Integration

**File**: `nes-query-compiler/src/Phases/PipeliningPhase.cpp`

When the parser type is `ARROW`, `createScanOperator()` creates a `ScanPhysicalOperator` with an
ArrowBufferRef instead of an InputFormatter. For source-to-sink pipelines with non-NATIVE formats,
a scan+emit pipeline is inserted between the source and sink.

## Test Coverage

### Unit Tests

**ArrowBufferRefTest** (30 tests, compiled + interpreted):
- `MixedTypesWithNullableAndVarSized` — all field types in one schema
- `AllFixedWidthTypes` — every integer and float type
- `SmallEmitBufferProducesMultipleOutputs` — buffer overflow splits across outputs
- `EmptyInput` — zero rows handled correctly
- `LargeStrings` — strings near buffer size
- `LargeStringsMultipleChildBuffers` — strings exceeding single child buffer
- `StringLargerThanChildBuffer` — unpooled allocation for oversized strings
- `NullableVarSized` — nullable string fields with null bitmap
- `MultipleVarSizedColumns` — child buffer interleaving with stride
- `AllNullVarSized` — all-null string column
- `EmptyStringVsNullString` — distinguishes empty string from null
- `NullableVarSizedArrowBitmap` — Arrow validity bitmap for nullable strings
- `BooleanColumnRowToArrow` — Scan(ROW) → Emit(Arrow) with 100 boolean rows
- `BooleanColumnArrowToArrow` — Scan(Arrow) → Map → Emit(Arrow), 14-column schema
- `BooleanMultiBufferArrowToArrow` — multi-buffer execution with boolean columns

**ArrowFileSourceTest** (7 enabled, 3 disabled):
- `FixedWidthRoundtrip` — write via sink, read back, verify int32/uint64/double values
- `NullableRoundtrip` — nullable fields with null bitmap round-trip
- `VarSizedRoundtrip` — string data through child buffers
- `MultipleBatches` — multiple source fillTupleBuffer calls
- `LargeBatchSplitAcrossBuffers` — batch exceeding buffer capacity
- `BooleanColumnRoundtrip` — 100-row boolean round-trip
- `VarSizedExceedsSingleChildBuffer` — strings requiring multiple child buffers

**ArrowFileSinkTest** (3 tests):
- `WriteFixedWidthTypes` — validates IPC output with PyArrow reader
- `NullableFieldsRoundtrip` — nullable column output
- `MultipleBatches` — multiple execute() calls produce multi-batch file

### End-to-End BATS Tests

- **`arrow_file.bats`** — Generator → ArrowFileSink → validate with PyArrow
- **`arrow_flight.bats`** — Generator → ArrowFlightSink → validate with Flight server
- **`arrow_roundtrip.bats`** — Generator → ArrowFileSink → ArrowFileSource → CSV → validate
- **`pyarrow_interop.bats`** — PyArrow generates 1000-row file with all types → NES reads and
  writes back → PyArrow validates every value. Covers: INT8, INT16, INT32, INT64, UINT8, UINT16,
  UINT32, UINT64, FLOAT32, FLOAT64, BOOLEAN, nullable INT32, nullable FLOAT64.

### Python Utilities

- `generate_arrow_testdata.py` — generates deterministic 1000-row Arrow IPC file with 14 columns
- `validate_arrow_roundtrip.py` — validates NES output against expected values per column
- `validate_arrow_file.py` — validates Arrow file structure and content
- `flight_server.py` — receives Arrow Flight DoPut streams for testing

## Limitations

### 1. Source Loads Full Arrow Batch Into Memory

ArrowFileSource uses `arrow::ipc::RecordBatchStreamReader` which loads an entire RecordBatch into
memory. The source then slices it into TupleBuffer-sized chunks. If the input file contains a
single large batch (e.g., millions of rows), the entire batch is held in memory while being
drained.

**Proper fix**: Implement an Arrow InputFormatter (like the CSV formatter) that reads raw bytes
incrementally and parses Arrow IPC framing across TupleBuffer boundaries. This would make the
source a generic file reader with bounded memory usage.

### 2. No Schema Validation in ArrowFileSource

The source does not validate the Arrow file's schema against the declared NES logical source
schema. If the file has different column counts, types, or ordering, the source silently produces
corrupt data.

**Impact**: Three unit tests are disabled pending this fix:
- `SchemaMismatchDetected` — different column count
- `SchemaTypeMismatchDetected` — different column types
- `NonexistentFileRejectedAtValidation` — file existence not checked at CREATE time

### 3. No Arrow IPC File Format Support

Only Arrow IPC **streaming** format (`.arrows`) is supported. Arrow IPC **file** format (with
random access footer), Parquet, Feather, and other Arrow-based formats are not supported.

### 4. Bit-by-Bit Bitmap Copies in Source

The source copies validity bitmaps and BOOLEAN data bit-by-bit in a loop. When source and
destination bit offsets are byte-aligned (the common case), this could be replaced with `memcpy`
for better performance.

### 5. ArrowFlightSink Nullable Column Issue

The Arrow Flight test server notes that nullable columns are "EXPECTED TO FAIL due to inline null
byte issue." Nullable fields transmitted via Arrow Flight DoPut may produce incorrect results.

### 6. No ArrowFlightSource

There is no source that reads from an Arrow Flight server (DoGet). Only the sink direction
(DoPut) is implemented.

### 7. Sink Layout Computation Duplicated

ArrowFileSink and ArrowFlightSink each have their own capacity/offset computation in
`wrapBufferAsRecordBatch()` that must stay in sync with `LowerSchemaProvider`. Any layout change
requires updating three places.
