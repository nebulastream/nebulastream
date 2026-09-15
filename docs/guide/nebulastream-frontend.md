# NebulaStream Frontend Reference

This document describes the verified functionality of NebulaStream's frontend interfaces as validated by automated
tests.

## Table of Contents

- [Overview](#overview)
- [Identifier Case Sensitivity](#identifier-case-sensitivity)
- [NES-REPL (Interactive REPL)](#nes-repl-interactive-repl)
    - [Starting the REPL](#starting-the-repl)
    - [Embedded Mode](#embedded-mode)
    - [Distributed Mode](#distributed-mode)
- [NES-CLI (One-Shot CLI)](#nes-cli-one-shot-cli)
    - [Basic Usage](#basic-usage)
    - [Setup File Format](#setup-file-format)
    - [Query Management](#query-management)

---

## Overview

NebulaStream provides three primary frontend interfaces:

1. **`nes-repl-embedded`** - Interactive REPL that runs the coordinator and a single worker in its own process
2. **`nes-repl`** - Interactive REPL that runs the coordinator in its own process and deploys queries to remote workers
3. **`nes-cli`** - One-shot CLI that applies a setup file and starts, stops, or inspects queries

Every frontend runs its coordinator in its own process.
The coordinator stores workers, sources, sinks, models, and queries in its catalog.

The REPLs print JSON with `-f JSON`, and `nes-cli status` prints JSON.

### Version information

Every non-test binary accepts `--version` / `-v` and prints build metadata (git commit incl. `+dirty`, build
timestamp/type, sanitizer, compiler and effective flags, stdlib, log level, vcpkg baseline) — useful when reporting
issues or debugging remote deployments.

---

## Identifier Case Sensitivity

Unquoted SQL identifiers are case-insensitive and normalized to uppercase. For example, `stream`, `STREAM`, and
`StReAm` all refer to the same identifier.

Double-quoted identifiers are case-sensitive and preserve their exact spelling. Consequently, `"stream"` is distinct
from the unquoted identifier `stream` and from `"Stream"`. This applies to source, sink, field, alias, and configuration
key names in both REPL variants and the CLI. An unquoted identifier matches a quoted identifier only when the quoted
spelling matches its uppercase-normalized form; for example, `stream` matches `"STREAM"` but not `"stream"`.

```sql
CREATE LOGICAL SOURCE "quotedSource" ("mixedValue" UINT64);
CREATE SINK "quotedSink" ("projectedValue" UINT64) TYPE Void;

SELECT "mixedValue" AS "projectedValue"
FROM "quotedSource"
INTO "quotedSink";
```

Double quotes delimit identifiers; single quotes delimit string values. For example, `"status"` refers to a field,
whereas `'status'` is a string literal.

CLI setup files must include the SQL double quotes as part of the YAML string when defining a case-sensitive
identifier:

```yaml
logical:
  - name: '"quotedSource"'
    schema:
      - name: '"mixedValue"'
        type: UINT64

physical:
  - logical: '"quotedSource"'
    # ...
```

For quoted identifiers, JSON responses and CSV schema headers contain the preserved spelling without the surrounding
SQL double quotes. For example, `"mixedValue"` is reported as `mixedValue`.

---

## NES-REPL (Interactive REPL)

### Starting the REPL

```bash
# Embedded mode (single-node)
nes-repl-embedded -d -f JSON

# Distributed mode (multi-node)
nes-repl -d -f JSON

# Keep the catalog in a file, so a later start continues with the same workers, sources, sinks, and queries
nes-repl --db catalog.db
```

The REPL runs the coordinator in its own process, so it does not connect to a separate server.

**Flags:**

- `-d`, `--debug` - Debug mode with detailed logging
- `-f <format>` - Output format: `JSON` for programmatic access, `TEXT` for tabular format (default: `TEXT`)
- `--db <path>` - Path to a persistent SQLite catalog, created if it does not exist.
  Without it, the catalog is in memory and is lost when the REPL exits.
- `--on-exit <behavior>` - Behavior when REPL exits (default: `DO_NOTHING`)
  - `DO_NOTHING` - Exit immediately, leaving queries running on workers
  - `WAIT_FOR_QUERY_TERMINATION` - Wait for all queries to finish before exiting
  - `STOP_QUERIES` - Stop all running queries and wait for termination before exiting
- `-e`, `--error-behaviour <behavior>` - Error handling behavior
  - `FAIL_FAST` - Exit with non-zero code on first error (default for non-interactive mode)
  - `RECOVER` - Ignore errors and continue (default for interactive mode)
  - `CONTINUE_AND_FAIL` - Continue execution but return non-zero exit code at the end
- `-w`, `--workerConfig <file>` - YAML file with fully qualified optimizer options (`optimizer.*`) and,
  for `nes-repl-embedded`, worker options (`worker.*`)
- `-- <options>` - The same options on the command line, e.g. `-- --optimizer.join_strategy=HASH_JOIN`.
  They must not set an option that the `-w` file also sets.

### Embedded Mode

The embedded mode runs queries locally on a single embedded worker. By default, the worker is identified internally as
`localhost:8080` (virtual address - no actual network port is allocated).

Sources and sinks are automatically placed on the single node. No `HOST` configuration is required.

> [!NOTE]
> **Starting a worker:** Each worker is started with separate gRPC and data addresses:
> ```bash
> nes-single-node-worker -- --grpc=0.0.0.0:8080 --data_address=<dns-name/ip>:9090
> ```
> The gRPC address (port 8080 by convention) is the address used to identify workers in setup files and for source/sink placement. The data address (port 9090 by convention) is used internally for data transfer between workers.

> [!NOTE]
> In embedded mode, terminating the REPL also terminates the embedded worker. This means `--on-exit DO_NOTHING` and `--on-exit STOP_QUERIES` behave identically - all queries will be terminated when the REPL exits.

#### Basic Workflow Example

```sql
-- 1. Create a logical source schema
CREATE LOGICAL SOURCE endless(ts UINT64);

-- 2. Create a physical source (data generator)
CREATE PHYSICAL SOURCE FOR endless
TYPE Generator
SET(
    'ALL' as "SOURCE".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES,
    'CSV' as INPUT_FORMATTER."TYPE",
    'emit_rate 10' AS "SOURCE".GENERATOR_RATE_CONFIG,
    10000000 AS "SOURCE".MAX_RUNTIME_MS,
    1 AS "SOURCE".SEED,
    'SEQUENCE UINT64 0 10000000 1' AS "SOURCE".GENERATOR_SCHEMA
);

-- 3. Create a sink (file output)
CREATE SINK someSink(TS UINT64)
TYPE File
SET(
    'out.csv' as "SINK".FILE_PATH,
    'CSV' as "SINK".OUTPUT_FORMAT
);

-- 4. Check queries (should be empty initially)
SHOW QUERIES;
-- Returns: []

-- 5. Submit a query
SELECT TS FROM ENDLESS INTO SOMESINK;
-- Returns: [{"query_id":"<query-id>"}]

-- 6. View running queries
SHOW QUERIES;
-- Returns: Array with global and local query instances
-- Query statuses: "Running" | "Registered" | "Started"

-- 7. Filter queries by ID
SHOW QUERIES WHERE ID = '<query-id>';

-- 8. Drop a query
DROP QUERY WHERE ID = '<query-id>';

-- 9. Verify cleanup
SHOW QUERIES;
-- Returns: []
```

**Expected Response Structure:**

```json
{
  "query_id": "amazing_stallion",
  "query_status": "Running",
  "running": {
    "formatted": "2025-11-18 15:06:57.377000",
    "since_epoch": 1763478417377000,
    "unit": "microseconds"
  },
  "started": {
    "formatted": "2025-11-18 15:06:57.369000",
    "since_epoch": 1763478417369000,
    "unit": "microseconds"
  },
  "stopped": {
    "formatted": "1970-01-01 00:00:00.000000",
    "since_epoch": 0,
    "unit": "microseconds"
  }
}
```

### Distributed Mode

Distributed mode requires explicit worker registration/removal before deploying queries.
The `HOST` configuration is required for all sources and sinks to specify their target worker.
Queries are always deployed based on the most recent topology state.

> [!NOTE]
> `nes-repl` does not create or start workers - they must be started independently before registration.

> [!WARNING]
> `DROP WORKER` fails the worker's fragments that have not terminated, and with them their queries.
> The coordinator tries to stop those fragments on the worker, but gives up after a few seconds when the worker is unreachable.

#### Single Worker Example

```sql
-- 1. Register a worker node
CREATE WORKER 'sink-node:8080' SET ('sink-node:9090' AS DATA);
-- Returns: [{"worker":"sink-node:8080"}]

-- 2. Create logical source
CREATE LOGICAL SOURCE endless(ts UINT64);

-- 3. Create physical source with host specification
CREATE PHYSICAL SOURCE FOR endless
TYPE Generator
SET(
    'ALL' as "SOURCE".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES,
    'CSV' as INPUT_FORMATTER."TYPE",
    'emit_rate 10' AS "SOURCE".GENERATOR_RATE_CONFIG,
    10000000 AS "SOURCE".MAX_RUNTIME_MS,
    'sink-node:8080' AS "SOURCE"."HOST",  -- Specify target host (gRPC address)
    1 AS "SOURCE".SEED,
    'SEQUENCE UINT64 0 10000000 1' AS "SOURCE".GENERATOR_SCHEMA
);

-- 4. Create sink with host specification
CREATE SINK someSink(TS UINT64)
TYPE File
SET(
    'out.csv' as "SINK".FILE_PATH,
    'CSV' as "SINK".OUTPUT_FORMAT,
    'sink-node:8080' AS "SINK"."HOST"  -- Specify target host (gRPC address)
);

-- 5. Deploy query
SELECT TS FROM ENDLESS INTO SOMESINK;
```

Query status shows one global query status as well as potentially multiple local query statuses (one per worker).

#### Complex Multi-Node Example (8-Node Topology)

```sql
-- worker creation (multi-statement)
CREATE WORKER 'sink-node:8080' SET ('sink-node:9090' AS DATA);
CREATE WORKER 'source-node-1:8080' SET ('source-node-1:9090' AS DATA,
    'intermediate-node-1:8080' AS "DOWNSTREAM");
CREATE WORKER 'source-node-2:8080' SET ('source-node-2:9090' AS DATA,
    'intermediate-node-1:8080' AS "DOWNSTREAM");
CREATE WORKER 'source-node-3:8080' SET ('source-node-3:9090' AS DATA,
    'intermediate-node-2:8080' AS "DOWNSTREAM");
CREATE WORKER 'source-node-4:8080' SET ('source-node-4:9090' AS DATA,
    'intermediate-node-2:8080' AS "DOWNSTREAM");
CREATE WORKER 'source-node-5:8080' SET ('source-node-5:9090' AS DATA,
    'intermediate-node-2:8080' AS "DOWNSTREAM");
CREATE WORKER 'intermediate-node-1:8080' SET ('intermediate-node-1:9090' AS DATA,
    'sink-node:8080' AS "DOWNSTREAM");
CREATE WORKER 'intermediate-node-2:8080' SET ('intermediate-node-2:9090' AS DATA,
    'sink-node:8080' AS "DOWNSTREAM");

-- Deploy multiple queries to different nodes
SELECT ID, VALUE, TIMESTAMP
FROM Generator(..., 'source-node-1:8080' AS "SOURCE"."HOST", ...)
INTO Print('sink-node:8080' AS "SINK"."HOST", ...);

SELECT ID, VALUE, TIMESTAMP
FROM Generator(..., 'source-node-5:8080' AS "SOURCE"."HOST", ...)
INTO Print('sink-node:8080' AS "SINK"."HOST", ...);

-- Verify query distribution
SHOW QUERIES;
-- Returns: 8 total queries (2 global + 6 local instances across nodes)

-- Drop specific query
DROP QUERY WHERE ID='<query-id>';
```

---

## NES-CLI (One-Shot CLI)

`nes-cli` runs one command and exits.
Each invocation starts a coordinator in its own process, runs the command, and stops the coordinator.

The coordinator keeps its catalog in `$XDG_STATE_HOME/nebucli/coordinator.db`,
or in `$HOME/.local/state/nebucli/coordinator.db` when `XDG_STATE_HOME` is not set.
The catalog persists between invocations, so `stop` and `status` find the queries that an earlier `start` created.

> [!NOTE]
> The coordinator runs only while a command runs.
> Between invocations, the catalog does not record state changes on the workers, e.g. a query that completes.

### Basic Usage

```bash
nes-cli [-d] [--embedded] [-s <setup.yaml>] <command>
```

```bash
# Display help
nes-cli --help

# Print the plans of the setup file's queries without starting them
nes-cli -s setup.yaml dump
nes-cli -d -s setup.yaml dump  # With debug output

# Start the queries of the setup file, and wait until they run
nes-cli -s setup.yaml start

# Start an ad-hoc query in place of the setup file's queries
nes-cli -s setup.yaml start 'SELECT * FROM GENERATOR_SOURCE INTO VOID_SINK'

# Wait until the queries have completed, not only until they run
nes-cli -s setup.yaml start --until-completed

# Show the status of one query, or of all queries
nes-cli status <query-id>
nes-cli status

# Stop queries
nes-cli stop <query-id>
nes-cli stop <query-id-1> <query-id-2> <query-id-3>

# Use an environment variable for the setup file
export NES_SETUP_FILE=setup.yaml
nes-cli dump
nes-cli start

# Read the setup file from stdin
cat setup.yaml | nes-cli -s - dump
cat setup.yaml | nes-cli -s - start
```

**Flags:**

- `-s`, `--setup <file>` - Setup file path, or `-` to read from stdin
- `-d`, `--debug` - Debug mode with detailed logging
- `--embedded` - Run the queries on a worker in the CLI process, not on remote workers

**Commands:**

- `start [--until-completed] [<query>...]` - Applies the setup file and starts the given queries, or the setup file's queries when none are given.
  Prints the id of each started query on its own line.
  Waits up to 15 seconds until every query runs, or with `--until-completed`, without a time limit until every query has completed.
- `stop [-w <seconds>] <query-id>...` - Stops the queries and waits until they have terminated, for 15 seconds or the given time.
- `status [-w <seconds>] [<query-id>...]` - Prints the queries as JSON, all queries when no id is given.
  With `-w`, it first waits up to the given time until the workers have reported the state of every fragment that has not terminated.
- `dump [<query>...]` - Applies the setup file and prints the plans of the given queries, or of the setup file's queries.

Only `start` and `dump` read the setup file.

**Setup File Resolution Order:**

The CLI looks for the setup file in the following priority order:
1. `-s <file>` flag - Explicitly specified file path, or `-s -` to read from stdin
2. `NES_SETUP_FILE` environment variable
3. `setup.yaml` in current directory
4. `setup.yml` in current directory

### Setup File Format

Setup files define workers, logical sources, physical sources, sinks, and models in YAML format.
Each command that reads the file creates only what does not exist in the catalog yet.
The `workers` field is required.
The `query` field can contain 0, 1, or multiple query statements:

- **Omitted**: No queries in the setup file (use ad-hoc query via command line argument)
- **Single query**: `query: | SELECT ...` (string)
- **Multiple queries**: `query: [...]` (array of strings)

> [!NOTE]
> Providing a query via the command line (e.g., `nes-cli -s setup.yaml start 'SELECT ...'`) will override any queries
> defined in the setup file's `query` field.

**Example: Single Query Setup**

```yaml
query: |
  SELECT * FROM GENERATOR_SOURCE INTO VOID_SINK

sinks:
  - name: VOID_SINK
    host: worker-1:8080
    schema:
      - name: DOUBLE
        type: FLOAT64
    type: Void
    config: { }
    parser_config: { }

logical:
  - name: GENERATOR_SOURCE
    schema:
      - name: DOUBLE
        type: FLOAT64

physical:
  - logical: GENERATOR_SOURCE
    host: worker-1:8080
    parser_config:
      type: CSV
      field_delimiter: ","
    type: Generator
    source_config:
      generator_rate_type: FIXED
      generator_rate_config: emit_rate 10
      stop_generator_when_sequence_finishes: NONE
      seed: 1
      generator_schema: |
        NORMAL_DISTRIBUTION FLOAT64 0 1

workers:
  - host: worker-1:8080
    data_address: worker-1:9090
    max_operators: 10000
```

**Example: Multiple Queries in a Single Setup File**

```yaml
query:
  - SELECT * FROM GENERATOR_SOURCE INTO VOID_SINK
  - SELECT * FROM GENERATOR_SOURCE INTO VOID_SINK
  - SELECT * FROM GENERATOR_SOURCE INTO VOID_SINK

sinks:
  - name: VOID_SINK
    host: worker-1:8080
    schema:
      - name: DOUBLE
        type: FLOAT64
    type: Void
    config: { }
    parser_config: { }

logical:
  - name: GENERATOR_SOURCE
    schema:
      - name: DOUBLE
        type: FLOAT64

physical:
  - logical: GENERATOR_SOURCE
    host: worker-1:8080
    parser_config:
      type: CSV
      field_delimiter: ","
    type: Generator
    source_config:
      generator_rate_type: FIXED
      generator_rate_config: emit_rate 1000
      stop_generator_when_sequence_finishes: NONE
      seed: 1
      generator_schema: |
        NORMAL_DISTRIBUTION FLOAT64 0 1

workers:
  - host: worker-1:8080
    data_address: worker-1:9090
    max_operators: 10000
```

**Example: Ad-hoc Query (No Query in the Setup File)**

```yaml
# No query field - the setup file only defines infrastructure

sinks:
  - name: VOID_SINK
    host: worker-1:8080
    schema:
      - name: DOUBLE
        type: FLOAT64
    type: Void
    config: { }
    parser_config: { }

logical:
  - name: GENERATOR_SOURCE
    schema:
      - name: DOUBLE
        type: FLOAT64

physical:
  - logical: GENERATOR_SOURCE
    host: worker-1:8080
    parser_config:
      type: CSV
      field_delimiter: ","
    type: Generator
    source_config:
      generator_rate_type: FIXED
      generator_rate_config: emit_rate 10
      stop_generator_when_sequence_finishes: NONE
      seed: 1
      generator_schema: |
        NORMAL_DISTRIBUTION FLOAT64 0 1

workers:
  - host: worker-1:8080
    data_address: worker-1:9090
    max_operators: 10000
```

```bash
# Provide query as command line argument
nes-cli -s setup.yaml dump 'SELECT * FROM GENERATOR_SOURCE INTO VOID_SINK'
nes-cli -s setup.yaml start 'SELECT * FROM GENERATOR_SOURCE INTO VOID_SINK'
```

**Example: Multi-Worker Setup with Data Routing**

```yaml
query: |
  SELECT * FROM GENERATOR_SOURCE INTO VOID_SINK

sinks:
  - name: VOID_SINK
    host: worker-2:8080 # sink located at worker-2
    schema:
      - name: DOUBLE
        type: FLOAT64
    type: Void
    config: { }
    parser_config: { }

logical:
  - name: GENERATOR_SOURCE
    schema:
      - name: DOUBLE
        type: FLOAT64

physical:
  - logical: GENERATOR_SOURCE
    host: worker-1:8080 # source located at worker-1
    parser_config:
      type: CSV
      field_delimiter: ","
    type: Generator
    source_config:
      generator_rate_type: FIXED
      generator_rate_config: emit_rate 1000
      stop_generator_when_sequence_finishes: NONE
      seed: 1
      generator_schema: |
        NORMAL_DISTRIBUTION FLOAT64 0 1

workers:
  - host: worker-1:8080
    data_address: worker-1:9090
    max_operators: 10000
    downstream: [ worker-2:8080 ]  # Route data to worker-2
  - host: worker-2:8080
    data_address: worker-2:9090
    max_operators: 10000
```

### Model Registration

The setup file supports an optional `models` section for registering ML models. Models are registered before
queries are submitted, so they can be referenced by `MODEL_INFERENCE` in the query.

```yaml
models:
  - name: iris
    path: /path/to/iris.onnx
    input:
      - name: p1
        type: FLOAT32
      - name: p2
        type: FLOAT32
      - name: p3
        type: FLOAT32
      - name: p4
        type: FLOAT32
    output:
      - name: setosa
        type: FLOAT32
      - name: versicolor
        type: FLOAT32
      - name: virginica
        type: FLOAT32
```

Each model entry requires:
- `name` - Identifier used in `MODEL_INFERENCE(name, ...)` queries
- `path` - Absolute path to an `.onnx` model file (must exist at registration time)
- `input` - List of input fields with name and type (must match the model's input tensor)
- `output` - List of output fields with name and type (must match the model's output tensor)

For the equivalent SQL syntax and full usage examples, see `guide/query_api.md`.

**Example: Complete Setup File with Model Inference**

```yaml
query: |
  SELECT * FROM MODEL_INFERENCE(iris, stream) INTO result

models:
  - name: iris
    path: /data/model/iris.onnx
    input:
      - name: p1
        type: FLOAT32
      - name: p2
        type: FLOAT32
      - name: p3
        type: FLOAT32
      - name: p4
        type: FLOAT32
    output:
      - name: setosa
        type: FLOAT32
      - name: versicolor
        type: FLOAT32
      - name: virginica
        type: FLOAT32

sinks:
  - name: result
    host: worker-1:8080
    schema:
      - name: stream$p1
        type: FLOAT32
      - name: stream$p2
        type: FLOAT32
      - name: stream$p3
        type: FLOAT32
      - name: stream$p4
        type: FLOAT32
      - name: SETOSA
        type: FLOAT32
      - name: VERSICOLOR
        type: FLOAT32
      - name: VIRGINICA
        type: FLOAT32
    type: Print
    config:
      output_format: CSV
    parser_config: { }

logical:
  - name: stream
    schema:
      - name: p1
        type: FLOAT32
      - name: p2
        type: FLOAT32
      - name: p3
        type: FLOAT32
      - name: p4
        type: FLOAT32

physical:
  - logical: stream
    host: worker-1:8080
    parser_config:
      type: CSV
      field_delimiter: ","
    type: generator
    source_config:
      generator_schema: NORMAL_DISTRIBUTION FLOAT64 0 1,NORMAL_DISTRIBUTION FLOAT64 0 1,NORMAL_DISTRIBUTION FLOAT64 0 1,NORMAL_DISTRIBUTION FLOAT64 0 1,
      stop_generator_when_sequence_finishes: NONE

workers:
  - data_address: worker-1:9090
    host: worker-1:8080
```

### Query Management

**Checking Status:**

```bash
nes-cli status <query-id>

# Wait up to 5 seconds until the workers have reported the state of every fragment that has not terminated
nes-cli status -w 5 <query-id>
```

Returns a JSON array with one entry per query.
Each entry lists the query's fragments, one per worker that runs a part of the query,
and each fragment has the state of its worker in `worker_state`.
An abridged example:

```json
[
  {
    "id": 1,
    "sql": "SELECT * FROM GENERATOR_SOURCE INTO VOID_SINK",
    "state": "Running",
    "error": null,
    "fragments": [
      {
        "id": 1,
        "host_addr": "worker-1:8080",
        "current_state": "Running",
        "desired_state": "Completed",
        "worker_state": "Active"
      }
    ]
  }
]
```

**Query States:**

- `"Pending"`, `"Started"` - The query is being deployed to its workers
- `"Running"` - Every fragment of the query runs or has completed
- `"Completed"` - The query ended by itself, e.g. because its sources reached the end of their streams
- `"Stopped"` - The query was stopped
- `"Failed"` - A fragment failed, and `error` holds the first failure and its worker

**Worker States:** `"Pending"`, `"Active"`, `"Unreachable"`, `"Removed"`.

**Stopping Queries:**

```bash
# Stop single query
nes-cli stop <query-id>

# Stop multiple queries, and wait up to 30 seconds until they have terminated
nes-cli stop -w 30 <id1> <id2> <id3>
```
