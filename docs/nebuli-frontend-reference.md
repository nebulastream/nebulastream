# NebulaStream Frontend Reference

This document describes the verified functionality of NebulaStream's frontend interfaces as validated by automated
tests.

## Table of Contents

- [Overview](#overview)
- [NES-REPL (Interactive REPL)](#nes-repl-interactive-repl)
    - [Starting the REPL](#starting-the-repl)
    - [Embedded Mode](#embedded-mode)
    - [Distributed Mode](#distributed-mode)
- [NES-CLI (One-Shot Topology Controller)](#nes-cli-one-shot-topology-controller)
    - [Basic Usage](#basic-usage)
    - [Topology File Format](#topology-file-format)
    - [Query Management](#query-management)

---

## Overview

NebulaStream provides three primary frontend interfaces:

1. **`nes-repl-embedded`** - Single-node embedded worker with local query execution via interactive REPL
2. **`nes-repl`** - Distributed query controller for multi-node deployments via interactive REPL
3. **`nes-cli`** - Stateless one-shot CLI for deploying and controlling queries from topology files

All interfaces support JSON output for programmatic access.

---

## NES-REPL (Interactive REPL)

### Starting the REPL

```bash
# Embedded mode (single-node)
nes-repl-embedded -d -f JSON

# Distributed mode (multi-node)
nes-repl -d -f JSON
```

**Flags:**

- `-d` - Debug mode with detailed logging
- `-f <format>` - Output format: `JSON` for programmatic access, `TEXT` for tabular format (default: `TEXT`)
- `-s <address>` - Server address to connect to (default: `localhost:8080`). Not required for `nes-repl-embedded`.
- `--on-exit <behavior>` - Behavior when REPL exits (default: `DO_NOTHING`)
  - `DO_NOTHING` - Exit immediately, leaving queries running on workers
  - `WAIT_FOR_QUERY_TERMINATION` - Wait for all queries to finish before exiting
  - `STOP_QUERIES` - Stop all running queries and wait for termination before exiting
- `-e <behavior>` - Error handling behavior
  - `FAIL_FAST` - Exit with non-zero code on first error (default for non-interactive mode)
  - `RECOVER` - Ignore errors and continue (default for interactive mode)
  - `CONTINUE_AND_FAIL` - Continue execution but return non-zero exit code at the end

### Embedded Mode

The embedded mode runs queries locally on a single embedded worker. The worker is accessed internally at
`localhost:9090` (virtual address - no actual network port is allocated).

Sources and sinks are automatically placed on the single node. No `HOST` configuration is required.

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
    'ALL' as `SOURCE`.STOP_GENERATOR_WHEN_SEQUENCE_FINISHES,
    'CSV' as PARSER.`TYPE`,
    'emit_rate 10' AS `SOURCE`.GENERATOR_RATE_CONFIG,
    10000000 AS `SOURCE`.MAX_RUNTIME_MS,
    1 AS `SOURCE`.SEED,
    'SEQUENCE UINT64 0 10000000 1' AS `SOURCE`.GENERATOR_SCHEMA
);

-- 3. Create a sink (file output)
CREATE SINK someSink(ENDLESS.TS UINT64)
TYPE File
SET(
    'out.csv' as `SINK`.FILE_PATH,
    'CSV' as `SINK`.INPUT_FORMAT
);

-- 4. Check queries (should be empty initially)
SHOW QUERIES;
-- Returns: []

-- 5. Submit a query
SELECT TS FROM ENDLESS INTO SOMESINK;
-- Returns: [{"global_query_id":"<query-id>"}]

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
  "global_query_id": "amazing_stallion",
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
> Removing workers from the topology will not terminate running queries.
> In general the behavior around changing topologies is not yet specified.

#### Single Worker Example

```sql
-- 1. Register a worker node
CREATE WORKER "sink-node:9090" AT "sink-node:8080";
-- Returns: [{"worker":"sink-node:9090"}]

-- 2. Create logical source
CREATE LOGICAL SOURCE endless(ts UINT64);

-- 3. Create physical source with host specification
CREATE PHYSICAL SOURCE FOR endless
TYPE Generator
SET(
    'ALL' as `SOURCE`.STOP_GENERATOR_WHEN_SEQUENCE_FINISHES,
    'CSV' as PARSER.`TYPE`,
    'emit_rate 10' AS `SOURCE`.GENERATOR_RATE_CONFIG,
    10000000 AS `SOURCE`.MAX_RUNTIME_MS,
    "sink-node:9090" AS `SOURCE`.`HOST`,  -- Specify target host
    1 AS `SOURCE`.SEED,
    'SEQUENCE UINT64 0 10000000 1' AS `SOURCE`.GENERATOR_SCHEMA
);

-- 4. Create sink with host specification
CREATE SINK someSink(ENDLESS.TS UINT64)
TYPE File
SET(
    'out.csv' as `SINK`.FILE_PATH,
    'CSV' as `SINK`.INPUT_FORMAT,
    "sink-node:9090" AS `SINK`.`HOST`  -- Specify target host
);

-- 5. Deploy query
SELECT TS FROM ENDLESS INTO SOMESINK;
```

Query status shows one global query status as well as potentially multiple local query statuses (one per worker).

#### Complex Multi-Node Example (8-Node Topology)

```sql
-- worker creation (multi-statement)
CREATE WORKER "sink-node:9090" AT "sink-node:8080";
CREATE WORKER "source-node-1:9090" AT "source-node-1:8080"
    SET("intermediate-node-1:9090" AS `DOWNSTREAM`);
CREATE WORKER "source-node-2:9090" AT "source-node-2:8080"
    SET("intermediate-node-1:9090" AS `DOWNSTREAM`);
CREATE WORKER "source-node-3:9090" AT "source-node-3:8080"
    SET("intermediate-node-2:9090" AS `DOWNSTREAM`);
CREATE WORKER "source-node-4:9090" AT "source-node-4:8080"
    SET("intermediate-node-2:9090" AS `DOWNSTREAM`);
CREATE WORKER "source-node-5:9090" AT "source-node-5:8080"
    SET("intermediate-node-2:9090" AS `DOWNSTREAM`);
CREATE WORKER "intermediate-node-1:9090" AT "intermediate-node-1:8080"
    SET("sink-node:9090" AS `DOWNSTREAM`);
CREATE WORKER "intermediate-node-2:9090" AT "intermediate-node-2:8080"
    SET("sink-node:9090" AS `DOWNSTREAM`);

-- Deploy multiple queries to different nodes
SELECT ID, VALUE, TIMESTAMP
FROM Generator(..., "source-node-1:9090" AS `SOURCE`.`HOST`, ...)
INTO Print("sink-node:9090" AS `SINK`.`HOST`, ...);

SELECT ID, VALUE, TIMESTAMP
FROM Generator(..., "source-node-5:9090" AS `SOURCE`.`HOST`, ...)
INTO Print("sink-node:9090" AS `SINK`.`HOST`, ...);

-- Verify query distribution
SHOW QUERIES;
-- Returns: 8 total queries (2 global + 6 local instances across nodes)

-- Drop specific query
DROP QUERY WHERE ID='<query-id>';
```

---

## NES-CLI (One-Shot Topology Controller)

NES-CLI is a stateless CLI tool for deploying and managing queries based on YAML topology files. Unlike the interactive
REPL, `nes-cli` performs single operations and exits.

### Basic Usage

```bash
# Display help
nes-cli --help

# Dump topology (validate and print parsed topology)
nes-cli -t topology.yaml dump
nes-cli -d -t topology.yaml dump  # With debug output

# Start query from topology file
nes-cli -t topology.yaml start

# Start ad-hoc query (override topology query)
nes-cli -t topology.yaml start 'SELECT * FROM GENERATOR_SOURCE INTO VOID_SINK'

# Check query status
nes-cli -t topology.yaml status <query-id>

# Stop query
nes-cli -t topology.yaml stop <query-id>

# Stop multiple queries
nes-cli -t topology.yaml stop <query-id-1> <query-id-2> <query-id-3>

# Use environment variable for topology file
export NES_TOPOLOGY_FILE=topology.yaml
nes-cli dump
nes-cli start
```

**Flags:**

- `-t <file>` - Topology file path (or use `NES_TOPOLOGY_FILE` environment variable)
- `-d` - Debug mode with detailed logging

### Topology File Format

Topology files define the complete system state in YAML format, including workers, logical sources, physical sources,
and sinks. The `query` field can contain 0, 1, or multiple query statements:

- **Omitted**: No queries in topology file (use ad-hoc query via command line argument)
- **Single query**: `query: | SELECT ...` (string)
- **Multiple queries**: `query: [...]` (array of strings)

> [!NOTE]
> Providing a query via the command line (e.g., `nes-cli -t topology.yaml start 'SELECT ...'`) will override any queries
> defined in the topology file's `query` field.

**Example: Single Query Topology**

```yaml
query: |
  SELECT * FROM GENERATOR_SOURCE INTO VOID_SINK

sinks:
  - name: VOID_SINK
    host: worker-1:9090
    schema:
      - name: GENERATOR_SOURCE$DOUBLE
        type: FLOAT64
    type: Void
    config: { }

logical:
  - name: GENERATOR_SOURCE
    schema:
      - name: DOUBLE
        type: FLOAT64

physical:
  - logical: GENERATOR_SOURCE
    host: worker-1:9090
    parser_config:
      type: CSV
      fieldDelimiter: ","
    type: Generator
    source_config:
      generator_rate_type: FIXED
      generator_rate_config: emit_rate 10
      stop_generator_when_sequence_finishes: NONE
      seed: 1
      generator_schema: |
        NORMAL_DISTRIBUTION FLOAT64 0 1

workers:
  - host: worker-1:9090
    grpc: worker-1:8080
    capacity: 10000
```

**Example: Multiple Queries in Single Topology**

```yaml
query:
  - SELECT * FROM GENERATOR_SOURCE INTO VOID_SINK
  - SELECT * FROM GENERATOR_SOURCE INTO VOID_SINK
  - SELECT * FROM GENERATOR_SOURCE INTO VOID_SINK

sinks:
  - name: VOID_SINK
    host: worker-1:9090
    schema:
      - name: GENERATOR_SOURCE$DOUBLE
        type: FLOAT64
    type: Void
    config: { }

logical:
  - name: GENERATOR_SOURCE
    schema:
      - name: DOUBLE
        type: FLOAT64

physical:
  - logical: GENERATOR_SOURCE
    host: worker-1:9090
    parser_config:
      type: CSV
      fieldDelimiter: ","
    type: Generator
    source_config:
      generator_rate_type: FIXED
      generator_rate_config: emit_rate 1000
      stop_generator_when_sequence_finishes: NONE
      seed: 1
      generator_schema: |
        NORMAL_DISTRIBUTION FLOAT64 0 1

workers:
  - host: worker-1:9090
    grpc: worker-1:8080
    capacity: 10000
```

**Example: Ad-hoc Query (No Query in Topology)**

```yaml
# No query field - topology only defines infrastructure

sinks:
  - name: VOID_SINK
    host: worker-1:9090
    schema:
      - name: GENERATOR_SOURCE$DOUBLE
        type: FLOAT64
    type: Void
    config: { }

logical:
  - name: GENERATOR_SOURCE
    schema:
      - name: DOUBLE
        type: FLOAT64

physical:
  - logical: GENERATOR_SOURCE
    host: worker-1:9090
    parser_config:
      type: CSV
      fieldDelimiter: ","
    type: Generator
    source_config:
      generator_rate_type: FIXED
      generator_rate_config: emit_rate 10
      stop_generator_when_sequence_finishes: NONE
      seed: 1
      generator_schema: |
        NORMAL_DISTRIBUTION FLOAT64 0 1

workers:
  - host: worker-1:9090
    grpc: worker-1:8080
    capacity: 10000
```

```bash
# Provide query as command line argument
nes-cli -t topology.yaml start 'SELECT * FROM GENERATOR_SOURCE INTO VOID_SINK'
```

**Example: Multi-Worker Topology with Data Routing**

```yaml
query: |
  SELECT * FROM GENERATOR_SOURCE INTO VOID_SINK

sinks:
  - name: VOID_SINK
    host: worker-2:9090 # sink located at worker-2
    schema:
      - name: GENERATOR_SOURCE$DOUBLE
        type: FLOAT64
    type: Void
    config: { }

logical:
  - name: GENERATOR_SOURCE
    schema:
      - name: DOUBLE
        type: FLOAT64

physical:
  - logical: GENERATOR_SOURCE
    host: worker-1:9090 # source located at worker-1
    parser_config:
      type: CSV
      fieldDelimiter: ","
    type: Generator
    source_config:
      generator_rate_type: FIXED
      generator_rate_config: emit_rate 1000
      stop_generator_when_sequence_finishes: NONE
      seed: 1
      generator_schema: |
        NORMAL_DISTRIBUTION FLOAT64 0 1

workers:
  - host: worker-1:9090
    grpc: worker-1:8080
    capacity: 10000
    downstream: [ worker-2:9090 ]  # Route data to worker-2
  - host: worker-2:9090
    grpc: worker-2:8080
    capacity: 10000
```

### Query Management

**Checking Status:**

```bash
nes-cli -t topology.yaml status <query-id>
```

Returns JSON array with query status information:

```json
[
  {
    "global_query_id": "amazing_stallion",
    "query_status": "Running"
  },
  {
    "grpc_addr": "worker-1:8080",
    "query_status": "Running"
  },
  {
    "grpc_addr": "worker-2:8080",
    "query_status": "Running"
  }
]
```

**Query Status Values:**

- `"Running"` - Query is actively processing
- `"PartiallyStopped"` - Some query instances have stopped (e.g., source reached end of stream)
- `"Unreachable"` - Cannot reach one or more workers. Affected LocalQueries will have the `ConnectionError` state.

**Stopping Queries:**

```bash
# Stop single query
nes-cli -t topology.yaml stop <query-id>

# Stop multiple queries
nes-cli -t topology.yaml stop <id1> <id2> <id3>
```
