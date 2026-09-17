---
title: "NebuLi: Distributed Deployment"
weight: 60
---

NebulaStream provides two primary frontend interfaces for distributed deployments:

1. **`nes-repl`** - Distributed query controller for multi-node deployments via interactive REPL
2. **`nes-cli`** - Stateless one-shot CLI for deploying and controlling queries from topology files

Both interfaces support JSON output for programmatic access.

---

## `nes-repl` (Interactive REPL)

### Starting the REPL

```bash
nes-repl -d -f JSON

# As a client of a running nes-server, which then holds the catalog and the workers
nes-repl --coordinator http://127.0.0.1:8081 -f JSON
```

**Flags:**

- `-d` - Debug mode with detailed logging
- `-f <format>` - Output format: `JSON` for programmatic access, `TEXT` for tabular format (default: `TEXT`)
- `--coordinator <url>` - Use the coordinator of the `nes-server` at this URL instead of starting one in this process. `--db` and `--optimizer` then belong to the server and are rejected.
- `--on-exit <behavior>` - Behavior when REPL exits (default: `DO_NOTHING`)
  - `DO_NOTHING` - Exit immediately, leaving queries running on workers
  - `WAIT_FOR_QUERY_TERMINATION` - Wait for all queries to finish before exiting
  - `STOP_QUERIES` - Stop all running queries and wait for termination before exiting
- `-e <behavior>` - Error handling behavior
  - `FAIL_FAST` - Exit with non-zero code on first error (default for non-interactive mode)
  - `RECOVER` - Ignore errors and continue (default for interactive mode)
  - `CONTINUE_AND_FAIL` - Continue execution but return non-zero exit code at the end

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

## `nes-cli` (One-Shot Topology Controller)

NES-CLI is a stateless one-shot client of a running coordinator, `nes-server`. Unlike the REPL, `nes-cli` performs single operations and exits: it registers the workers, sources, sinks and models of a YAML setup file on the coordinator, submits queries, and reads or stops them. Nothing is persisted in the CLI itself; the coordinator keeps the catalog and drives the workers between invocations.

The coordinator has to be running, with the workers started separately:

```bash
nes-server --listen 0.0.0.0:8081                 # remote workers, registered through the CLI
nes-server --listen 0.0.0.0:8081 --optimizer-config '{"join_strategy":"HASH_JOIN"}'
```

### Basic Usage

```bash
# Display help
nes-cli --help

# Where the coordinator is (default http://127.0.0.1:8081), as a flag or from the environment
nes-cli --coordinator http://coordinator:8081 status
export NES_COORDINATOR=http://coordinator:8081

# Dump the query plans (registers the setup, plans the queries without executing them)
nes-cli -s setup.yaml dump
nes-cli -d -s setup.yaml dump  # With debug logging in nes-cli.log

# Start the queries of a setup file, waiting until they run
nes-cli -s setup.yaml start
nes-cli -s setup.yaml start --until-completed   # wait until they complete instead

# Start ad-hoc queries (override the setup file's queries)
nes-cli -s setup.yaml start 'SELECT * FROM GENERATOR_SOURCE INTO VOID_SINK'

# Show all queries, or the given ones, with their fragments
nes-cli status
nes-cli status <query-id>

# Stop queries, waiting up to 15 seconds (or -w <seconds>) for them to terminate
nes-cli stop <query-id>
nes-cli stop <query-id-1> <query-id-2> <query-id-3>

# Run any SQL statement on the coordinator
nes-cli sql 'SHOW QUERIES'

# Use the environment variable for the setup file
export NES_SETUP_FILE=setup.yaml
nes-cli dump
nes-cli start

# Read the setup from stdin
cat setup.yaml | nes-cli -s - dump
cat setup.yaml | nes-cli -s - start 'SELECT * FROM GENERATOR_SOURCE INTO VOID_SINK'

# Works with Docker too
cat setup.yaml | docker run -i -e NES_COORDINATOR=http://coordinator:8081 nebulastream/nes-cli -s - start
```

**Flags:**

- `--coordinator <url>` - The coordinator's base URL, also read from `NES_COORDINATOR`; default `http://127.0.0.1:8081`
- `-s <file>` - Setup file path, or `-` to read from stdin
- `-o auto|json|table` - Output format: `auto` prints a table on a terminal and JSON otherwise
- `-d` - Debug mode with detailed logging in `nes-cli.log`

**Setup File Resolution Order:**

The CLI looks for the setup file in the following priority order:
1. `-s <file>` flag - Explicitly specified file path, or `-s -` to read from stdin
2. `NES_SETUP_FILE` environment variable
3. `setup.yaml` in current directory
4. `setup.yml` in current directory

An `optimizer:` section in a setup file is ignored with a warning: the optimizer is configured on the coordinator, which plans every query, with `nes-server --optimizer-config`.

### Topology File Format

Topology files define the complete system state in YAML format, including workers, logical sources, physical sources,
and sinks. The `query` field can contain 0, 1, or multiple query statements:

- **Omitted**: No queries in topology file (use ad-hoc query via command line argument)
- **Single query**: `query: | SELECT ...` (string)
- **Multiple queries**: `query: [...]` (array of strings)

> [!NOTE]
> Providing a query via the command line (e.g., `nes-cli -s setup.yaml start 'SELECT ...'`) will override any queries
> defined in the topology file's `query` field.

**Example: Single Query Topology**

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

**Example: Multi-Worker Topology with Data Routing**

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

The topology file supports an optional `models` section for registering ML models. Models are registered before
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

For the equivalent SQL syntax and full usage examples, see the [Query API]({{< ref "APIConcepts" >}}) guide.

### Query Management

**Checking Status:**

```bash
nes-cli status <query-id>
```

When the output is not a terminal (or with `-o json`), the CLI prints one JSON object per query with its fragments,
each fragment carrying the state of the worker it is placed on:

```json
[
  {
    "id": 1,
    "name": null,
    "sql": "SELECT * FROM GENERATOR_SOURCE INTO VOID_SINK",
    "state": "Running",
    "start_timestamp": "2026-09-16T10:00:00Z",
    "stop_timestamp": null,
    "error": null,
    "fragments": [
      {
        "id": 1,
        "query_id": 1,
        "host_addr": "worker-1:8080",
        "num_operators": 3,
        "has_source": true,
        "current_state": "Running",
        "desired_state": "Completed",
        "start_timestamp": "2026-09-16T10:00:00Z",
        "stop_timestamp": null,
        "error": null,
        "last_observed_at": "2026-09-16T10:00:05Z",
        "worker_state": "Active"
      }
    ]
  }
]
```

On a terminal (or with `-o table`) the same information is printed as tables: the queries, then the fragments of
each worker.

**Query States:** `Pending`, `Started`, `Running`, `Completed`, `Stopped`, `Failed`. A worker's state is `Active`
or `Unreachable`; a query keeps running while one of its workers is unreachable, and the fragments resume when the
worker comes back.

**Stopping Queries:**

```bash
# Stop single query
nes-cli stop <query-id>

# Stop multiple queries, waiting up to 60 seconds for them to terminate
nes-cli stop -w 60 <id1> <id2> <id3>
```
