# NebulaStream Frontend Reference

This page is the reference for NebulaStream's frontend interfaces. For step-by-step examples on how to deploy the system
and run queries, see [Quick Start](../getting-started/get_started.md) and
[Distributed Deployment](../getting-started/distributed.md).

## Overview

NebulaStream provides three primary frontend interfaces:

1. **`nes-repl-embedded`** - Single-node embedded worker with local query execution via interactive REPL
2. **`nes-repl`** - Distributed query controller for multi-node deployments via interactive REPL
3. **`nes-cli`** - Stateless one-shot CLI for deploying and controlling queries from topology files

All interfaces support JSON output for programmatic access.

### Version Information

Every non-test binary accepts `--version` / `-v` and prints build metadata (git commit incl. `+dirty`, build
timestamp/type, sanitizer, compiler and effective flags, stdlib, log level, vcpkg baseline).

## NES-Single-Node-Worker (Worker)

`nes-single-node-worker` runs a worker, which executes queries. In distributed deployments, every worker is started
separately and then registered with `CREATE WORKER` in `nes-repl` or in the `workers` section of a `nes-cli` topology
file. `nes-repl-embedded` contains its own worker and needs no separate one.

The worker is available as the Docker image [`nebulastream/nes-worker`](https://hub.docker.com/r/nebulastream/nes-worker),
which runs `nes-single-node-worker`, so flags are given directly after the image name.

```bash
nes-single-node-worker -- --grpc=0.0.0.0:8080 --data_address=<dns-name/ip>:9090
```

### Flags

- `-h`, `--help` - Print the list of flags and all worker options with their defaults, then exit
- `-v`, `--version` - Print build information and exit (see [Version Information](#version-information))
- `-w <file>`, `--workerConfig <file>` - Read worker options from a YAML file. Options given after `--` override the
  values from the file.
- `-- <worker options>` - Everything after `--` is a worker option in the form `--<option>=<value>`

### Worker Options

- `--grpc=<address>` - Address the worker listens on (default: `[::]:8080`). `[::]:<port>` or `0.0.0.0:<port>`
  accept connections on all interfaces. In `CREATE WORKER`, in the `workers` section of a topology file, and as `HOST`
  of sources and sinks, the worker is referred to by an address under which it is reachable, e.g. `worker-1:8080`.
- `--data_address=<host:port>` - Address used for data transfer between workers. Required on every worker of a
  distributed deployment, including workers that only send data. It must be an address under which the other workers
  can reach this worker (not `0.0.0.0`), and it must match the `DATA` address in `CREATE WORKER` or the `data_address`
  in the topology file. Otherwise, no data arrives at the sink.
- `--enable_event_trace=true` - Write a trace file (`trace_<data address>_<timestamp>.json`) in Chrome tracing format
  to the working directory, for performance analysis (default: `false`).
- `--worker.<option>=<value>` - Query engine options, e.g. `--worker.query_engine.number_of_worker_threads=4`, memory
  budget, buffer sizes, and network queue sizes. Run `nes-single-node-worker --help` for the full list.

In a YAML file passed with `-w`, the same options are written as nested keys:

```yaml
grpc: 0.0.0.0:8080
data_address: worker-1:9090
worker:
  query_engine:
    number_of_worker_threads: 4
```

## NES-REPL (Interactive REPL)

### Starting the REPL

Both REPL variants are available as Docker images,
[`nebulastream/nes-repl-embedded`](https://hub.docker.com/r/nebulastream/nes-repl-embedded) and
[`nebulastream/nes-repl`](https://hub.docker.com/r/nebulastream/nes-repl), which run the respective binary, so flags are
given directly after the image name. Use `docker run -i` to pass statements via stdin.

```bash
# Embedded mode (single-node)
nes-repl-embedded -d -f JSON

# Distributed mode (multi-node)
nes-repl -d -f JSON
```

#### Flags

- `-h`, `--help` - Print the list of flags and exit
- `-v`, `--version` - Print build information and exit (see [Version Information](#version-information))
- `-d`, `--debug` - Enable debug logging, written to `nes-repl.log` in the working directory. To see a query plan,
  use `EXPLAIN`.
- `-f <format>` - Output format: `JSON` for programmatic access, `TEXT` for tabular format (default: `TEXT`). A single
  `SHOW` statement can override it with `FORMAT`; see [Statements](#statements).
- `--on-exit <behavior>` - Behavior when the REPL exits (default: `DO_NOTHING`)
  - `DO_NOTHING` - Exit immediately, leaving queries running on workers
  - `WAIT_FOR_QUERY_TERMINATION` - Wait for all queries to finish before exiting
  - `STOP_QUERIES` - Stop all running queries and wait for termination before exiting
- `-e <behavior>`, `--error-behaviour <behavior>` - Error handling behavior
  - `FAIL_FAST` - Exit with non-zero code on first error (default if stdin or stdout is not a terminal)
  - `RECOVER` - Print the error and continue (default if stdin and stdout are terminals)
- `--optimizer <key>=<value>` - Override a query optimizer option; can be repeated. See
  [Optimizer Options](#optimizer-options). Example: `--optimizer join_strategy=HASH_JOIN`
- `-- <worker options>` - `nes-repl-embedded` only. Everything after `--` configures the embedded worker, using the
  same [worker options](#worker-options) as `nes-single-node-worker`. Example:
  `-- --worker.query_engine.number_of_worker_threads=2`

#### Optimizer Options

These options are set with `--optimizer <key>=<value>` in the REPL and in the `optimizer` section of a
[topology file](#topology-file-format). Unknown keys and values are rejected.

| Key | Values | Default | Description |
|---|---|---|---|
| `join_strategy` | `NESTED_LOOP_JOIN`, `HASH_JOIN`, `OPTIMIZER_CHOOSES` | `OPTIMIZER_CHOOSES` | Join implementation used for stream joins. |
| `network.sender_queue_size` | number | `1024` | Size of the sender queue per network channel. |
| `network.max_pending_acks` | number | `64` | Maximum number of buffers sent but not yet acknowledged per network channel. |
| `network.receiver_queue_size` | number | `10` | Size of the receiver queue per network channel. |
| `network.backpressure_upper_threshold` | number | `1000` | Number of buffered tuples at which a network channel applies backpressure. |
| `network.backpressure_lower_threshold` | number | `200` | Number of buffered tuples at which a network channel releases backpressure. |

#### Interactive Commands

When both stdin and stdout are terminals, the REPL also accepts these commands:

- `help` - Show the list of commands
- `clear` - Clear the screen
- `quit` - Exit the REPL
- `exit` - Alias for `quit`

### Statements

Statements end with a semicolon. Names in `WHERE` filters are matched against the names that `SHOW` returns, which
are uppercase for unquoted identifiers (see [Identifier Case Sensitivity](#identifier-case-sensitivity)).

| Statement | Syntax | Description |
|---|---|---|
| `CREATE LOGICAL SOURCE` | `CREATE LOGICAL SOURCE <name>(<field> <type> [NOT NULL], ...)` | Defines a logical source and its schema. See [Sources and Sinks](sources-and-sinks.md). |
| `CREATE PHYSICAL SOURCE` | `CREATE PHYSICAL SOURCE FOR <logical source> TYPE <type> SET(<value> AS <key>, ...)` | Defines a physical source for a logical source. |
| `CREATE SINK` | `CREATE SINK <name>(<field> <type>, ...) TYPE <type> SET(<value> AS <key>, ...)` | Defines a sink. |
| `CREATE WORKER` | `CREATE WORKER '<host:port>' SET('<host:port>' AS DATA, '<host:port>' AS "DOWNSTREAM", <n> AS "CAPACITY")` | `nes-repl` only. Registers a worker by its gRPC address. `DATA` is its data address (see [Worker Options](#worker-options)), `DOWNSTREAM` names a worker it sends data to and can be repeated, `CAPACITY` limits how many operators are placed on it. |
| `CREATE MODEL` | `CREATE MODEL <name>('<path>') INPUT(<field> <type>, ...) OUTPUT(<field> <type>, ...)` | Registers an ML model. See [Model Inference](operators.md#model-inference). |
| `SHOW QUERIES` | `SHOW QUERIES [WHERE ID = '<query id>']` | Shows the status of all queries, or of one query. See [Query Status Output](#query-status-output). |
| `SHOW LOGICAL SOURCES` | `SHOW LOGICAL SOURCES [WHERE NAME = '<name>']` | Lists logical sources and their schemas. |
| `SHOW PHYSICAL SOURCES` | `SHOW PHYSICAL SOURCES [FOR <logical source>] [WHERE ID = <id>]` | Lists physical sources with their IDs and configuration. |
| `SHOW SINKS` | `SHOW SINKS [WHERE NAME = '<name>']` | Lists sinks and their configuration. |
| `SHOW MODELS` | `SHOW MODELS` | Lists registered models. |
| `SHOW VERSION` | `SHOW VERSION` | Shows the build information of each worker. |
| `DROP QUERY` | `DROP QUERY WHERE ID = '<query id>'` | Stops a query. The query stays listed by `SHOW QUERIES` with status `Stopped`. |
| `DROP LOGICAL SOURCE` | `DROP LOGICAL SOURCE WHERE NAME = '<name>'` | Removes a logical source. |
| `DROP PHYSICAL SOURCE` | `DROP PHYSICAL SOURCE WHERE ID = <id>` | Removes a physical source; the ID is shown by `SHOW PHYSICAL SOURCES`. |
| `DROP SINK` | `DROP SINK WHERE NAME = '<name>'` | Removes a sink. |
| `DROP MODEL` | `DROP MODEL WHERE NAME = '<name>'` | Removes a model. |
| `EXPLAIN` | `EXPLAIN [(<stage>, ...)] [FORMAT TEXT\|VISUAL\|VERBOSE] <query>` | Prints the plan of a query without running it. Stages are `LOGICAL`, `OPTIMIZED`, and `DISTRIBUTED` (default: all three). |
| Query | `<query>` | Submits a query and returns its query ID. See [Operators](operators.md) and [Functions](functions.md). |

Every `SHOW` statement accepts a trailing `FORMAT TEXT` or `FORMAT JSON`, which overrides `-f` for that statement.
Every `DROP` statement returns the entry it removed.

### Embedded Mode

The embedded mode runs queries locally on a single embedded worker. By default, the worker is identified internally as
`localhost:8080`.

Sources and sinks are automatically placed on the single node. No `HOST` configuration is required.

> [!NOTE]
> In embedded mode, terminating the REPL also terminates the embedded worker. This means `--on-exit DO_NOTHING` and `--on-exit STOP_QUERIES` behave identically - all queries will be terminated when the REPL exits.

### Distributed Mode

Distributed mode requires explicit worker registration before deploying queries.
The `HOST` configuration is required for all sources and sinks to specify their target worker.
Queries are always deployed based on the most recent topology state.

> [!NOTE]
> `nes-repl` does not create or start workers - they must be started independently before registration; see
> [NES-Single-Node-Worker](#nes-single-node-worker-worker).

> [!WARNING]
> In general the behavior around changing topologies is not yet specified.

For a walkthrough of a distributed deployment, see [Distributed Deployment](../getting-started/distributed.md).

### Query Status Output

`SHOW QUERIES` returns one entry per query instance:

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
  }
}
```

`running` and `stopped` only appear once the query has reached that state.

## NES-CLI (One-Shot Topology Controller)

NES-CLI is a "stateless" CLI tool for deploying and managing queries based on YAML topology files. Unlike the REPL, `nes-cli` performs single operations and exits. The CLI is stateless in the sense that source/sink catalogs and topology configuration are always loaded from YAML files—nothing is persisted between invocations.

The CLI is available as the Docker image [`nebulastream/nes-cli`](https://hub.docker.com/r/nebulastream/nes-cli), which
runs `nes-cli`, so flags are given directly after the image name.

> [!NOTE]
> **Implementation Detail:** To enable query management across CLI invocations, the CLI maintains an internal mapping of global query IDs to local query instances in `$XDG_STATE_HOME/nebucli/` (or `$HOME/.local/state/nebucli/`). This is an implementation detail and should not be relied upon.

### Basic Usage

```bash
# Display help
nes-cli --help

# Print the query plans (like EXPLAIN) without starting the queries
nes-cli -t topology.yaml dump
nes-cli -d -t topology.yaml dump  # With debug output

# Start query from topology file
nes-cli -t topology.yaml start

# Start ad-hoc query (override topology query)
nes-cli -t topology.yaml start 'SELECT * FROM GENERATOR_SOURCE INTO VOID_SINK'

# Check query status
nes-cli -t topology.yaml status <query-id>

# Show all queries running on each worker
nes-cli -t topology.yaml status

# Stop query
nes-cli -t topology.yaml stop <query-id>

# Stop multiple queries
nes-cli -t topology.yaml stop <query-id-1> <query-id-2> <query-id-3>

# Use environment variable for topology file
export NES_TOPOLOGY_FILE=topology.yaml
nes-cli dump
nes-cli start

# Read topology from stdin
cat topology.yaml | nes-cli -t - dump
cat topology.yaml | nes-cli -t - start
cat topology.yaml | nes-cli -t - start 'SELECT * FROM GENERATOR_SOURCE INTO VOID_SINK'
cat topology.yaml | nes-cli -t - status <query-id>
cat topology.yaml | nes-cli -t - stop <query-id>

# Works with Docker too
cat topology.yaml | docker run -i nebulastream/nes-cli -t - start
cat topology.yaml | docker run -i nebulastream/nes-cli -t - dump
cat topology.yaml | docker run -i nebulastream/nes-cli -t - stop <query-id>
cat topology.yaml | docker run -i nebulastream/nes-cli -t - status <query-id>
```

#### Flags

- `-h`, `--help` - Print the list of flags and subcommands and exit
- `-v`, `--version` - Print build information and exit
- `-t <file>` - Topology file path, or `-` to read from stdin
- `-d`, `--debug` - Enable debug logging, printed to the console and written to `nes-cli.log` in the working directory.
  To see query plans, use `dump`.

### Topology File Format

A topology file is a YAML file that describes everything `nes-cli` needs to run queries: the workers, the sources and
sinks with their schemas, optionally ML models and optimizer settings, and the queries themselves. Each section
corresponds to a REPL [statement](#statements) and takes the same settings.

| Key | Required | Corresponds to | Fields |
|---|---|---|---|
| `workers` | yes | `CREATE WORKER` | `host` (gRPC address) and `data_address` (see [Worker Options](#worker-options)), and optionally `max_operators` (like `CAPACITY`) and `downstream` (list of worker hosts) |
| `logical` | yes | `CREATE LOGICAL SOURCE` | `name`, `schema` |
| `physical` | yes, unless every query defines its sources inline | `CREATE PHYSICAL SOURCE` | `logical`, `type`, `host`, `parser_config` ([input formatter parameters](sources-and-sinks.md#input-formatter-parameters)), and optionally `source_config` ([source parameters](sources-and-sinks.md#source-parameters)) |
| `sinks` | yes, unless every query defines its sink inline | `CREATE SINK` | `name`, `type`, `host`, `schema`, and optionally `config` ([sink parameters](sources-and-sinks.md#sink-parameters)) and `parser_config` ([output formatter parameters](sources-and-sinks.md#output-formatter-parameters)) |
| `query` | no | Query | One query as a string, or several queries as a list of strings |
| `models` | no | `CREATE MODEL` | `name`, `path`, `input`, `output`; see [Model Inference](operators.md#model-inference) |
| `optimizer` | no | `--optimizer` | [Optimizer options](#optimizer-options) as nested keys, e.g. `join_strategy: HASH_JOIN` or `network: { sender_queue_size: 512 }` |

- A `schema` is a list of fields, each with `name`, `type`, and optionally `nullable: true` (default: `false`).
- Parameter names are written in lowercase, e.g. `stop_generator_when_sequence_finishes: NONE` in `source_config`.
- A query can define a source or sink inline instead of referring to a named one, e.g.
  `INTO Void('worker-2:8080' AS "SINK"."HOST")`.
- Unknown top-level keys are rejected.

> [!NOTE]
> A query given on the command line (e.g., `nes-cli -t topology.yaml start 'SELECT ...'`) overrides the queries in the
> `query` field. Without a `query` field, the query must be given on the command line.

For a complete topology file, see
[Distributed Deployment](../getting-started/distributed.md#write-the-topology-file).

### Query Management

#### Checking Status

```bash
nes-cli -t topology.yaml status <query-id>
```

Returns a JSON array with one global entry and one entry per worker, with the same fields as
[Query Status Output](#query-status-output) (shortened here):

```json
[
  {
    "query_id": "amazing_stallion",
    "query_status": "Running"
  },
  {
    "worker": "worker-1:8080",
    "query_status": "Running"
  },
  {
    "worker": "worker-2:8080",
    "query_status": "Running"
  }
]
```

#### Query Status Values

- `"Running"` - Query is actively processing
- `"PartiallyStopped"` - Some query instances have stopped (e.g., source reached end of stream)
- `"Unreachable"` - Cannot reach one or more workers. Affected LocalQueries will have the `ConnectionError` state.

#### Stopping Queries

```bash
# Stop single query
nes-cli -t topology.yaml stop <query-id>

# Stop multiple queries
nes-cli -t topology.yaml stop <id1> <id2> <id3>
```

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

CLI topology files must include the SQL double quotes as part of the YAML string when defining a case-sensitive
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
