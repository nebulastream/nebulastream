NebulaStream provides two primary frontend interfaces for single-node deployments:

1. **`nes-repl-embedded`** - Single-node embedded worker with local query execution via interactive REPL
2. **`nes-cli`** - Stateless one-shot CLI for deploying and controlling queries from topology files (also usable in single-node topologies)

Both interfaces support JSON output for programmatic access.

## Version information

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

---

## `nes-repl-embedded` (Interactive REPL)

### Starting the REPL

```bash
# Embedded mode (single-node)
nes-repl-embedded -d -f JSON
```

**Flags:**

- `-d` - Debug mode with detailed logging
- `-f <format>` - Output format: `JSON` for programmatic access, `TEXT` for tabular format (default: `TEXT`)
- `--on-exit <behavior>` - Behavior when REPL exits (default: `DO_NOTHING`)
  - `DO_NOTHING` - Exit immediately, leaving queries running on workers
  - `WAIT_FOR_QUERY_TERMINATION` - Wait for all queries to finish before exiting
  - `STOP_QUERIES` - Stop all running queries and wait for termination before exiting
- `-e <behavior>` - Error handling behavior
  - `FAIL_FAST` - Exit with non-zero code on first error (default for non-interactive mode)
  - `RECOVER` - Ignore errors and continue (default for interactive mode)
  - `CONTINUE_AND_FAIL` - Continue execution but return non-zero exit code at the end

### Embedded Mode

The embedded mode runs queries locally on a single embedded worker. By default, the worker is identified internally as
`localhost:8080` (virtual address - no actual network port is allocated).

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

For distributed, multi-worker deployments, see [Distributed Deployment]({{< ref "Distributed" >}}).
