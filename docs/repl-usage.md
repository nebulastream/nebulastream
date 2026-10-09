# Driving NebulaStream: REPL and CLI

How to deploy, inspect and stop queries — with the ODBC source and HTTP sink in mind.
Everything below was exercised against a live `nes-single-node-worker` on
`prospective-study-charity`.

## Pick the right tool first

There are three front-ends, and the choice is not cosmetic — it decides whether your
queries survive the command that created them.

| Binary | Worker | State lives in | Use it for |
| --- | --- | --- | --- |
| `nes-repl-embedded` | starts its own, in-process | the process | one-off experiments, smoke tests |
| `nes-repl` | connects to a running one | **the repl process** | an interactive exploration session |
| `nes-cli` | connects to a running one | a topology file + on-disk query state | **an interactive operational workload** |

**For a workload of "deploy a query, go do something else, come back and ask for status,
deploy another" — use `nes-cli`.** The reason is in the next section.

## The trap: repl state does not survive the process

`nes-repl` keeps the logical-source, sink and worker catalogs *in its own process*. A
second invocation starts empty. But queries you deployed keep running in the worker,
because the default `--on-exit` is `DO_NOTHING`.

Measured:

```
INVOCATION A: deploy query          -> "query_id":"aaaaaaaa-1111-..."
INVOCATION B: SHOW QUERIES          -> []
INVOCATION C: SHOW LOGICAL SOURCES  -> []
   ...meanwhile the sink file kept growing: 49 bytes -> 127 bytes
```

So a sequence of separate `nes-repl` commands **orphans queries**: still running, no
longer addressable. `nes-repl` is fine as *one long-lived interactive session*, and wrong
as a series of one-shot commands.

`--on-exit` controls this: `DO_NOTHING` (default, queries survive),
`STOP_QUERIES` (tear them down with the session), `WAIT_FOR_QUERY_TERMINATION`.

## The interactive operational workload: `nes-cli`

`nes-cli` is stateless by design. Each invocation rebuilds the cluster picture from a
topology file and reads query ids from an on-disk state backend, so separate commands
compose.

Start the worker once, and leave it running:

```bash
nes-single-node-worker            # listens on [::]:8080
```

Then, one command at a time:

```bash
# deploy — prints the query id on stdout
nes-cli -t topology.yaml start
# -> swift_appaloosa_4121

# ...go do something else...

# ask for status
nes-cli -t topology.yaml status swift_appaloosa_4121

# deploy another, while the first is still running
nes-cli -t topology.yaml start

# stop the first
nes-cli -t topology.yaml stop swift_appaloosa_4121
```

`status` returns one entry for the distributed query and one per worker-local query:

```json
[
    {
        "query_id": "swift_appaloosa_4121",
        "query_status": "Running",
        "started": { "formatted": "2026-07-30 11:57:51.169000", ... },
        "running": { "formatted": "2026-07-30 11:57:51.196000", ... }
    },
    {
        "query_id": "swift_appaloosa_4121",
        "local_query_id": "e13af3ee-2cb9-44b2-94cd-ec2953513c54",
        "worker": "localhost:8080",
        "query_status": "Running", ...
    }
]
```

After `stop`, `query_status` becomes `Stopped` and a `stopped` timestamp appears.

### Choosing the query id

Scraping the generated name off stdout is only necessary if you let it be generated. As in
the repl, a query can name itself, and `nes-cli` now deploys it under that id:

```bash
nes-cli -t topology.yaml start \
  'SELECT ts FROM GEN INTO OUT SET ('"'"'nightly-rollup'"'"' AS "QUERY"."ID")'
# -> nightly-rollup
```

That works for the `query:` block of the topology file too. For a query you do not want to
edit — an ad-hoc one, or the one in the topology file — `--query-id` sets the id from
outside and overrides an id the query carries:

```bash
nes-cli -t topology.yaml start --query-id nightly-rollup
nes-cli -t topology.yaml status nightly-rollup
nes-cli -t topology.yaml stop   nightly-rollup
```

An id names one query in the cluster, so `--query-id` is rejected when the invocation
submits more than one query — give those their ids in the queries themselves.

For a single-node deployment you can drop the topology file for `status` and `stop`, since
neither performs placement:

```bash
nes-cli --no-topology status swift_appaloosa_4121
nes-cli --no-topology stop   swift_appaloosa_4121
```

The worker set is recovered from the persisted query state, which already records which
host each local query runs on. `start` still needs `-t`, because placement does need the
topology.

Resolution order if you omit `-t`: the `-t` flag, then `NES_TOPOLOGY_FILE`, then
`topology.yaml` / `topology.yml` in the working directory. Without `--no-topology` a
missing topology file is still an error, so a typo'd path fails loudly rather than
silently running against an empty cluster.

### Topology file

```yaml
query: |
  SELECT ts FROM GEN INTO OUT

workers:
  - host: localhost:8080

sinks:
  - name: OUT
    host: localhost:8080
    schema:
      - name: ts
        type: UINT64
        nullable: false
    type: File
    config:                       # sinks use `config`
      file_path: /tmp/cli_out.csv
      output_format: CSV

logical:
  - name: GEN
    schema:
      - name: ts
        type: UINT64
        nullable: false

physical:
  - logical: GEN
    host: localhost:8080
    parser_config:
      type: CSV
    type: Generator
    source_config:                # physical sources use `source_config`, NOT `config`
      seed: 1
      generator_schema: SEQUENCE UINT64 0 10000000 1
      stop_generator_when_sequence_finishes: ALL
```

The YAML loader is strict and its errors are good — it names the offending key, the path
and the line: `Unknown key 'config' at physical[0] (line 32). Expected one of: logical,
type, host, parser_config, source_config`.

## Interactive session with `nes-repl`

When you want to explore rather than operate, run one session and keep it open:

```bash
nes-repl -s localhost:8080 -f JSON
```

Inside, the query-lifecycle statements are:

```sql
SHOW QUERIES;
SHOW QUERIES WHERE id = '<query-id>';
DROP QUERY WHERE id = '<query-id>';
```

Give queries your own id so you can address them without scraping generated names:

```sql
SELECT ts FROM gen INTO out SET ('11111111-2222-3333-4444-555555555555' AS "QUERY"."ID");
```

### `nes-repl` needs more than `nes-repl-embedded`

A script that works embedded will not necessarily work against a real worker. The
distributed path additionally requires:

1. `CREATE WORKER 'localhost:8080';` before anything else — otherwise
   `placement failure; Found errors in query plan`.
2. `'localhost:8080' AS "SOURCE".HOST` on every physical source, and `"SINK".HOST` on
   every sink — otherwise `Could not handle source statement. "SOURCE"."HOST" was not set`.
3. Stricter source validation (e.g. Generator's `STOP_GENERATOR_WHEN_SEQUENCE_FINISHES`
   becomes required).

### Scripting the embedded REPL

`nes-repl-embedded` exits at stdin EOF and takes its in-process worker with it, killing a
still-running query. Hold stdin open:

```bash
{ cat query.sql; sleep 45; } | nes-repl-embedded -f JSON
```

## SQL config syntax

Config entries are `<value> AS <PREFIX>.<KEY>`. Values are **single-quoted** — double
quotes mean identifier.

```sql
CREATE PHYSICAL SOURCE FOR readings TYPE ODBC SET(
    'NATIVE' AS INPUT_FORMATTER."TYPE",
    'SELECT id, ts, reading FROM dbo.readings WHERE ts > ? ORDER BY ts' AS "SOURCE"."QUERY",
    '10.0.0.5' AS "SOURCE"."DB_HOST",
    'ODBC Driver 18 for SQL Server' AS "SOURCE"."DRIVER",
    'true' AS "SOURCE"."TRUST_SERVER_CERTIFICATE"
);
```

- **Keys are UPPERCASE.** Unquoted identifiers are canonicalised to upper case, so a
  lower-case config key is unreachable and validation throws `std::out_of_range`.
- **Quote reserved words** used as keys: `"SOURCE"."QUERY"`, `INPUT_FORMATTER."TYPE"`.
  Others (`"SOURCE".DB_HOST`) are fine bare. Quoted-uppercase and unquoted-uppercase
  canonicalise identically, so quoting everything is safe.
- **The input-format prefix is `INPUT_FORMATTER`, not `PARSER`.** systest accepts
  `PARSER`; the REPL and CLI do not. Omitting it gives *"Source config does not contain
  input formatter type"*.

## HTTP sink

```sql
CREATE SINK alerts(id INT32 NOT NULL, reading FLOAT64) TYPE HTTP SET(
    '10.0.0.9' AS "SINK".IP_ADDRESS,
    '8000' AS "SINK".PORT,
    'alerts' AS "SINK".ENDPOINT,
    '/var/log/nes/httpsink.log' AS "SINK".LOG_FILE_PATH,
    'JSON' AS "SINK".OUTPUT_FORMAT
);
```

- **`OUTPUT_FORMAT` must be set explicitly.** It falls back to `NATIVE`, and the compiler
  then inserts no output formatter — the sink POSTs raw binary tuples instead of JSON.
- **`LOG_FILE_PATH` is required** (no default); `start()` fails if the file cannot be
  opened. Every payload is mirrored there and flushed immediately.
- Posts as `Content-Type: application/x-ndjson`, one JSON object per tuple:
  `{"ID":2,"READING":39.1}`.
- The sink does not set `CURLOPT_WRITEFUNCTION`, so libcurl prints every HTTP **response
  body to stdout**. Harmless, but it will fill your logs.

## ODBC source

- The query needs **exactly one `?`** — the `> watermark` lower bound. Any other count is
  rejected at validation.
- **The watermark starts at connect time and there is no end-of-stream.** Rows that
  already exist are never returned, and a restart does not backfill.
- Set `TIMEZONE_OFFSET_HOURS` if the source column stores local wall-clock rather than UTC.

## LOOKBACK: enriching an alert from a database when it fires

`LOOKBACK` runs a pre-defined query against a named database connection and renders the
result rows into one `VARSIZED` string. It exists for the alert queries: the message a
clinician receives needs context the stream does not carry — prior values for the case,
the last blood-gas panel — and that context has to be fetched **only when a threshold is
breached**, because the clinical database cannot be polled per tuple.

```sql
SELECT FMT(VARSIZED('PH {} bei Fall {}. Vorwerte: {}'),
           Wert, mlife_FallNr,
           LOOKBACK(VARSIZED('mlife'),
                    VARSIZED('SELECT TOP 3 Bez, Wert FROM dbo.LabVal WHERE FallNr = ? ORDER BY Zeitpunkt DESC'),
                    VARSIZED('{}={}; '),
                    mlife_FallNr)) AS ALERT_TEXT
FROM medical_data_source
WHERE Bez = 'PH' AND CASTTOTYPE(Wert AS FLOAT64) < 7.2
INTO alertSink;
```

Arguments: connection name, the SQL, the row template, then one expression per `?` in the
SQL. The first three must be constant literals — they are resolved once when the query is
compiled, not per tuple.

- **The trigger semantics are structural, not a feature of the function.** A function in
  the `SELECT` list is evaluated by a projection that sits above the selection, and
  `PredicatePushdownRule` pushes the `WHERE` further down still. So the database is touched
  once per tuple that clears the filter, and not at all otherwise. Put a `LOOKBACK` in a
  query with no meaningful filter and it runs per tuple — there is nothing in the function
  to stop it.
- **Parameters are bound, never interpolated.** `?` markers inside string literals are not
  counted, so `WHERE note LIKE 'why?%' AND id = ?` has one parameter.
- **A constant the lookback filters on has to be a bound parameter, not an embedded
  literal.** The SQL argument is itself a NES string literal, and NES has no way to carry a
  quote through one: `''` ends the NES literal (a parse error), and `\'` survives with its
  backslash intact, so the database receives `\'` and rejects it. Write `AND Bez = ?` and
  pass the value — a stream field or a `VARSIZED(...)` constant — as a further argument.
  That is the better form regardless: it is bound rather than interpolated, and it lets one
  prepared statement serve every value.
- **Placeholders are positional.** `{}` takes result columns in order; the count must match
  the query's column count. A named placeholder such as `{Wert}` is rejected rather than
  silently treated as positional.
- **No rows renders as the empty string**, not NULL. `LOOKBACK` is `NOT NULL`.
- **A database failure does not lose the alert.** By default the alert is emitted with an
  empty lookback string and a warning is logged. Set `strict: true` on the connection to
  make the failure fail the query instead.
- Each worker thread opens its own connection lazily and prepares the statement once. The
  round trip is synchronous: a stalled database delays that pipeline for up to
  `query_timeout_ms`.

### Connections

Credentials stay out of query text. Connections are named in a YAML file pointed at by
`NES_LOOKBACK_CONFIG`, read the first time a connection is resolved:

```yaml
connections:
  mlife:
    db_host: 10.0.0.5
    db_port: "1433"
    database: mlife
    username: nes_reader
    password: ${MLIFE_PASSWORD}   # ${VAR} is expanded from the environment
    driver: ODBC Driver 18 for SQL Server
    trust_server_certificate: true
    query_timeout_ms: 2000        # default 2000; login and query timeout
    max_rows: 10                  # default 10; rows rendered into the string
    max_result_bytes: 8192        # default 8192; truncated at a UTF-8 boundary
    strict: false                 # default false
  # A fixture: canned rows, no database. This is how the systests run in CI.
  fixture:
    rows:
      - ["HB", "6.2"]
```

An unset `${VAR}` is an error, not an empty expansion — a missing password fails loudly
instead of attempting an anonymous connect. An unknown key is rejected too, so a typo in
`password` cannot silently become an anonymous connect either.

**The node you submit the query to needs the config file, not just the node that runs it.**
The connection name is resolved during type inference, at submission. That is deliberate:
lowering happens per placed sub-plan, and in a distributed plan the projection carrying the
`LOOKBACK` can land on a different worker than the selection. A lowering failure there
leaves the upstream worker's network sink retrying a channel whose receiver never came up,
and the query hangs instead of failing.

## Gotchas that cost time

- **Rebuild the front-end binaries.** A stale `nes-repl` / `nes-repl-embedded` rejects
  valid SQL with confusing `no viable alternative` grammar errors — including the repl's
  own test file. `cmake --build <build> --target nes-repl nes-repl-embedded nes-cli`.
- **systest cannot configure non-File/Checksum sinks.** `SLTSinkFactory::registerSink`
  (`nes-systests/systest/src/SystestBinder.cpp`) ignores the parsed sink config and
  rebuilds one for `File` and `CHECKSUM` only. HTTP-sink work has to go through the REPL
  or CLI.
