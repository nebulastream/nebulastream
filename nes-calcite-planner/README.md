# nes-calcite-planner (prototype)

Plans a NebulaStream `SELECT` query with Apache Calcite and returns it as a Substrait plan over gRPC.
Service and messages: `src/main/proto/nes_calcite_planner.proto`.

```
export JAVA_HOME=<jdk 21>
mvn package                                    # runs the tests, builds target/nes-calcite-planner.jar
java -jar target/nes-calcite-planner.jar 50061
```

## Contract

- The request carries the SQL and the schemas of all sources it may reference. Names resolve case-insensitively.
- Unsigned fields are widened to the next signed type. The caller re-infers the real types from its source schemas.
- Columns used in `DESCRIPTOR(...)` are typed as `TIMESTAMP(3)` for that query, since TUMBLE/HOP require timestamps.
  They appear in Substrait as `precision_timestamp<3>`.
- Standard Substrait relations: `read` (named table), `filter`, `project` (uses `emit` to select output columns), `set` (union).
- Windowed aggregations arrive as `extension_single` whose `detail` is a `nes.calcite.WindowAggregate`.
  Its field indices refer to the input relation; output is `window_start, window_end, group_keys..., aggregates...`.
- Unsupported or invalid queries fail with `INVALID_ARGUMENT`.

## Supported windows

```sql
SELECT window_start, window_end, id, SUM(val) AS s
FROM TABLE(TUMBLE(TABLE s1, DESCRIPTOR(ts), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, id

-- HOP(table, DESCRIPTOR(ts), slide, size)
FROM TABLE(HOP(TABLE s1, DESCRIPTOR(ts), INTERVAL '10' SECOND, INTERVAL '1' MINUTE))
```

The aggregation must group by `window_start` and/or `window_end`. It can use SUM, COUNT, COUNT(*), MIN, MAX and AVG,
without DISTINCT or FILTER. A `WHERE` on non-window columns is allowed. Window offsets and the deprecated
`GROUP BY TUMBLE(...)` syntax are not supported.

## Joins, subqueries, CTEs

- Windowed joins: `TUMBLE`/`HOP` on both sides with the same window, joined on `l.window_start = r.window_start`
  (inner, left, right, full). The condition is split with Calcite's `JoinInfo`: with at least one `leftField = rightField`
  key it is a hash join on the keys, the rest becomes `residual` (evaluated on every key match, also for outer joins);
  without keys it is a nested-loop join. They arrive as `extension_multi` with a `nes.calcite.WindowJoin`.
- A join side may also be a tumbling windowed aggregation (joined on its `window_start`).
- `IN`/`EXISTS` subqueries, `NOT IN`/`NOT EXISTS` and `LATERAL` are decorrelated by Calcite. Correlated on the window
  (`r.window_start = l.window_start`), they become a windowed join with a per-window deduplication of the subquery
  (`IN`/`EXISTS`) or a left join plus `IS NULL` (`NOT IN`/`NOT EXISTS`, non-null keys).
- CTEs are inlined at every use.

## NebulaStream side

Every query is planned by Calcite: NebulaStream has no optimizer, lowering or pipelining of its own anymore. The
frontend (REPL, CLI, systest) calls the service at `$NES_CALCITE_PLANNER` (default `localhost:50061`) and gets the placed
pipelines back (see "Physical plans" below).

- A query is written in Calcite's dialect. Our SQL grammar only recognizes it (a query starts with `SELECT`, `WITH`,
  `VALUES`, `TABLE` or `(`) and keeps its text; only the sink clause after `INTO` is ours (a sink name or an anonymous
  sink such as `File(...)`). In the REPL, the prefix `CALCITE` is optional.
- `EXPLAIN [FORMAT TEXT | VERBOSE] <query>` shows the placed pipelines per worker; `VERBOSE` shows the planner's steps
  first.
- `nes-calcite/` (C++) resolves each worker's part of the plan into a self-contained `WorkerPart`: the part's Substrait
  relation, with the source and sink descriptors and the models it reads from the coordinator's catalogs. A worker
  (embedded, or remote with the gRPC request `StartPhysicalQuery`) builds its pipelines from the part alone
  (`nes-calcite-pipelines`) and compiles them.
- Systest plans every test file this way: the queries are written against the sources the file declares.

`nes-calcite/e2e/slt_run.py` runs a sqllogictest file through the REPL and compares every result with SQLite.

## Native library

With GraalVM, the build compiles the planner with `native-image --shared` into `libnes-calcite-planner.so` and links it
into `nes-calcite`. The planner then runs in-process: no JVM, no gRPC, no service to start. The development image has
GraalVM and Maven and sets `GRAALVM_HOME`, so CI builds and the executable images (`package-docker-images-all`, which
copies the library to `/usr/lib`) always use the native library. Elsewhere:

    GRAALVM_HOME=<graalvm> cmake --preset ...    # or -DNES_CALCITE_GRAALVM_HOME=<graalvm>; mvn must be on the PATH

The build makes the jar and the library (about 3 min and 9 GB RAM; native-image has no incremental mode) whenever the
planner's sources change. `CalcitePlanner.cpp` creates one GraalVM isolate per process and calls `nes_calcite_plan`
(serialized `PlanRequest` in, `PlanResponse` out, see `NativeEntryPoints.java`); errors arrive in `PlanResponse.error`.
Without GraalVM, NebulaStream calls the planner service instead (see above).

Two parts of Calcite compile generated Java code at runtime (Janino), which a native image cannot do. The planner
therefore uses `CachingMetadataHandlerProvider` for metadata (Calcite's `ProxyingMetadataHandlerProvider` plus the
per-query cache of the generated handlers; without the cache, reordering a 60-table join took 40 s instead of 5 s) and
`InterpretingRexExecutor` (Calcite's `RexInterpreter`) for constant folding, on the JVM as well; both produce
byte-identical plans to Calcite's defaults on the recorded requests. What the image may reflect on is listed under `src/main/resources/META-INF/native-image/`:
- `nes/nes-calcite-planner/`: recorded by the tracing agent while planning those requests:

      java -Dnes.calcite.dumpRequests=DIR -jar target/nes-calcite-planner.jar   # then run queries against it
      $GRAALVM/bin/java -agentlib:native-image-agent=config-output-dir=CONFIG -cp target/nes-calcite-planner.jar \
          nes.calcite.NativePlanner DIR/*.pb

  `NativePlanner <request.pb>...` prints the plan or error per request; run it on the JVM and as a native executable
  (`native-image -cp target/nes-calcite-planner.jar nes.calcite.NativePlanner`) to compare the two.
- `nes/calcite-metadata-handlers/`, `nes/protobuf-messages/`, `nes/calcite-reflective/` and `nes/nes-resources/`: whole
  families of classes and our resource files,
  written by `native/registrations.py`, because which of them a query reaches depends on its operators and size, and
  tracing missed some.

## Anonymous sources

A query can read a source that is not in the catalog with a table function per source type, as DuckDB's `read_csv`:

```sql
SELECT id, val
FROM TABLE(FILE(
    schema => 'id UINT64 NOT NULL, val INT32',
    file_path => 'data.csv',
    format => 'CSV',
    format_field_delimiter => ';'))
```

- `schema` (required): the fields as in `CREATE LOGICAL SOURCE` (`name TYPE [NOT NULL]`, quoted names keep their case).
- `host`: the worker that runs the source. `format`: the input formatter type; `format_<option>`: its options.
- Every option of the source type under its name (e.g. `file_path` for `FILE`, `generator_schema` for `GENERATOR`).

The request lists the registered source types and their options (`PlanRequest.source_types`, `format_options`), so
Calcite checks the argument names; the engine checks the values. `AnonymousSources.java` turns the call into an
`AnonymousSource` payload, which becomes our anonymous source operator. Not yet in physical plans.

## Unsigned integers

`UINT8`..`UINT64` are Calcite's `UTINYINT`..`UBIGINT` and, in Substrait, the user-defined types `u8`..`u64` of
`extension:io.substrait:unsigned_integers`, which declares their arithmetic and sum/min/max; `nes_unsigned_integers.yaml`
adds comparisons and modulus (see `Unsigned.java`). What a mix of signed and unsigned values means differs between
systems, so it fails: an operation takes unsigned operands of one type (a narrower unsigned operand is widened, a
non-negative integer literal is cast to the unsigned type), and a cast to or from an unsigned type must keep every value
(e.g. `UINT32` to `BIGINT`). Like all our integer arithmetic, unsigned arithmetic wraps on overflow.

## Physical plans

With `PlanRequest.physical` (which the frontend always sets), Calcite plans down to our pipelines, and the engine
neither optimizes, lowers nor pipelines the query: it only compiles the pipelines.

- `NesPhysical.java`: one Calcite node per engine operator, in the convention `NES`: `SourceScan`, `Scan`, `Selection`,
  `Map`, `Emit`, `Sink`. The field names of each node's row type are the engine's physical field names.
  - A `Map` computes one value into one field, which it appends to the row.
  - A pipeline starts at a `Scan` (or is a `SourceScan` or the `Sink`) and ends at an `Emit`. Only an `Emit` drops or
    reorders fields: it writes the given fields in the given order.
  - A `Scan` after a `SourceScan` parses the source's raw data. An `Emit` followed by a `Scan` (a buffer written and read
    again) is only for a result that several pipelines share; there is none yet.
  - Two helpers are not engine operators: `Retain` (a projection: keep these fields) and `UnionAll` (UNION ALL, or a
    logical source with several physical sources).
  - Converter rules turn Calcite's logical operators into these; Calcite's `VolcanoPlanner` chooses among them. A
    projection becomes a `Map` per computed value and a `Retain`.
- `Pipelines.finish` resolves the helpers: the operators above a `UnionAll` are copied into each of its inputs, up to the
  pipeline's `Emit`, so every input is a pipeline of its own that writes into the next one (e.g. the sink); a `Retain`
  becomes part of that `Emit`, and the field references in between are renumbered.
- `NesConverterProvider` writes each node as a Substrait extension relation with a `PhysicalOperator` payload: its output
  schema, its pipeline, and what the engine needs to build it (e.g. a source's origin id).
- `nes-calcite/src/SubstraitToPhysicalPlan.cpp` builds the pipelines from the payloads, with the worker's buffer size;
  `QueryCompiler::compileQuery(PhysicalQuery)` compiles them.
- `docs/physical-plans.html` shows example queries with their logical plan, physical plan and pipelines.
- `docs/planner-steps.html` shows every planner step as a graph, and the plan after each rule of the rule-based steps
  (`PlanRequest.detailed_steps`, see `Steps` and `PlanGraph`). Regenerate it with `docs/tools/generate_steps.sh <java>`
  after `mvn package`. Its queries are also a systest, `nes-systests/calcite/PlannerExamples.test`.
- `docs/tools/generate_systest_steps.sh <systest binary> <out directory> [java]` shows the same for every systest query:
  it runs the systests with `--plan-only` (each query is planned, none runs) and `NES_CALCITE_DUMP_REQUESTS` (each request
  to the planner is saved under its query id), plans
  the saved requests again with detailed steps, and writes one page per test file and an index. The planner service must
  run.

### Windows

A windowed aggregation (TUMBLE or HOP over event time) becomes three operators:

- `WatermarkAssigner`: `Pipelines.finish` puts it directly after the pipeline's `Scan`, below Selections and Maps,
  unless a Map computes the time field.
- `WindowBuild`: ends the pipeline and adds each row to the window's state.
- `WindowProbe`: starts the next pipeline and emits a row per closed window and key.

The builds and the probe of one window share its state (`WindowState`, in the engine one operator handler). Under a
union, each branch has its own build, and the probe reads all of them. The probe produces the engine's result types
(an unsigned COUNT, a 64-bit SUM, a DOUBLE AVG). A Map per result casts it to Calcite's type.

Not yet: GLOBAL windows (finite inputs) and joins.

### Placement

With `PlanRequest.topology` (the REPL sends its worker catalog), the plan is placed before it becomes physical
operators, as our own optimizer places logical operators:

1. `NesLogical.expandSources`, the first step after the conversion from SQL: each logical source becomes a scan of each
   physical source (a UNION ALL of several). All later steps see the physical sources; filters and projections go below
   these unions, so that each source's rows shrink where it runs.
   `Watermarks.assign` then puts the watermark assigner of each window input directly after its sources, so that it is
   the first operator after the source.
2. `Placement.java` puts each logical operator on one worker with an integer linear program (ojAlgo, pure Java):
   - `x[o][w] = 1` if operator `o` runs on worker `w`. Each operator runs on exactly one worker.
   - A physical source runs on its worker, and the sink on the sink's worker. A watermark assigner runs on the worker of
     its source.
   - A worker runs at most `max_operators` operators. Sources, their watermark assigners and the sink cost no capacity.
   - An operator sends its rows only to its own worker or to a worker downstream of it.
   - Objective: each cut (an operator whose consumer runs on another worker) costs a fixed amount plus the bytes it
     sends, as Calcite estimates them, times the hops between the workers.
3. The plan is cut into one part per worker and cut. The rows go over the network along the shortest path. A worker in
   between only forwards them (`NetworkSource → NetworkSink`).
4. All parts become physical operators and pipelines together (one Volcano run over `NesLogical.Parts`). A part ends in
   the `Sink` or in a `NetworkSink`. On the receiving worker, a `Scan` reads the `NetworkSource`.

Operators that the engine builds together are one logical operator during placement: a window (build and probe) or a
source (and its parser). Placement therefore needs no rules for them. A pipeline only ends at a cut where the solver
chose one.

Rows keep their origins, sequence numbers and watermarks over the network. A `NetworkSource` has the origins of its
channel's `NetworkSink`, so a window behind a cut waits for the watermarks of the original sources.

A placed plan has one Substrait root per part. Each operator carries its worker's host. Pipeline ids and origin ids are
unique in the full plan, because a worker builds all its parts as one plan (`toPipelines(plan, context, worker, ...)`).
`QueryManager::start(PlacedPhysicalQuery)` starts the part of each worker.

A network channel needs the data address of each worker on it. The REPL's own worker has none unless `--data_address`
sets it, and that flag applies to every embedded worker. A demo therefore uses workers from `CREATE WORKER ... AS DATA`.

Not yet: joins, operator costs (CPU) for the placement, and statistics for better byte estimates.
