# Connection Test (ConnTest)
**ConnTest** (*Conn*ection Test), is an end-to-end framework for testing source and sink connectors against *real*, external services in isolation. Instead of mocking a broker or a database, ConnTest brings the real thing up in Docker (Mosquitto, Postgres, …), drives the actual connector implementation against it, and validates the round-trip — so a passing test means the connector works with the real protocol, not a stand-in.

ConnTest is a **CLI**. A single entry point, `conntest`, runs on the host, drives `docker compose`, and runs the test battery inside a per-plugin container:

```bash
./nes-conn-test/runner/conntest run                                 # full battery (every connector)
./nes-conn-test/runner/conntest run round_trip                      # one scenario, every connector
./nes-conn-test/runner/conntest run --plugin MQTTSource             # one connector, every scenario
./nes-conn-test/runner/conntest run reconnect --plugin MQTTSource   # one scenario, one connector
./nes-conn-test/runner/conntest run --build                         # build conntest-harness first, then run
./nes-conn-test/runner/conntest run round_trip -- -s -v             # forward flags to pytest (after `--`)
./nes-conn-test/runner/conntest list                                # discovered connectors × scenarios
./nes-conn-test/runner/conntest --help
```

Adding a connector to the battery needs **zero edits to `nes-conn-test/`**: drop a `conn-test/` directory into the plugin (`loader.py`, `compose.yaml`, a `configs/{valid,invalid}/*.nesql` tree, and a `scenarios.py` binding), compile the connector into the harness, and the framework discovers and parametrizes it automatically. The [Usage](#usage) section below adds the selector semantics, the remaining subcommands, and the CLion integration; the rest of this document explains how the pieces fit together.

## Core design principles

ConnTest is built around a handful of load-bearing **design principles**. They are not mechanically enforced — they are enforced by review and by the battery itself — but nearly every decision below follows from one. Each is stated with the anti-pattern it blocks in `nes-conn-test/core_design_principles.md`; in brief:

- **generator-is-truth** — one `Generator(schema, seed)` is the single source of all test data; it adapts to the schema parsed from the config under test, and its rows are *both* the input given to the connector *and* the read-back oracle.
- **plugin-isolation** — a connector's entire test surface lives in its plugin directory; the framework stays connector-agnostic, needing no edits to add one.
- **engine-fidelity** — ConnTest exercises NebulaStream *as it ships*; it composes around the engine (wrap / decorate / drive) and never changes engine code to suit the test.
- **config-fidelity** — the framework rewrites only the dynamic keys it owns (the endpoint, the per-test resource name); every other authored config value passes through verbatim.
- **battery-is-ground-truth** — make the code satisfy the battery, never the reverse; never weaken, skip, `xfail`, or shrink a test to turn it green.
- **compose-topology** — service location, port, and credentials come from compose DNS on the project network, never host networking or published ports — what makes the setup portable across architectures and CI.
- **deterministic** — the run is reproducible in two dimensions: replayable data (a fixed seed) and synchronized control (handshakes, never sleeps; a firing timeout is always a failure, never synchronization).
- **externally-bounded** — a streaming read is bounded by the framework's own byte quota, not an end-of-stream; the generated stream carries data only, never an injected sentinel.

Two terms recur throughout, and it is worth fixing them now:

- **the runner** — the Python (pytest-based) orchestrator under `nes-conn-test/runner/`. It generates the data, drives the connector, injects faults, and validates the result.
- **the harness** — the C++ binary `conntest-harness` under `nes-conn-test/driver/`. It links the NES engine, loads the connector through the plugin registry, and runs it under the runner's control.

## Usage

`conntest` is the single entry point. It runs on the host (or a CI VM), drives `docker compose`, and runs the battery via pytest *inside* a per-plugin `runner` container that compose creates already on the service network — so the harness reaches the data service by DNS (`broker:1883`, `db:5432`) with no host networking. If the host has no `docker compose`, `conntest` transparently self-wraps once into a dev container and proceeds identically, so local and CI share one code path.

### From the command line

The first positional argument is a scenario **short name** (`round_trip`, `empty`, `config_valid`, `bad_endpoint`, `reconnect`, …); anything it does not recognize is forwarded to pytest as a raw `-k` expression. `--plugin <Name>` brings up *only* that connector's infrastructure and runs only its tests — unlike a bare `-k Name` filter, which still cycles every other connector's docker stack up and down only to deselect it.

Other subcommands: `ci` (exactly what CI runs — `run` with every selector forced off), `coverage` (the instrumented battery + coverage wiring), `debug` (run one test under `lldb-server` for an attaching CLion), and `install-clion` ((re)writes the CLion run configs).

### From CLion

Two run configurations ship under `.idea/runConfigurations/` (regenerated by `conntest install-clion`):

| Run config | Action |
|---|---|
| **ConnTest** | Click ▶. Runs `conntest run --build`: builds `conntest-harness`, then runs the full battery via pytest. To scope it, edit (or duplicate) the config's inline command and append a scenario short name and/or `--plugin <Name>` — e.g. `conntest run round_trip --plugin MQTTSource`. |
| **ConnTest Debug** | Click 🐞. Runs one scenario for one connector under `lldb-server` and attaches CLion as an LLDB client, so breakpoints fire in the connector and harness C++. By default it debugs `MQTTSource`'s `round_trip_10_buffers`; to debug something else, edit the sibling **ConnTest Debug Launcher** config's command — it takes the regular `conntest debug` arguments, e.g. `conntest debug round_trip_10_buffers --detach --plugin MQTTSink`. |

> **Debug prerequisite.** Set `HOST_NEBULASTREAM_ROOT=<host repo path>` as a Docker Toolchain environment argument. CLion configures CMake *inside* the dev container, so the harness debug info is written with the container's bind-mount path; `-fdebug-prefix-map` rewrites those paths back to the host repo path, but only when `HOST_NEBULASTREAM_ROOT` is set. Without it, host-path breakpoints never bind and `conntest debug` refuses to start. Breakpoints work in `nes-conn-test/driver/*.cpp` (the harness), `nes-plugins/{Sources,Sinks}/<Plugin>/*.cpp` (the connector under debug), and any transitive engine code.

## Python integration
- **The quality gate is part of the existing `format` job.** `nes-conn-test/runner/check.sh` runs three tools — `ruff check` (lint, `select = ALL` minus a justified ignore list), `ruff format` (the clang-format equivalent), and `mypy --strict` — and `scripts/format.sh` invokes it, so the CI format check gates the Python with **no new workflow**. Like the clang checks it is scoped to changed files and silent on success; `-i` applies the auto-fixes.
- **Dependencies are plugin-isolated and locked.** Each connector declares its loader's pip dependencies in its own `conn-test/pyproject.toml`; a uv workspace at the repo root assembles the union, and a single committed `uv.lock` is the version source of truth (`uv sync --frozen`). Adding a connector's client library touches only that connector's directory — the same `plugin-isolation` story as the rest of its test surface.
- **The connector seam is statically checked.** Loaders inherit the `SourceLoader` / `SinkLoader` ABCs, so a missing method fails construction and a wrong override signature fails `mypy --strict` at gate time — before a battery ever runs.
- Per-OS virtualenvs (`.venv-linux` / `.venv-darwin`) let the host gate and the dev-container/CI gate share one repo checkout without tearing the environment down on every switch; the lockfile keeps them identical in content.

## Architecture

ConnTest has four moving parts:

1. **The runner** (Python / pytest) orchestrates the test: it discovers connectors, brings up their docker stacks, generates data, drives the harness, injects faults, and validates.
2. **The harness** (C++ / `conntest-harness`) links the NES engine, loads the connector through the plugin registry, and runs it as either a *source* or a *sink* under the runner's step-by-step control.
3. **The external service** (Mosquitto, Postgres, …) — the real system the connector talks to, brought up in Docker per connector.
4. **Toxiproxy** — an optional fault sidecar placed on the connector ↔ service link, which lets the runner cut, restore, or degrade *only* the connector's link without touching its own seed/read-back path.

```
   ┌────────────┐   control socket    ┌──────────────────┐                  ┌─────────────────┐
   │   runner   │◀── cmd / reply ────▶│   C++ harness    │◀─── TCP data ───▶│    toxiproxy    │
   │  (pytest)  │                     │ (the connector)  │    (both ways)   │  listen:<port>  │
   └─────┬──────┘                     └──────────────────┘  cfg → proxy     └────────┬────────┘
         │ ↕ seed / read_back                                          upstream ↕    │
         │   (direct, bypasses the proxy)                            (both ways)     │
         └──────────────────────────┐                          ┌───────────────────┘
                                 ┌──┴──────────────────────────┴──┐
                                 │  external service (postgres,..) │
                                 └─────────────────────────────────┘

   cut / restore + toxics are applied at toxiproxy → they hit only the connector's link.
```

The runner drives the harness over a command/reply **control socket** (a unix-domain socket on a shared filesystem path — no network). The harness reaches the service for its data, while the runner seeds and reads the service back *directly*. When a scenario needs a fault, toxiproxy sits between the harness and the service; because the runner's seed/read-back path bypasses the proxy, a fault degrades only the connector's link and the read-back oracle stays trustworthy while the fault is in flight.

### Toxiproxy

[Toxiproxy](https://github.com/Shopify/toxiproxy) is Shopify's TCP fault-injection proxy. ConnTest merges its sidecar (`runner/toxiproxy.compose.yaml`) into a connector's stack only when a scenario needs it, and `link.py` controls it over its HTTP API. The simplest control is the on/off `cut` / `restore` (toggling the proxy's `enabled` flag) for a clean, deterministic outage. Beyond that, toxiproxy can attach **toxics** — partial degradations short of a full cut, each with a direction and a probability — that model the failure modes a real network exhibits: lost connections, high latency, low throughput, or stream batching:

| Toxic | Effect | Key attribute |
|---|---|---|
| `latency` | delay every chunk | `latency` ± `jitter` (ms) |
| `bandwidth` | cap throughput | `rate` (KB/s) |
| `timeout` | stop data, then close after the timeout (`0` = stall forever; caveat: *removal* severs the connection) | `timeout` (ms) |
| `reset_peer` | hard TCP RST after a delay | `timeout` (ms) |
| `slow_close` | delay the TCP close (FIN) | `delay` (ms) |
| `slicer` | fragment the stream into small slices | `average_size`, `delay` (µs) |
| `limit_data` | forward N bytes, then close | `bytes` |

Today the framework wires `cut` / `restore` and the bidirectional `latency` toxic twice over: `throttle` (used by `slow_link`) and `silence` / `unsilence` (used by `silent_link` — a delay far above the scenario's hold stalls all bytes while keeping the connection established). The `timeout` toxic looks like the natural fit for silence but is a trap: *removing* it severs the stalled connection, turning the fault back into a cut — see `Link.silence`. The remaining toxics are listed here and in `link.py` so a future degraded-link scenario can reach for one without rediscovering the API.

## Scenarios

The core abstraction of ConnTest is the **scenario**. A scenario is a single connector- and kind-blind function that exercises one interaction between a connector and its external service — a happy-path round trip, a config-validation pass, a connection loss, a process crash. Scenarios live in one shared **catalog** (`nes-conn-test/runner/src/conntest_runner/scenarios.py`); a connector does not write them. It *opts in* to a scenario through its `conn-test/scenarios.py` and supplies the binding (which config(s), which `setup_file`, the declared outcome). The conformance battery then parametrizes every catalog scenario over every connector that bound it.

The current catalog:

| Scenario | Docker | What it proves |
|---|---|---|
| `round_trip_1_buffer` / `_10_buffers` / `_100_buffers` | ✅ | The full seed → read → verify round trip at a fixed buffer tier (the 0→1 boundary, the default functional tier, the sustained-churn tier). |
| `round_trip_n_buffers` | ✅ | A round trip at a connector-chosen buffer count (`n_buffers=N`). |
| `empty` | ✅ | Lifecycle only: open → close with nothing seeded — both must succeed. |
| `config_valid` | ❌ | Every matched config binds cleanly. |
| `config_invalid` | ❌ | Every matched config is rejected with a declared error code. |
| `bad_endpoint` | ✅ | Opening against a dead endpoint raises the declared error. |
| `reconnect` | ✅ + link | A warm link cut + restore between two batches — does the connector recover? |
| `outage` | ✅ + link | A batch in flight *while* the link is cut — does it survive or is it lost? |
| `slow_link` | ✅ + link | A degraded-but-alive link (bidirectional latency) — slow ≠ broken. |
| `silent_link` | ✅ + link | A multi-second silence on a live connection the connector must bridge. |
| `crash_recovery` | ✅ | SIGKILL the harness mid-stream; a fresh process must re-open and resume. |

The four scenarios marked `+ link` route the connector through toxiproxy (`needs_link`); `config_valid` / `config_invalid` are docker-free (they only bind a config and spin up no service). Each scenario is justified by what *only it* proves; the per-scenario sections below spell that out.

### Scenario definitions

A scenario is written in terms of a small set of kind-blind **steps** on a per-case context, `Ctx`. The same step name does the right thing for a source or a sink, so the scenario body never branches on the connector kind (beyond `ctx.is_source`):

| Step | Source | Sink |
|---|---|---|
| `ctx.setup()` | run the loader's pre-connect backend hook (create table, open a subscriber) | same |
| `ctx.connect()` | `OPEN` the source | `START` the sink (reports how many buffers it materialized) |
| `ctx.transfer()` | seed the dataset, fill its quota, compare the observed stream | write every buffer (read-back compare deferred to `finish`) |
| `ctx.disconnect()` | `CLOSE` the source | `STOP` the sink |
| `ctx.finish()` | no-op (the source compared in `transfer`) | read the sink back and multiset-compare against the oracle |
| `ctx.bind()` | `BIND` / validate the config on whichever side this case drives | |
| `ctx.kill()` | SIGKILL the harness process (ungraceful death; next `connect()` respawns it) | |
| `ctx.source_batches(k)` / `ctx.seed_and_fill(rows)` | split the dataset into `k` batches; seed+fill+compare one batch | |
| `ctx.write_buffers(n)` / `ctx.write_rest()` | | drain the next `n` buffers / all remaining buffers |
| `ctx.link.cut()` / `.restore()` / `.throttle(latency_ms=…)` / `.silence()` / `.unsilence()` | sever, degrade, or stall the toxiproxy link (`needs_link` scenarios) | |

Each step is one request/reply handshake with the harness (or one acked side effect against the service): a SUBACK, a publish-ack, a toxiproxy HTTP reply, a quota satisfied. There are no sleeps in the control path. Three layered timeouts exist purely as stall-breakers — a **per-step harness watchdog** (`--step-timeout-ms`, default 30 s, which a scenario may raise via `step_timeout_ms=`), a **global framework deadline** (default 120 s), and bounded loader waits — and a firing timeout is always a failure, never a normal transition.

Because the steps are kind-blind, most scenarios read as a short, declarative sequence. The base round-trip body, shared verbatim by every `round_trip_*` tier, is the canonical example:

```python
def _round_trip(ctx: Ctx) -> None:
    ctx.setup()
    ctx.connect()
    ctx.transfer()
    ctx.disconnect()
    ctx.finish()
```

The catalog function writes only the *ideal* path; a generic `run_case` turns the binding's declared `Expectation` into the pass/fail decision: an `OK` binding must complete cleanly, an `ERROR <code>` binding must raise a connector error with that code, and a harness-origin error (infra failure or step timeout) is never an expected outcome and always fails.

### Config scenarios

The most basic scenarios are `config_valid` and `config_invalid`. They are **docker-free**: they only `BIND` the pristine config through NES's query parser and binder and spin up no live service, so they take no `setup_file`. Both bodies are a single `ctx.bind()`; only the declared outcome (enforced by `run_case`) differs:

```python
def config_valid(ctx: Ctx) -> None:
    ctx.bind()


def config_invalid(ctx: Ctx) -> None:
    ctx.bind()
```

`config_valid` takes a glob of configs that must all bind cleanly:

```python
config_valid(path="valid/*.nesql"),
```

The runner collects every config matched by the glob, binds each, and the scenario passes only if all bind without error.

`config_invalid` works the same way but pairs the glob with the error every matched config's rejection must carry. The runner resolves error codes against `ExceptionDefinitions.inc`, so a typo or a retired code fails at scenario-parse time, not silently at run time. Bind one `config_invalid` per distinct code — a glob fans out to many files that share it:

```python
config_invalid(path="invalid/missing-topic.nesql", error=1000),
config_invalid(path="invalid/other_errors/*.nesql", error=1001),
```

The error may be given as a numeric code or by its symbolic name, interchangeably:

```python
config_invalid(path="invalid/missing-topic.nesql", error="InvalidConfigParameter"),
```

The framework expects this file layout in a connector that supports ConnTest (taken from `MQTTSource`):

```
nes-plugins/Sources/MQTTSource/conn-test/
├── loader.py          # the connector's backend IO (the only code a connector adds)
├── scenarios.py       # the scenario bindings (config globs + declared outcomes)
├── compose.yaml       # the data service(s) — broker / db (+ healthcheck)
├── pyproject.toml     # the loader's pip dependencies
├── mosquitto.conf     # one or more setup_files mounted into the service
└── configs/
    ├── valid/         # pristine, authored configs the live scenarios reuse
    │   ├── basic.nesql
    │   ├── multicol.nesql
    │   └── persistent.nesql
    └── invalid/       # configs whose binding must be rejected
        └── missing-topic.nesql
```

The live scenarios reuse the same `valid/*.nesql` files the config scenarios bind; there is no separate `.template` and no dedicated `bad_endpoint.sql`. A scenario overrides only the dynamic keys it owns (the endpoint and target) via the loader's `config_overrides`, leaving every other authored value verbatim (`config-fidelity`).

### Loader — config rewriting

Every connector defines a **loader** — a class that inherits `SourceLoader` or `SinkLoader` from `contracts.py`. The base class is the single source of truth for what a loader is: construction fails (`TypeError`) if an `@abstractmethod` is left unimplemented, and mypy checks the override signatures at class-definition time, so a wrong `seed_batch` / `read_back` signature fails the per-connector type gate rather than a battery run. The common surface is two methods:

```python
def config_overrides(self, *, target: str, endpoint: str) -> dict[str, str]
def setup(self, *, target: str, schema: Any) -> None  # prepare the backend, e.g. create a table
```

`config_overrides` receives the safe-to-use `endpoint` (the compose service's DNS `host:port`) and the per-test `target` (a resource name the framework generated and made unique per scenario). The loader maps them onto whatever config keys its connector uses for networking and resource selection:

```python
# loader.py of MQTTSource — a topic-addressed broker:
return {"server_uri": f"tcp://{endpoint}", "topic": target, "client_id": target}

# loader.py of ODBCSource — a host/port DSN:
host, port = split_endpoint(endpoint)
return {"db_host": host, "db_port": str(port)}
```

This is what lets the live scenarios reuse pristine configs: `config_overrides` swaps in just the keys a test run must control, and everything else passes through.

### Round trip

The round-trip scenarios are the functional core: seed the connector with deterministic data and verify it comes back unchanged. The shape is exactly the `_round_trip` body above; the tiers differ only in how many buffers of data they drive (`round_trip_1_buffer`, `_10_buffers`, `_100_buffers`, or a configurable `_n_buffers`), so the data path never branches on the count. A binding names the config(s) and the `setup_file` to mount:

```python
round_trip_1_buffer("valid/basic.nesql", setup_file="mosquitto.conf"),
round_trip_10_buffers("valid/basic.nesql", "valid/multicol.nesql", setup_file="mosquitto.conf"),
```

A tier may bind more than one config (the 10-buffer tier above also runs the multi-column schema). Unlike the config scenarios, a round trip needs a live service — which is where Docker comes in.

### Docker

The `setup_file` a binding names is mounted into the connector's docker-compose stack, replacing a placeholder in the plugin's `compose.yaml`:

```yaml
services:
  broker:
    image: eclipse-mosquitto:2.0
    expose: ["1883"]  # the framework derives the connector endpoint from this
    command: ["/usr/sbin/mosquitto", "-c", "/mosquitto/config/mosquitto.conf"]
    volumes:
      # the placeholder ConnTest fills from the binding's setup_file:
      - ${HOST_NEBULASTREAM_ROOT}/nes-plugins/Sources/MQTTSource/conn-test/${CONNTEST_SETUP_FILE:?CONNTEST_SETUP_FILE required}:/mosquitto/config/mosquitto.conf:ro
    healthcheck:
      test: ["CMD", "mosquitto_pub", "-h", "localhost", "-t", "healthcheck", "-m", "test", "-q", "1"]
      interval: 5s
      timeout: 3s
      retries: 5
      start_period: 5s
      start_interval: 200ms
```

The one rule the docker setup follows is: **never ask "which container am I" — let compose *create* the test container already on the service network.** ConnTest merges the plugin's `compose.yaml` (its data service only) with the framework's `runner.compose.yaml` (the single generic `runner` service) — and, for a `needs_link` scenario, `toxiproxy.compose.yaml` — into one throwaway project per connector, named `nebulastream-conntest-<uuid>`. Because compose places the runner on the project network, the harness reaches the service by its compose DNS name (`broker:1883`, `db:5432`) with no published ports, no host networking, and no container-IP guessing — the design's portability across host and CI follows directly. Distinct `setup_file`s run as separate stacks; each connector's project is brought up and `down -v`'d independently, so there is no cross-connector state.

```
   host / CI runner VM / self-wrap dev container
   ┌────────────────────────────┐
   │  conntest  (Python CLI)     │   drives `docker compose` over the socket
   └─────────────┬──────────────┘
                 │
                 │   docker compose -f <plugin>/conn-test/compose.yaml \
                 │                  -f nes-conn-test/runner/runner.compose.yaml \
                 │                  -p nebulastream-conntest-<uuid>
                 ▼
   ┌──────────────────  project network: <project>_default  ──────────────────┐
   │                                                                          │
   │   ┌─────────────────┐                          ┌───────────────────────┐ │
   │   │  DATA SERVICE   │                          │  RUNNER               │ │
   │   │  broker:1883    │ ◀──── resolves by ────── │  (dev image)          │ │
   │   │   or  db:5432   │       compose DNS        │                       │ │
   │   │                 │                          │  pytest               │ │
   │   │  plugin-owned   │                          │   └─ Popen ─▶ harness │ │
   │   └─────────────────┘                          │  (unix-socket stepper)│ │
   │                                                │  framework-owned      │ │
   │                                                └───────────────────────┘ │
   └──────────────────────────────────────────────────────────────────────────┘
```

### Loader — setup

Once the docker stack is healthy, the runner calls the loader's `setup` hook (through `ctx.setup()`) before the connector opens. `setup` is the pre-connect backend hook: it creates the per-test resource and/or opens a persistent reader. The framework passes it the schema it parsed from the config, so the resource is derived from that schema rather than hardcoded:

```python
# ODBCSource — loader: (re)create the per-test table from the parsed schema
def setup(self, *, target: str, schema: list[NativeColumn]) -> None:
    self._schema = schema
    _execute(self._dsn(), f'DROP TABLE IF EXISTS "{target}"')
    _execute(self._dsn(), self._create_sql(target, schema))
```

`setup` is part of the contract, not optional — a byte source like MQTT, whose topic needs no pre-creation, implements it empty (caching the schema for later) so the framework can call it uniformly with no capability probe.

### The data generator

> **generator-is-truth** — one `Generator(schema, seed)`, adapting to the schema parsed from the config under test, is the single source of all test data.

The data generator is what makes ConnTest schema-driven. From a connector's parsed schema and a fixed seed, it deterministically derives the exact rows that fill the N buffers a scenario asks for (`rows_per_buffer = buffer_size // row_width`). Those rows are *both* the input the connector is fed and the oracle the result is checked against, so a connector author never has to author test data, keep fixture files in sync with configs, or maintain a second schema definition — a new valid config just works.

#### Data formats

The generator is implemented in Python and the **runner speaks only JSONL** (JSON Lines) on the wire — it never sees the engine's native tuple format. JSONL is a popular, human-readable format that natively represents null values, missing values, nested types, arrays, and complex layouts, so it is future-proof and — crucially — produces straightforward text dumps that make a mismatch easy to read while debugging. The runner exchanges JSONL with the harness through two files: a sink's input on `--input-path`, and a source's observed output on `--observed-path`.

> **engine-fidelity** — JSONL ↔ native reuses the engine's own formatters; the framework writes no codec of its own.

The conversion between JSONL and native tuples is done entirely on the C++ side by a `Formatter` class — a lean wrapper that assembles the engine's *own* JSON input and output formatters into interpreted scan→emit pipelines. The win is twofold: the framework reimplements no JSON formatting, and the test data stays in lockstep with how the engine actually parses and emits JSON. The cost is pulling several heavy engine libraries into the harness; the reduced surface for codec drift makes it worth it. (The `Formatter` also canonicalizes JSON keys — stripping the `src$` qualifier and lowercasing — so they line up with the runner whatever the SQL binder folded the identifiers to.)

The **loader**, by contrast, never touches JSONL framing of its own; it deals in **typed rows** on both ends, and the framework owns the wire encoding:

- A **JSON-wire source** (MQTT) serializes the typed rows to JSONL with the framework's `rows_to_jsonl` and publishes the bytes; the engine's JSON input formatter parses them back. A **native source** (ODBC) simply `INSERT`s the typed rows into its database — no wire encoding, since the engine reads typed tuples directly.
- A **JSON-wire sink** (MQTT) collects the records the sink published and parses them back into typed rows with `jsonl_to_rows`. A **native sink** (ODBC) `SELECT`s the rows it wrote. Either way the loader returns typed rows that the framework multiset-compares against the generated oracle.

### Connect

`connect()` opens the connection. For a **source**, it `OPEN`s the connector. For a **sink**, `START` also materializes the run's input into tuple buffers and reports how many buffers it produced, so the scenario can later step whole buffers.

### Protocol

The runner and the harness communicate over a command/reply session on the unix-domain control socket. The wire model lives in `protocol.py` (the reply types and the error-code table) and `harness.py` (the steppers that spawn and drive the harness). Each command is exactly one newline-terminated line and produces exactly one JSON reply line, until EOF.

The command verbs reflect the source/sink interfaces:

```python
# Source-mode stepper:  BIND | OPEN | FILL <n_rows> <n_bytes> | CLOSE
# Sink-mode   stepper:  BIND | START [<offset>] | WRITE <n_buffers> | STOP
```

A successful reply is `{"ok":true, …}` with a phase and any step-specific fields; a failure is `{"ok":false, …}` carrying its `origin`:

```json
{"ok":true,"phase":"fill","eos":false}
```
```json
{"ok":false,"phase":"open","origin":"connector","code":4002,
 "name":"CannotOpenSource","message":"…"}
```

`origin` is the load-bearing field. A `connector`-origin error is a real connector outcome a scenario may *expect* (`ConnectorError`); a `harness`-origin error is an infra failure or a step timeout that a scenario can *never* expect (`HarnessError`). Error codes resolve through `ExceptionDefinitions.inc` on both sides, so the Python table can never drift from the C++ enum.

Configs and JSONL data are exchanged as files; the harness learns their paths from its CLI flags (`--config`, `--input-path`, `--observed-path`) rather than over the socket.

### The C++ harness

The harness (`conntest-harness`) is a CLI binary the runner spawns with `subprocess.Popen`. It parses its arguments (`--mode source|sink`, `--config`, `--control-sock`, `--buffer-size`, `--step-timeout-ms`, and the mode-specific data paths), `connect()`s the control socket itself (rather than inheriting an fd, which keeps the `lldb-server` debug path working), and runs either a `SourceSession` or a `SinkSession`. The harness enforces a **per-step watchdog** (`--step-timeout-ms`, default 30 s) that a scenario may raise; on expiry the step returns a harness-origin error. The per-step bound is intentionally finer-grained than the runner's single, larger global deadline, so a stuck step is caught and attributed to its phase rather than tripping one coarse timer.

#### SourceSession

```python
# Source-mode stepper:  BIND | OPEN | FILL <n_rows> <n_bytes> | CLOSE
```

On **BIND**, the session binds the config (the one the framework rewrote with the injected endpoint/target) and returns the engine's exact physical **`row_width`** (the fixed region, including null flags) computed from the schema. The runner uses `row_width` to size "N buffers" worth of rows.

On **OPEN**, the session builds a fresh `SourceHandle` and starts its thread, but holds the source at a backpressure gate so it parks right after `open()`. This matters because the engine's `SourceHandle`/`SourceThread` surface *every* exception as a generic `RunningRoutineFailure` and offer no deterministic signal that `open()` itself succeeded. To recover both, the session wraps the connector in an **`InterceptorSource`**, a decorator that:

1. **Intercepts exceptions** and captures the connector's *real* error code (in a shared slot) before `SourceThread` flattens it to `RunningRoutineFailure` — so a connector author can test against connector-specific codes.
2. **Intercepts the `open()` call** and fulfills a promise the moment `open()` returns (or rethrows on failure) — so OPEN can report success or the real error deterministically, since a *successful* open otherwise emits no event.

`FILL` carries a **quota**, not raw data: the source pulls its data from the external service, so the runner sends the two measures of the requested batch — its row count *and* its JSONL byte length — and the harness compares against whichever matches how it counts (tuples for a native source, JSONL bytes for an opaque one). Before each `FILL`, the runner calls the loader's `seed_batch(rows, *, target)` to push that batch into the service, so the data lands on the source's already-wired subscription. The first `FILL` releases the backpressure gate (the "GO") and the source begins reading; the session drains buffers until the observed count reaches the quota (or the step watchdog fires, which is a failure). It then renders the observed buffers to JSONL via the `Formatter` (a no-op for an opaque source, which already carries JSONL) and writes them to the file named by `--observed-path`, which the runner reads back as its sole source-correctness oracle.

`CLOSE` closes the source. A subsequent `OPEN` builds a brand-new connector on the same cached descriptor, which is how the reconnect/crash scenarios model a fresh client taking over.

#### SinkSession

```python
# Sink-mode stepper:  BIND | START [<offset>] | WRITE <n_buffers> | STOP
```

**BIND** binds the descriptor and, like the source, reports the schema's `row_width`.

**START** materializes the run's input and starts the sink. The full run's JSONL lives in the file named by `--input-path`; START reads it into tuple buffers up front — decoding it into native row-layout buffers via the `Formatter` for a native sink, or slicing it into record-aligned buffers (bytes carried through unparsed) for a JSON-wire sink — then creates the sink and starts it. START's optional `<offset>` resumes at an already-committed buffer (used by crash recovery); it reports back the number of buffers it materialized (`n_buffers`), which the runner uses to step whole buffers.

```json
{"ok":true,"phase":"start","n_buffers":10}
```

On **WRITE `<n>`**, the session pops the next `n` buffers (from the START offset), loads them into a bounded MPMC queue, and drains them through the sink with a configurable pool of worker threads (`--threads`), each running the sink pipeline. It waits until the threads drain every buffer, an exception is thrown, or the step watchdog fires, then replies with the records actually written:

```json
{"ok":true,"phase":"write","units_written":120}
```
```json
{"ok":false,"phase":"write","origin":"connector","code":4004,
 "name":"CannotOpenSink","message":"…"}
```

After the sink has written, the runner has the loader's `read_back(*, target, schema, expect_records)` read the data back out of the external service — preferably via an official client — and return it as typed rows (a `SELECT` for a native sink, parsed records via `jsonl_to_rows` for a JSON-wire one). **STOP** calls the sink's `stop()` and waits for the threads to settle (or an exception / timeout).

### Validation

> **generator-is-truth** — the generated rows are both the input and the oracle.

Validation is one comparison, shared by both kinds: an **order-insensitive multiset compare of typed rows, keyed by schema type**. The runner canonicalizes each row field by its declared type — floats by their raw IEEE bytes (a FLOAT32 compares at single precision), NULL as a distinct token, integers coerced through `int` so a DB `Decimal` matches the oracle — then compares the two multisets. It is order- insensitive on purpose: a DB `SELECT` or a broker may reorder, and correctness must not depend on the clock or arrival order. On mismatch, the diff names the offending rows on each side.

For a **source**, the runner parses the observed JSONL back into typed rows and multiset-compares them against the generated batch. For a **sink**, it compares the loader's typed-row read-back against the same generated rows the sink was fed. Because the generator is the single source of both the input and the oracle, the two can never drift; and because the seed is a fixed constant, any failure replays deterministically from its pytest id (which carries the buffer count N, e.g. `round_trip_10_buffers-ODBCSource-basic-N10`), with `Dataset.dump()` printing the oracle rows as JSONL.

### Bad endpoint

`bad_endpoint` points the connector at an unreachable service and asserts that `OPEN`/`START` raises the declared connector error (`CannotOpenSource` / `CannotOpenSink`, or a connector-specific code). It reuses a `valid/*.nesql` config — the loader's `config_overrides` swaps in a dead endpoint — so no separate bad-config file is needed. It proves the connector surfaces a clean, attributable error on a connect failure rather than hanging or crashing. The body is the connect step alone:

```python
def bad_endpoint(ctx: Ctx) -> None:
    ctx.connect()
```

```python
bad_endpoint("valid/basic.nesql", error=4002, setup_file="mosquitto.conf"),
```

### Empty

`empty` is lifecycle-only: open → close (start → stop) with **nothing seeded**, and asserts both succeed. It is the 0-buffer boundary that `round_trip_1_buffer` (the 0→1 boundary) does not cover, and it isolates a class of bug where a connector mishandles an open/close with no data in flight — independent of any data path.

```python
def empty(ctx: Ctx) -> None:
    ctx.setup()
    ctx.connect()
    ctx.disconnect()
```

### Reconnect

`reconnect` proves a connector recovers from a **warm** connection loss: the connector process stays alive, but its link to the service is severed and restored between two batches. It is a `needs_link` scenario — the connector is routed through toxiproxy, and `ctx.link.cut()` / `ctx.link.restore()` give a deterministic outage that a real network would only produce flakily. The runner transfers the first batch, cuts and restores the link, then transfers the second; `OK` iff the connector reconnects and delivers it. A connector (or config) that cannot recover declares how it fails — `error=<code>` if it raises, or `loss=…` if the link heals but the second batch is dropped (e.g. a short idle budget that lets a source self-terminate during the gap).

```python
def reconnect(ctx: Ctx) -> None:
    ctx.setup()
    ctx.connect()
    if ctx.is_source:
        head, tail = ctx.source_batches(2)
        ctx.seed_and_fill(head)
        ctx.link.cut()
        ctx.link.restore()
        if ctx.expect_lossy:
            ctx.loader.seed_batch(tail, target=ctx.target)
            result = ctx.src.fill(*ctx.data.fill_counts(tail))
            ctx.data.compare_fill([], observed=result.observed)
        else:
            ctx.seed_and_fill(tail)
    else:
        ctx.write_buffers(1)
        ctx.link.cut()
        ctx.link.restore()
        ctx.write_rest()
    ctx.disconnect()
    ctx.finish()
```

The same connector is often bound twice, once per outcome:

```python
reconnect("valid/persistent-patient.nesql", setup_file="mosquitto.conf"),            # recovers → OK
reconnect("valid/persistent.nesql", loss=True, setup_file="mosquitto.conf"),         # gives up → source LOSS
```

### Outage

`outage` is the harder sibling of `reconnect`: instead of cutting the link in the *quiet* between two batches, a batch is in flight **while the link is cut**. Whether that batch survives is a property of the connector's configuration, and the declared outcome picks the read-back oracle — `OK` expects the whole dataset back, `LOSS` expects the during-cut batch gone. Toxiproxy is again what makes the outage deterministic. The two kinds realize the cut from opposite ends:

- **Source** — the *far* subscriber goes offline while a QoS-0 batch is published; whether the broker queues it for the offline subscription is the `queue_qos0_messages` policy, selected by the mounted `setup_file`. Delivered → the observed stream must be exactly batch-A-then-B; lost → filling B's quota yields exactly B, deterministically proving A was dropped. The source returns `OK` either way, so a source loss carries no error code (a bare `LOSS`).
- **Sink** — the *near* link to the broker drops mid-write. A buffering sink queues the during-cut batch and flushes it on reconnect (`OK`); a non-buffering sink rejects the write with a connector error (`LOSS <code>`), and the read-back — which goes straight to the service, not through the proxy — confirms only the pre-cut batch survived.

```python
def outage(ctx: Ctx) -> None:
    ctx.setup()
    ctx.connect()
    if ctx.is_source:
        a, b = ctx.source_batches(2)
        ctx.link.cut()
        ctx.loader.seed_batch(a, target=ctx.target, qos=0)
        ctx.link.restore()
        ctx.loader.seed_batch(b, target=ctx.target, qos=1)
        expected = b if ctx.expect_lossy else ctx.dataset.rows()
        result = ctx.src.fill(*ctx.data.fill_counts(expected))
        ctx.data.compare_fill(expected, observed=result.observed)
        ctx.disconnect()
        return
    kept = ctx.write_buffers(1)
    ctx.link.cut()
    if not ctx.expect_lossy:
        ctx.write_rest()
        ctx.link.restore()
        ctx.disconnect()
        ctx.finish()
        return
    err = None
    try:
        ctx.write_buffers(1)
    except ConnectorError as e:
        err = e
    ctx.link.restore()
    ctx.disconnect()
    ctx.verify_rows(ctx.dataset.rows()[:kept])
    if err is not None:
        raise err
```

```python
outage("valid/persistent.nesql", setup_file="mosquitto-queue-qos0.conf"),   # queued + delivered → OK
outage("valid/persistent.nesql", loss=True, setup_file="mosquitto.conf"),   # dropped → source LOSS
```

### Slow link

`slow_link` covers the "slow ≠ broken" property: a degraded-but-alive link where bytes still flow, just slowly. It applies a **bidirectional latency toxic** (the catalog default is 200 ms/direction, overridable per binding) *before* connecting — so the link is slow from the handshake on — then runs a normal full transfer. `OK` iff the connector tolerates the latency without dropping data or mistaking a slow link for a dead one or an end-of-stream. This is distinct from `outage` and `crash_recovery`, where bytes actually *stop*; here they never do, which is exactly the case a naive idle-timeout would misclassify. It is one of the two degraded-but-alive scenarios: `slow_link` probes the *rate* axis (continuous flow, per-operation delay), `silent_link` the *continuity* axis (one long byte-free interval) — see `silent_link` for why the two discriminate independently.

```python
def slow_link(ctx: Ctx) -> None:
    ctx.setup()
    ctx.link.throttle(latency_ms=ctx.latency_ms)
    ctx.connect()
    ctx.transfer()
    ctx.disconnect()
    ctx.finish()
```

### Silent link

`silent_link` is a true *silence*: `Link.silence()` stalls every byte in both directions for ~15 seconds while the connection stays fully established — no FIN, no RST — then `unsilence()` lifts it and a second batch is sent. Nothing moves during the gap; the gap's *length* is what is under test. What it flushes out is a connector that mistakes silence for failure: an idle/read deadline that declares end-of-stream, a keepalive that expires, a "no traffic = dead peer" heuristic. A connector that simply keeps the connection open bridges it (`OK`); one whose own budget fires during the gap declares how it fails (`error`/`loss`). This is the deliberate counterpart to `reconnect`/`outage`, whose *cut* severs the connection — a connector may legitimately treat a severed connection as fatal, but silence on a live connection is a normal stretch of any real deployment's life. `slow_link` is the sibling on the other axis of the degraded-but-alive family: there bytes flow continuously (just late), so no idle timer ever accumulates; here one long byte-free interval is exactly what lets it. The two discriminate independently — MQTTSource's short-idle-budget config passes `slow_link` and loses the post-gap batch here. The hold is a deliberate, bounded fault duration injected by the scenario — not a wait-and-hope sleep, since there is no state to poll — and the global deadline bounds the case. In code it is `reconnect`'s body with `ctx.link.silence()` / `time.sleep(_SILENCE_HOLD_S)` / `ctx.link.unsilence()` in place of the cut+restore.

```python
silent_link("valid/basic.nesql", setup_file="tcp_relay.py"),    # TCP: no idle budget — bridges it → OK
silent_link("valid/persistent.nesql", loss=True, ...),          # MQTT: ~2 s idle budget fires during the gap → bare LOSS
silent_link("valid/persistent-patient.nesql", ...),             # MQTT: 30 s idle budget outlasts the hold → OK
```

### Crash recovery

`crash_recovery` models a connector **process** dying mid-stream — a worker killed by, say, an unrelated query — with no graceful `CLOSE`/`STOP`. The runner `SIGKILL`s the harness after the first batch, then spawns a brand-new process whose connector must take over the backend residue the dead one left behind. It is deliberately *not* a `needs_link` scenario: the process death **is** the fault, distinct from `reconnect`'s warm link cut where the connector lives. `OK` iff the round trip completes across the death.

The two kinds recover from opposite sides of the position-ownership boundary:

- **Source** — read the first batch, die, re-open, read the second. The read position is **backend-owned**, so the fresh connector resumes past the first batch on its own: a persistent broker session redelivers from its session position; a time-windowed poll's watermark resets to the restart instant, which already excludes the pre-crash rows.
- **Sink** — commit the first buffer, die, then re-`START` at that committed offset (`resume=True`) so the respawned sink writes only the tail. Here the **runner** owns the sink's position, so the resume is deterministic and the read-back stays duplicate-free.

```python
def crash_recovery(ctx: Ctx) -> None:
    ctx.setup()
    ctx.connect()
    if ctx.is_source:
        head, tail = ctx.source_batches(2)
        ctx.seed_and_fill(head)
        ctx.kill()
        ctx.connect()
        ctx.seed_and_fill(tail)
    else:
        ctx.write_buffers(1)
        ctx.kill()
        ctx.connect(resume=True)
        ctx.write_rest()
    ctx.disconnect()
    ctx.finish()
```

```python
crash_recovery("valid/persistent.nesql", setup_file="mosquitto.conf"),
```

