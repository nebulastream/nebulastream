# Connection Test (ConnTest)

**ConnTest** tests NebulaStream connectors against *real* external services. Instead of mocking the
other side, it brings it up in Docker, drives the actual connector implementation against it through
the engine's own source and sink machinery, and compares what arrives with what was sent. A passing test means
the connector works with the real protocol, not a stand-in.

It covers **sources** and **sinks**; `TCPSource` was the first source and `TCPSink` is the first sink.

## Running it

Each connector's battery is one ctest test, labelled `ConnTest` and `DockerCompose`:

```bash
cmake --build <build> --target conntest-harness conntest-config-test
ctest --test-dir <build> -L ConnTest --output-on-failure
```

`conntest-TCPSource` (the battery) needs `-DENABLE_DOCKER_TESTS=ON` like every `DockerCompose` test.
`conntest-runner-tests` (the runner's own unit tests) and `conntest-TCPSource-configs` (the committed configs bind)
run without Docker. The battery and the runner tests need `uv`, which the development image ships. In CI the battery runs in the Docker lane, with the sanitizer options of
`.github/.env/test-<sanitizer>.env` forwarded to the harness.

The runner's Python dependencies are in the root `pyproject.toml`, and each loader's are in its `conn-test/pyproject.toml`,
a member of the root's uv workspace. `uv.lock` locks them all.
The development image ships them in its venv, installed from `uv.lock`,
so with it the tests run without network, once the data services' images are pulled.
The root `pyproject.toml` and `uv.lock` are part of the dependency hash, so changing a dependency needs the development image of the new hash.
Outside the development image, uv installs the dependencies, which needs network.

ctest calls the runner's CLI, which can also be used directly, e.g. to pass arguments to pytest:

```bash
./nes-conn-test/runner/conntest run --build-dir <build> --plugin TCPSource
./nes-conn-test/runner/conntest run --build-dir <build> --plugin TCPSource -- -k round_trip
./nes-conn-test/runner/conntest list --build-dir <build>
```

Inside the development container the runner reuses that container's image; elsewhere, pass
`--image` or set `NES_DEV_IMAGE`.

## Architecture

See [Architecture.md](Architecture.md).

## Scenarios

A scenario is a connector-agnostic function in the shared catalog
(`runner/src/conntest_runner/scenarios.py`). A connector opts in through its `conn-test/scenarios.py`
and supplies the binding: which config(s) and the expected outcome.
A source takes the constructors from `conntest_runner.scenario_bindings.source` and a sink from `.sink`;
each module offers only the scenarios that its kind can bind, as listed below.

| Scenario | Kinds | What it proves |
|---|---|---|
| `round_trip_1_buffer` | both | less than one buffer of data arrives complete, so a source must emit a partly filled buffer |
| `round_trip_n_buffers` | both | the same with more buffers than the harness pools, so a source must wait, and a sink is called from several threads (100 by default, `n_buffers=N` changes it) |
| `empty` | both | open → close with no data; a sink delivers nothing |
| `bad_endpoint` | both | a dead endpoint raises the declared error; a sink that connects in the background raises it while it writes one buffer or stops |
| `close_while_reading` | source | close while the connector waits in a read for data that never comes; the stop must interrupt it within the step timeout |
| `full_buffer_not_held` | source | with a flush interval that never expires during a test, a full buffer still arrives, and the partly filled rest does not |
| `partial_buffer_held` | source | a partly filled buffer is held back for at least the flush interval; the harness measures each read call that returns a buffer |
| `slow_link` | both | the connection is slowed by 200 ms in each direction from before the connect; every row still arrives |
| `silent_link` | both | nothing flows on the live connection for 15 s between two batches; the second batch still arrives |
| `connection_lost` | source | the connection is cut for good after a first batch; the source ends its stream (or raises its declared error) instead of hanging |
| `crash_recovery` | source | SIGKILL the harness after a first batch; a fresh process re-opens and reads the second |

A source's data is compared as it is read.
A sink's is compared after it has stopped, with what its loader reads back from the service.

The catalog function writes only the ideal path. `run_case` enforces the declared outcome: an `OK`
binding must complete cleanly, an `ERROR <code>` binding must raise a connector error with that code,
and a harness-origin error (infrastructure failure, step timeout) always fails.

`slow_link`, `silent_link` and `connection_lost` route the connector through a toxiproxy container
(`runner/toxiproxy.compose.yaml`, controlled by `conntest_runner/link.py`), which joins the stack only for a connector that binds one of them.
The loader still seeds the backend directly, so a fault affects only the connector's connection.

Data comes from one deterministic generator: it generates rows for the schema that the harness bound from the config
and returns in its `BIND` reply, and its rows are both
what the loader seeds or the sink writes, and the oracle that what arrives is compared against. The wire form is JSONL.
N buffers of data are as many rows as fit in less than N times the harness's buffer size (4096 bytes) in that encoding,
so the last buffer is always partly filled.
A failing test id carries N (e.g. `round_trip_n_buffers-TCPSource-basic-N100`), so it replays exactly.

Config validation is not a scenario.
`conntest-<Name>-configs` binds every committed config of a connector unmodified, without Docker,
so each one is safe to copy as an example.
Configs a connector must reject belong in its own unit tests as inline values (see `TCPSource/tests/`),
not in config files.

## Adding a connector

A connector's whole test surface is in a `conn-test/` directory next to its `CMakeLists.txt`,
in `nes-plugins/<Kind>/<Name>/` (connection tests cover plugins, see `core_design_principles.md`);
nothing outside it changes, except `uv.lock`, which uv generates:

```
conn-test/
  compose.yaml          optional: exactly one service with an `expose:` port, the connector's endpoint
  configs/*.nesql       valid configs (CREATE LOGICAL SOURCE + CREATE PHYSICAL SOURCE, or one CREATE SINK); each must bind as committed
  loader.py             a SourceLoader subclass that seeds the service, or a SinkLoader subclass that reads back from it
  pyproject.toml        the loader's client libraries, if any; required even without them, as a member of the uv workspace
  scenarios.py          SCENARIOS = [...] built from conntest_runner.scenario_bindings.source or .sink
```

and one line in the connector's `CMakeLists.txt`:

```cmake
add_conn_test(PLUGIN <Name>)
```

The runner finds a connector only through this call, which records it in `<build>/nes-conn-test/connectors.txt`.
A loader's client library goes into `dependencies` in its `conn-test/pyproject.toml`, followed by `uv lock` at the repo root;
the new `uv.lock` is part of the dependency hash, like a vcpkg change.

The loader implements these methods (`conntest_runner/contracts.py`):

- `config_overrides(target, endpoint)` maps the framework's per-test endpoint (and resource name, e.g.
  a topic) onto the config keys the `.nesql` declares; every other authored value passes through.
- `setup(target, schema)` prepares the service before a source opens or a sink starts.
- For a source, `seed_batch(rows, target)` writes one batch of generated rows (`rows_to_jsonl` renders them).
  `wait_seeded()` is optional: a loader whose `seed_batch` returns before the backend has the rows waits for them here.
- For a sink, `read_back(target, schema, expect_records)` returns the rows that the sink wrote, after it has stopped
  (`jsonl_to_rows` parses them).

A sink config declares the `JSON` output format, because the harness hands the sink JSONL records:
the engine compiles the output formatter into the query, so a sink only ever receives formatted bytes.

`bad_endpoint` opens or starts the connector against `dead_endpoint`, `127.0.0.1:1` by default.
A connector whose endpoint is not `host:port`, such as a serial device, overrides it with a dead endpoint of its own kind.

The loader gets the endpoint of the service in `compose.yaml`.
A connector without a service has no `compose.yaml`, and its loader starts the server itself and sets its own endpoint.
That server must listen on the container's address, not on loopback, so that the fault proxy can reach it.
`TCPSource/conn-test/` and `TCPSink/conn-test/` are the references for that case:
the TCP source and sink are clients, so their loaders are the servers that they connect to.

The design principles every change should keep are in `core_design_principles.md`.
