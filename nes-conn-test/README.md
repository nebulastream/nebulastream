# Connection Test (ConnTest)

**ConnTest** tests NebulaStream connectors against *real* external services.
Instead of mocking the other side, it brings it up in Docker,
runs the actual connector implementation against it through the engine's own source and sink machinery,
and compares what arrives with what was sent.
A passing test means the connector works with the real protocol, not a stand-in.

ConnTest tests each connector in isolation; the systests test queries.
It covers **sources** and **sinks**.

## Running it

Each connector's battery is one ctest test, labelled `ConnTest` and `DockerCompose`:

```bash
cmake --build <build> --target conntest-harness conntest-config-test
ctest --test-dir <build> -L ConnTest --output-on-failure
```

`conntest-<Name>` (a connector's battery) needs `-DENABLE_DOCKER_TESTS=ON` like every `DockerCompose` test.
`conntest-runner-tests` (the runner's own unit tests) and `conntest-<Name>-configs` (the committed configs bind) run without Docker.
The battery and the runner tests need `uv`, which the development image ships.
In CI the battery runs in the Docker lane, with the sanitizer options of `.github/.env/test-<sanitizer>.env` forwarded to the harness.

The runner's Python dependencies are in the root `pyproject.toml`,
and each loader's are in its `conn-test/pyproject.toml`, a member of the root's uv workspace.
`uv.lock` locks them all.
The development image ships them in its venv, installed from `uv.lock`,
so with it the tests run without network, once the data services' images are pulled.
The root `pyproject.toml` and `uv.lock` are part of the dependency hash, so changing a dependency needs the development image of the new hash.
Outside the development image, uv installs the dependencies, which needs network.

ctest calls the runner's CLI, which can also be used directly, e.g. to pass arguments to pytest:

```bash
./nes-conn-test/runner/conntest run --build-dir <build> --plugin <Name>
./nes-conn-test/runner/conntest run --build-dir <build> --plugin <Name> -- -k transfer
./nes-conn-test/runner/conntest list --build-dir <build>
```

Inside the development container, the runner reuses that container's image.
Elsewhere, pass `--image` or set `NES_DEV_IMAGE`.

## Architecture

See [Architecture.md](Architecture.md).

## Scenarios

A scenario is a connector-agnostic function in the shared catalog (`runner/src/conntest_runner/scenarios.py`).
A connector opts in through its `conn-test/scenarios.py` and supplies the binding: which config(s) and the expected outcome.
A source takes the constructors from `conntest_runner.scenario_bindings.source`, and a sink from `.sink`.
Each module offers only the scenarios that its kind can bind, as listed below.
A binding can also pass keyword arguments to the loader's constructor,
for a config whose data the loader must seed or read back differently from the connector's other configs.

| Scenario | Kinds | What the connector must do |
|---|---|---|
| `transfer_1_buffer` | both | Deliver less data than one buffer completely. A source must emit a partly filled buffer, and a sink must deliver one. |
| `transfer_n_buffers` | both | Deliver many buffers of data (100 by default, `n_buffers=N` changes it) completely and exactly once. A source must wait while the harness's pool is used up. A sink is called from several worker threads at once, gets records that continue in child buffers, and may have a buffer retried that it could not take yet. |
| `empty` | both | Open and close without data. A source must close while it waits, and a sink must deliver nothing, not even an empty record. |
| `buffer_without_tuples` | sink | Deliver nothing, not even an empty record, for a buffer without tuples, which a query's operators emit when a filter drops every tuple of a buffer. |
| `bad_endpoint` | both | Fail with the declared error when the endpoint is unreachable. A sink that connects in the background, such as `TCPSink`, accepts the buffer that the scenario writes and fails once its connect times out, at the latest when it stops. |
| `close_while_reading` | source | End a read that waits for data that never comes, when the harness closes the source, within the step timeout. |
| `full_buffer_not_held` | source | Emit a full buffer at once, even with a flush interval that never ends during the test, and hold back the partly filled rest. |
| `partial_buffer_held` | source | Hold back a partly filled buffer for at least the flush interval. The harness measures each read that returns a partly filled buffer. |
| `slow_link` | both | Deliver every row while the link adds 200 ms in each direction from the start of the test, so the connect is slow too. |
| `silent_link` | both | Deliver every row although nothing flows on the open connection for 15 s between the first and the second half of the data. |
| `connection_lost` | source | End the stream, or fail with the declared error, when the connection is cut for good after the data has arrived, and not hang. |
| `crash_recovery` | source | Open in a fresh process after the harness was killed with SIGKILL after the first half of the data, while the service may still hold the killed connector's connection or session, and read the second half. Where the second half starts is the service's read position, not the connector's. |

A source's data is compared as it is read.
A sink's is compared after it has stopped, with what its loader reads back from the service.

The catalog function writes only the ideal path.
`run_case` enforces the declared outcome: an `OK` binding must complete cleanly,
an `ERROR <code>` binding must raise a connector error with that code,
and a harness-origin error (infrastructure failure, step timeout) always fails.

`slow_link`, `silent_link` and `connection_lost` route the connector through a toxiproxy container
(`runner/toxiproxy.compose.yaml`, controlled by `conntest_runner/link.py`),
which joins the stack only for a connector that binds one of them.
The loader still reaches the service directly, so a fault affects only the connector's connection.

Data comes from one deterministic generator.
It generates rows for the schema that the harness bound from the config and returned in its `BIND` reply.
Its rows are both what the loader seeds or the sink writes, and the oracle that what arrives is compared against.
The wire form is JSONL.
N buffers of data are as many rows as fit in less than N times the harness's buffer size (4096 bytes) in that encoding,
so they never fill N buffers completely.
The sink harness fills each buffer before it starts the next, so a sink gets the rows in at most N buffers.
A failing test id carries N (e.g. `transfer_n_buffers-<Name>-basic-N100`), so it replays exactly.

Config validation is not a scenario.
`conntest-<Name>-configs` binds every committed config of a connector unmodified, without Docker,
so each one is safe to copy as an example.
Configs that a connector must reject belong in its own unit tests as inline values, not in config files.

## Adding a connector

A connector's whole test surface is in a `conn-test/` directory next to its `CMakeLists.txt`,
in `nes-plugins/<Kind>/<Name>/` (connection tests cover plugins, see `core_design_principles.md`).
Nothing outside it changes, except `uv.lock`, which uv generates:

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
The call also records whether the connector is a source or a sink, from its directory under `nes-plugins`, and the loader must subclass the matching base class.
A loader's client library goes into `dependencies` in its `conn-test/pyproject.toml`, followed by `uv lock` at the repo root.
The new `uv.lock` is part of the dependency hash, like a vcpkg change.

The loader implements these methods (`conntest_runner/contracts.py`):

- `config_overrides(target, endpoint)` maps the framework's per-test endpoint (and resource name, e.g. a topic)
  onto the config keys that the `.nesql` declares.
  Every other authored value passes through.
- `setup(target, schema)` prepares the service before a source opens or a sink starts.
- For a source, `seed_batch(rows, target)` writes one batch of generated rows (`rows_to_jsonl` renders them).
  `wait_seeded()` is optional: a loader whose `seed_batch` returns before the backend has the rows waits for them here.
- For a sink, `read_back(target, schema, n_rows, timeout_s)` returns the rows that the sink wrote, after it has stopped.
  `timeout_s` is the harness's step timeout, which bounds the read-back as it bounds any step.
  `jsonl_to_rows` parses them.

A sink config declares the `JSON` output format, because the harness gives the sink JSONL records:
the engine compiles that output formatter into the query, so such a sink receives formatted bytes.

`bad_endpoint` opens or starts the connector against `dead_endpoint`, `127.0.0.1:1` by default.
A connector whose endpoint is not `host:port`, such as a serial device, overrides it with a dead endpoint of its own kind.

The loader gets the endpoint of the service in `compose.yaml`.
A connector without a service has no `compose.yaml`, and its loader starts the server itself and sets its own endpoint.
That server must listen on the container's address, not on loopback, so that the fault proxy can reach it.
For example, the loader of a source or sink that connects as a client is the server that the connector connects to.

The design principles every change should keep are in `core_design_principles.md`.
