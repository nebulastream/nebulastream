# Connection Test (ConnTest)

**ConnTest** tests NebulaStream connectors against *real* external services. Instead of mocking the
other side, it brings it up in Docker, drives the actual connector implementation against it through
the engine's own source machinery, and compares what arrives with what was sent. A passing test means
the connector works with the real protocol, not a stand-in.

This first version covers **sources**; `TCPSource` is the first connector.

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

The Python dependencies of the runner and of every loader are in the root `pyproject.toml`.
The development image ships them in its venv, installed from `uv.lock`,
so with it the tests run without network, once the data services' images are pulled.
`pyproject.toml` and `uv.lock` are part of the dependency hash, so changing them needs the development image of the new hash.
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

| Scenario | What it proves |
|---|---|
| `round_trip_1_buffer` | seed, read and compare less than one buffer of data, so the source must emit a partly filled buffer |
| `round_trip_n_buffers` | the same with more buffers than the harness pools, so the source must wait (100 by default, `n_buffers=N` changes it) |
| `empty` | open → close with nothing seeded |
| `bad_endpoint` | opening against a dead endpoint raises the declared error |
| `close_while_reading` | close while the connector waits in a read for data that never comes; the stop must interrupt it within the step timeout |
| `full_buffer_not_held` | with a flush interval that never expires during a test, a full buffer still arrives, and the partly filled rest does not |
| `crash_recovery` | SIGKILL the harness after a first batch; a fresh process re-opens and reads the second |

The catalog function writes only the ideal path. `run_case` enforces the declared outcome: an `OK`
binding must complete cleanly, an `ERROR <code>` binding must raise a connector error with that code,
and a harness-origin error (infrastructure failure, step timeout) always fails.

Data comes from one deterministic generator: it parses the schema from the config's
`CREATE LOGICAL SOURCE`, and its rows are both
what the loader seeds and the oracle the observed stream is compared against. The wire form is JSONL.
N buffers of data are as many rows as fit in less than N times the harness's buffer size (4096 bytes) in that encoding,
so the last buffer is always partly filled.
A failing test id carries N (e.g. `round_trip_n_buffers-TCPSource-basic-N100`), so it replays exactly.

Config validation is not a scenario.
`conntest-<Name>-configs` binds every committed config of a connector unmodified, without Docker,
so each one is safe to copy as an example.
Configs a connector must reject belong in its own unit tests as inline values (see `TCPSource/tests/`),
not in config files.

## Adding a connector

A connector's whole test surface is in a `conn-test/` directory next to its `CMakeLists.txt`;
nothing under `nes-conn-test/` changes, and only a loader with a client library edits a shared file, the root `pyproject.toml`:

```
conn-test/
  compose.yaml          optional: exactly one service with an `expose:` port, the connector's endpoint
  configs/*.nesql       valid configs (CREATE LOGICAL SOURCE + CREATE PHYSICAL SOURCE); each must bind as committed
  loader.py             a SourceLoader subclass that seeds the service
  scenarios.py          SCENARIOS = [...] built from conntest_runner.scenario_bindings
```

and one line in the connector's `CMakeLists.txt`:

```cmake
add_conn_test(PLUGIN <Name>)
```

The runner finds a connector only through this call, which records it in `<build>/nes-conn-test/connectors.txt`.
A loader's client library goes into `dependencies` in the root `pyproject.toml`, followed by `uv lock` at the repo root;
both files are part of the dependency hash, like a vcpkg change.

The loader implements three methods (`conntest_runner/contracts.py`):

- `config_overrides(target, endpoint)` maps the framework's per-test endpoint (and resource name, e.g.
  a topic) onto the config keys the `.nesql` declares; every other authored value passes through.
- `setup(target, schema)` prepares the service before the connector opens.
- `seed_batch(rows, target)` writes one batch of generated rows (`rows_to_jsonl` renders them).

The loader gets the endpoint of the service in `compose.yaml`.
A connector without a service has no `compose.yaml`, and its loader starts the server itself and sets its own endpoint.
`TCPSource/conn-test/` is the reference for that case: the TCP source is a client, so its loader is the server it connects to.

The design principles every change should keep are in `core_design_principles.md`.
