# nes-conn-test

End-to-end testing framework for source / sink connectors. It speaks to
real external systems (MQTT/Kafka brokers, DBs, etc.) in Docker rather than mocking
them, so a passing test means the connector actually works with the
real protocol implementation.

## Quick start

**Run the suite (CLion):** click ▶ on the **ConnTest** run config — it builds
`conntest-harness`, then runs the full battery. Edit or duplicate the config to
scope it (append a scenario short name and/or `--plugin <Name>`).

**Run the suite (CLI):**

```bash
./nes-conn-test/runner/conntest run                       # full battery
./nes-conn-test/runner/conntest run round_trip            # one scenario
./nes-conn-test/runner/conntest run --plugin MQTTSource   # one connector
./nes-conn-test/runner/conntest list                      # plugins x scenarios
```

Append `-- -s -v` to forward flags to pytest; see [Running tests](#running-tests).

**Debug a connector (CLion):** once, set `HOST_NEBULASTREAM_ROOT=<host repo
path>` under Settings → Build, Execution, Deployment → CMake → Environment and
Reload CMake. Then click 🐞 on **ConnTest Debug** and set breakpoints in the
connector source. To choose *what* to debug, edit the **ConnTest Debug Launcher**
config's command, e.g. `conntest debug round_trip --detach --plugin MQTTSink`.
Full detail (and the `HOST_NEBULASTREAM_ROOT` rationale): [Debugging](#debugging).

---

## How it works

Two pieces:

1. **`driver/conntest-harness`** — a small C++ binary that loads a
   `NES::Source`/`NES::Sink` connector via the plugin registry and runs a
   long-lived **stepper**: it connects to the runner's unix-domain control
   socket (`--control-sock`) and executes one command per line, replying
   with one JSON line, until EOF (source: `VALIDATE | OPEN | FILL <n> |
   DRAIN | CLOSE`; sink: `VALIDATE | START | WRITE <n> | STOP`). The runner
   drives a deterministic, event-synchronized sequence and can SIGKILL +
   respawn the harness mid-scenario to force reconnect-via-`open()`.

2. **`runner/`** — a pytest-based runner that:
   - discovers per-connector test surfaces by globbing
     `nes-plugins/<kind>/<Name>/conn-test/loader.py`,
   - brings up the connector's docker-compose stack (`compose.yaml`),
   - parametrizes ONE generic conformance battery (round_trip, empty,
     config, bad_endpoint, concurrent, reconnect) over every discovered
     source and sink — a shared, connector-blind scenario catalog,
   - drives the harness step by step and asserts the declared outcome.

To add a connector, drop a `conn-test/` dir into the plugin with
`loader.py`, `compose.yaml`, and a `configs/{valid,invalid}/*.nesql` tree
of pristine config files. The loader's `SCENARIOS` binds each scenario to
the config(s) it runs against; the live/bad-endpoint scenarios reuse a
`valid/*.nesql` file, with the loader's `config_overrides` swapping the
endpoint/target in by key (so there is no `.template` and no separate
`bad_endpoint.sql`). Zero edits to `nes-conn-test/` are needed; the
framework picks it up automatically.

The runner ↔ harness contract is documented inline in
[`runner/conntest_runner/protocol.py`](runner/conntest_runner/protocol.py).

---

## Docker setup

The one rule everything follows: **never ask "which container am I" — let
compose _create_ the test container already on the service network.** That
single decision is what makes the setup CI-correct and portable: no
`docker network connect $HOSTNAME` self-attach, no host-port discovery, no
`host.docker.internal`, no container-IP guessing.

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

**Two compose files, merged at run time into one throwaway project per plugin:**

| File | Owner | Declares |
|---|---|---|
| `nes-plugins/<kind>/<Name>/conn-test/compose.yaml` | the **plugin** | its data service(s) only — `broker`, `db`, … (+ healthcheck) |
| `nes-conn-test/runner/runner.compose.yaml` | the **framework** | the single generic `runner` service (one copy, names no plugin/broker/port) |
| `nes-conn-test/runner/toxiproxy.compose.yaml` | the **framework** | a generic `toxiproxy` fault sidecar, merged only when the plugin declares a `needs="link"` (reconnect) scenario |

**Lifecycle** — `conntest` runs this once per plugin, sequentially:

```
   up -d --wait <data svc>   ─▶   run --rm runner pytest   ─▶   down -v
   block on the healthcheck       one-off runner created        tear the whole
   (broker/db Healthy)            on the project network;       per-plugin project
                                  pytest Popens the harness     down (fresh next time)
```

**Why it's shaped this way (the parts that matter):**

- **pytest lives _inside_ the runner** because it drives the harness over a
  unix-domain control socket on a shared filesystem path
  (`/tmp/conntest-*/ctl-*.sock`) — purely container-internal, no network. The
  harness `connect()`s the socket itself (rather than inheriting an fd), which
  also keeps the `lldb-server` debug path working and drops the old
  pytest-must-be-the-parent constraint.
- **the harness reaches the service by DNS** (`broker:1883`, `db:5432`) because
  compose created the runner as a peer on the project network. No published
  data ports, no IPs, no host networking — the endpoint is literally the
  compose service name. (The loader's `compose_service`/`compose_port` must
  match a service in the plugin's `compose.yaml`; discovery validates this and
  fails fast on a mismatch.)
- **the orchestrator runs on the host** (or CI VM) and only drives
  `docker compose`. If the host has no `docker compose`, `conntest` self-wraps
  once into a dev container and proceeds identically — so local and CI share
  one code path.
- **isolation:** each plugin gets its own `nebulastream-conntest-<uuid>`
  project + network, brought up and `down -v`'d independently. Fresh data
  service per plugin, no cross-plugin state, collision-free project names.

The debug flow ([Debugging](#debugging)) layers a third file,
`compose.debug.yaml`, onto the runner (lldb-server port + a socat keepalive
sidecar). For how the runner is told _which_ broker to hand the harness, see
[Python discovery](#python-discovery-runtime).

---

## Running tests

### Prerequisites

- Docker daemon reachable.
- The dev image installed locally, e.g., `nebulastream/nes-development:local`
  (or any `nebulastream/nes-development:*` — the scripts auto-pick a
  reasonable one).
- A CMake build dir configured for the harness. `conntest` autodetects
  it: the conventional `cmake-build-debug` wins if present, otherwise any
  configured `cmake-build-*` (preferring a `CMAKE_BUILD_TYPE=Debug` one) is
  used — so CLion's Docker-toolchain dir
  (`cmake-build-debug-docker-<image>`) is picked up automatically. Pass
  `--build-dir <name>` to override. Headless equivalent:
  ```bash
  cmake -S . -B cmake-build-debug -DCMAKE_BUILD_TYPE=Debug
  ```

### From CLion

Two run configurations ship under `.idea/runConfigurations/`:

| Run config | What it does |
|---|---|
| **ConnTest** | Click ▶. Runs `conntest run --build`: builds `conntest-harness` in the runner, then runs the full conformance battery via pytest. |
| **ConnTest Debug** | Click 🐞. See [Debugging](#debugging) below. |

To run a subset, edit the **ConnTest** config's inline command (or duplicate the
config in CLion) and append a selector, e.g. `conntest run --build
round_trip`. The selector understands these shortnames:

```
round_trip       — round-trip
empty            — no-bytes scenario
large            — multi-KB checksum-only
binary           — full byte-range fixture
config           — docker-free, validate each configs/{valid,invalid}/*.nesql
bad_endpoint     — docker-free, unreachable broker
conformance      — all conformance scenarios
discovery        — framework self-tests (discovery)
protocol         — framework self-tests (HandshakePipes + Report)
```

Anything else is forwarded to pytest as-is, so a `-k expression` (e.g.
`-k "round_trip and MQTTSource"`) also works — the conformance cases are
parametrized under a single `test_scenario`, so select them by `-k`
substring rather than a `::nodeid`. Pass extra flags after `--`:
`round_trip -- -s -v`.

To scope a run to **one connector**, add `--plugin <Name>` (e.g.
`conntest run round_trip --plugin MQTTSource`). Unlike a bare `-k Name`
filter — which still cycles every plugin's docker stack up and down only to
deselect its tests — `--plugin` brings up *only* that connector's
infrastructure and runs only its tests (the framework meta tests are skipped
for a focused run). Combine it with a selector to narrow further, or omit the
selector to run all of that plugin's scenarios.

Duplicate the run config in CLion (right-click → Edit Configurations →
Copy) if you want several scenario-specific buttons.

### From the command line

```bash
# Full battery (every plugin + the meta tests):
./nes-conn-test/runner/conntest run

# Build the harness first, then run (local convenience):
./nes-conn-test/runner/conntest run --build

# Specific scenario:
./nes-conn-test/runner/conntest run round_trip

# Forward args to pytest:
./nes-conn-test/runner/conntest run round_trip -- -s -v

# List discovered plugins x scenarios:
./nes-conn-test/runner/conntest list
```

`conntest` is the single entry point (docker-setup-v3). It runs on the
host, drives `docker compose`, and runs pytest inside the per-plugin
compose `runner` container — which compose creates already on the broker
network, so `broker:1883` resolves by DNS with no self-attach. On a host
without `docker compose` it transparently self-wraps into a one-shot dev
container. First invocation of a fresh dev image takes ~30s while the
runner bootstraps `uv` (newer images ship it); subsequent runs are
cached.

### Native pytest gutter icons (optional)

If you want ▶ next to each `def test_…` in the editor:

1. Settings → Project → Python Interpreter → Add → On Docker.
2. Image: `nebulastream/nes-development:local` or similar.
3. Run `conntest run` once to materialize `.venv`, then point the
   interpreter at
   `/tmp/nebulastream/nes-conn-test/runner/.venv/bin/python`.
4. Mark `nes-conn-test/runner/` as test sources root. pytest config is
   auto-picked from `runner/pyproject.toml`.

Gutter icons are nicer for fine-grained "run this one test" workflows;
the smoke-config route above is more robust across CLion versions.

---

## Debugging

> **Prerequisite — set `HOST_NEBULASTREAM_ROOT` in your CLion CMake profile.**
> CLion configures CMake *inside* the dev container, where the repo is the
> bind-mount target (e.g. `/tmp/nebulastream-public`), so the harness debug
> info is written with that container path. `-fdebug-prefix-map` (root
> `CMakeLists.txt`) rewrites those paths back to your host repo path — but
> only when `HOST_NEBULASTREAM_ROOT` is set. Without it, breakpoints set on
> host paths never bind: the session detaches the instant you continue, and
> the run configs aren't generated. Add it under Settings → Build, Execution,
> Deployment → CMake → your profile → Environment:
> ```
> HOST_NEBULASTREAM_ROOT=<absolute path to this repo on the host>
> ```
> then Reload CMake Project and rebuild `conntest-harness`. (Flipping a global
> compile option means that first rebuild is a full one.) `conntest debug`
> refuses to start while the harness still has container paths, with the same
> hint.

The "ConnTest Debug" run config does the right thing
end-to-end. By default it debugs `MQTTSource`'s `round_trip` scenario; to
debug a different connector or scenario, edit the SCRIPT_TEXT of the
sibling "ConnTest Debug Launcher" config (same as you'd edit
"ConnTest") — it takes the regular `conntest debug` args, e.g.
`conntest debug round_trip --detach --plugin MQTTSink`. The chosen
connector's `setup_file` (e.g. `mosquitto.conf`) is mounted automatically.
Mechanically:

1. The *Before Launch* step runs `runner/conntest debug --detach`,
   which: cleans up any stale debug container holding port 2345, brings
   up the per-plugin compose stack (broker + the `runner` service with
   `seccomp=unconfined`, since lldb-server needs `personality(2)`),
   `exec`s pytest inside the runner, and waits until `lldb-server-19
   gdbserver` is listening on `0.0.0.0:2345`.
2. The harness reaches the runner over the unix-domain control socket it
   `connect()`s itself, which survives `lldb-server` (it closes only the
   inferior's *inherited* non-stdio fds, not a socket the inferior opens) —
   so the stepper protocol works unchanged under the debugger, with no
   special-case escape valve.
3. pytest exec's `lldb-server-19 gdbserver 0.0.0.0:2345 --
   conntest-harness …`. The harness's wall-timeout is auto-bumped to
   30 minutes so a paused debugger session doesn't trip the watchdog.
4. CLion attaches as an **LLDB** client at `connect://127.0.0.1:2345`.
   (Don't switch to the GDB client — its aarch64 'g' register-packet
   handling is broken against lldb-server's 816-byte reply.)
5. Symbol file: `<build-dir>/nes-conn-test/conntest-harness`, where
   `<build-dir>` is the autodetected build dir baked in by `conntest
   install-clion`.
6. The repo-wide `-fdebug-prefix-map=container=host` rewrites debug
   info paths so CLion gutter breakpoints resolve directly across the
   whole repo — no per-target source mappings required.

### Setting breakpoints

Set breakpoints anywhere in:

- `nes-conn-test/driver/*.cpp` — the harness
- `nes-plugins/{Sources,Sinks}/<Plugin>/*.cpp` — the connector under debug
  (e.g. `MQTTSource`, the default; or whichever `--plugin` you selected)
- Any transitive code (`nes-sources/`, etc.)

Click 🐞 → continue execution from CLion. Breakpoints fire as the
harness reaches them.

### Inspecting `std::string_view` (libstdc++)

lldb only ships a `string_view` summary for libc++, so on the
`local-libstd` image a view shows as `(_M_len = N, _M_str = "")` — empty for
binary/leading-NUL data even when `size()` is non-zero. Load the bundled
libstdc++ summary into CLion's lldb (it sources `~/.lldbinit` on start) to get
length-bounded, escaped output: add
`command script import <repo>/nes-conn-test/sv_fmt.py` to `~/.lldbinit`.

### When something goes wrong

| Symptom | Cause / fix |
|---|---|
| `Bind for 127.0.0.1:2345: port is already allocated` | A previous debug session left its container running. `conntest debug --detach` auto-cleans on the next run; if it persists, `docker ps --filter publish=2345` + `docker rm -f <id>`. |
| `personality set failed: Operation not permitted` | seccomp blocked lldb-server. `compose.debug.yaml` sets `seccomp=unconfined` on the runner; only a problem if you invoke pytest with `--native-debug-port=…` outside `conntest debug`. |
| `Remote connection closed` / CLion attaches before banner | Almost always a Colima/Lima port-forwarding race. `conntest debug --detach` polls the harness log for the listener-ready banner before exiting; if CLion still races, switch the run config to the foreground variant (drop the *Before Launch* step and run `./nes-conn-test/runner/conntest debug round_trip` in a terminal first). |
| Logs missing | Detached mode writes to `/tmp/conntest-debug-2345.log` (PID at `/tmp/conntest-debug-2345.pid`). |

### Foreground debug (manual control)

If you want to see the live output from pytest while debugging:

```bash
./nes-conn-test/runner/conntest debug round_trip
```

…then click Debug in CLion with the *Before Launch* step disabled.
Useful when iterating on harness or framework code itself, since
you'll see test stderr live rather than after the gdb-remote session
ends.

## How plugins get picked up automatically

Two independent mechanisms — one for Python discovery, one for C++ link
inclusion. They're decoupled and run at different times.

### Python discovery (runtime)

When `test_conformance.py` is collected, this fires at module import:

```python
_REPO_ROOT = Path(__file__).resolve().parents[3]
_PLUGINS: list[PluginEntry] = list(discover_plugins(_REPO_ROOT))
```

[`discovery.py`](runner/conntest_runner/discovery.py) walks
`nes-plugins/<kind>/` (default `kinds=("Sources",)`) and, for each
plugin subdirectory:

1. Checks for `conn-test/loader.py` — absent ⇒ skip silently.
2. Imports `loader.py` under a unique module name via
   `importlib.util.spec_from_file_location` (does **not** touch
   `sys.modules`, so each test run gets a fresh import).
3. Finds the single class in that module that quacks like a
   `TestDataLoader` — has `system` / `compose_service` /
   `compose_port` class attributes and a callable `seed`. Zero or
   more than one match raises.
4. Resolves the scenarios list: prefers a sibling `scenarios.py` if
   present, otherwise reads a `SCENARIOS` class attribute on the
   loader, otherwise raises.
5. Yields a `PluginEntry(name, kind, plugin_dir, conn_test_dir,
   loader_cls, scenarios)`.

`test_conformance.py` then parametrizes each scenario function over the
plugins that opted into that scenario:

```python
@pytest.mark.parametrize(
    "plugin,endpoint",
    [pytest.param(p, p, id=p.name) for p in _for("round_trip")],
    indirect=["endpoint"],
)
def test_round_trip(plugin, endpoint, ...):
```

So adding `nes-plugins/Sources/KafkaSource/conn-test/loader.py` is
enough to make `test_round_trip[KafkaSource]` appear in the next
`pytest --collect-only`.

### C++ link inclusion (configure + link time)

The plugin's own `CMakeLists.txt` registers itself into a global CMake
list via `add_plugin_as_library()`
([`cmake/PluginRegistrationUtil.cmake`](../cmake/PluginRegistrationUtil.cmake)):

```cmake
function(add_plugin_as_library plugin_name plugin_registry ...)
    add_library(${plugin_library} STATIC ${sources})
    set_property(GLOBAL APPEND PROPERTY "${plugin_registry}_plugin_libraries" "${plugin_library}")
endfunction()
```

For source connectors that property is `Source_plugin_libraries`.
After all subdirectories are processed it contains every active
source plugin.

`nes-conn-test/CMakeLists.txt` reads it and links each entry into the
harness, with two complications:

**Ordering.** The root `CMakeLists.txt` adds subdirectories via
`file(GLOB "nes-*")` which is alphabetical: `nes-conn-test` is
processed *before* `nes-plugins`, so an immediate `get_property` read
would yield an empty list. The fix is to defer the iteration to the
project root scope via `cmake_language(DEFER … EVAL CODE)`:

```cmake
cmake_language(EVAL CODE "
    cmake_language(DEFER DIRECTORY [[${PROJECT_SOURCE_DIR}]] CALL cmake_language EVAL CODE [[
        get_property(_source_plugin_libs GLOBAL PROPERTY Source_plugin_libraries)
        foreach (_plugin_lib IN LISTS _source_plugin_libs)
            target_link_libraries(conntest-harness PRIVATE
                    \"-Wl,--whole-archive\"
                    \$<TARGET_FILE:\${_plugin_lib}>
                    \"-Wl,--no-whole-archive\"
            )
            add_dependencies(conntest-harness \${_plugin_lib})
        endforeach ()
    ]])
")
```

This runs at *generate time*, after every plugin's `add_subdirectory()`
has populated the property.

**Dead-code elimination.** The harness has no compile-time reference to
any plugin — each plugin's `Register<Name>Source` (a static-init that
inserts the connector into `SourceRegistry::instance()`) is only
reachable through the registry, never named directly. Without
`--whole-archive`, the linker discards the entire plugin archive.
`-Wl,--whole-archive` / `-Wl,--no-whole-archive` force every object in
each plugin's static library into the final binary so the static-init
runs before `main()`. The `add_dependencies` line is
belt-and-suspenders: `$<TARGET_FILE:…>` inside `target_link_libraries`
does not register an automatic build-order edge under Ninja, so without
it the harness could try to link before the plugin archive has been
rebuilt.

### Putting it together

At harness runtime:

1. C++ static-init (before `main`): every linked plugin's
   `Register*Source` runs and inserts itself into the singleton
   `SourceRegistry`.
2. `main()` → `runSource(opts)` →
   `SourceRegistry::instance().create(sourceType, ...)` looks up the
   connector by the type-string from the SQL — succeeds because the
   plugin registered itself in step 1.

So adding a connector requires:

- A `CMakeLists.txt` calling `add_plugin_as_library(...)` (gets the C++
  archive linked).
- A `conn-test/loader.py` (gets the Python tests parametrized).

Both ends decoupled, both ends automatic.
