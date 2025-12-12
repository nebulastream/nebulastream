<div align="center">
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="docs/resources/NebulaBanner.png">
    <source media="(prefers-color-scheme: dark)" srcset="docs/resources/NebulaBannerDarkMode.png">
    <img alt="NebulaStream logo" src="docs/resources/NebulaBanner.png" height="100">
  </picture>
  <br />
  <!-- Badges -->
  <a href="https://github.com/nebulastream/nebulastream-public/actions/workflows/nightly.yml">
    <img src="https://github.com/nebulastream/nebulastream-public/actions/workflows/nightly.yml/badge.svg"
         alt="NES Nightly" />
  </a>
  <a href="https://bench.nebula.stream/c-benchmarks/">
    <img src="https://img.shields.io/badge/Benchmark-Conbench-blue?labelColor=3D444C"
         alt="Conbench" />
  </a>
  <a href="https://codecov.io/github/nebulastream/nebulastream" > 
    <img src="https://codecov.io/github/nebulastream/nebulastream/graph/badge.svg?token=ER83Nm1crF" alt="Codecov"/> 
  </a>  
</div>

----

NebulaStream is an end-to-end data-management system for cloud-edge-sensor deployments.
The platform combines ease of use, extensibility, and efficiency to let teams focus on business logic while the engine optimizes the data path.

**Core Pillars**
- **Ease of Use** - Rich, out-of-the-box functionality for a variety of stream processing queries.
- **Extensibility** - Uniform plugin registries for custom connectors, formats, operators, and optimizations.
- **Efficiency** - Hardware-tailored code, adaptive execution, and interleaved I/O to handle large workloads.

## Vision

NebulaStream is a joint research project at BIFOLD with contributors from the DIMA Group at TU Berlin and the DFKI IAM Group.
It aims to execute thousands of queries over millions of heterogeneous sources in massively distributed environments.
We advance this vision through five core technologies:
- **Heterogeneous Hardware Support** - Targets diverse CPU architectures and accelerators such as GPUs and TPUs.
- **Code Generation** - Compiles every query to efficient native code for low latency and energy usage.
- **In-Network Processing** - Pushes operators toward data sources to reduce network traffic.
- **Adaptive Resource Management** - Reacts to topology or workload changes without interrupting queries.

The system architecture spans sensor to cloud:
1. **Sources & Sinks** - Built-in connectors (e.g., JDBC, MQTT, TCP) and formats (e.g., CSV, JSON) with plugin hooks for custom components; extend them via the plugin framework in the [Extensibility guide](docs/guide/extensibility.md).
2. **I/O Handling** - Thread-shared source processing and asynchronous callbacks minimize waiting time.
3. **Query Submission** - SQL-like language with prebuilt operators such as join and aggregation, plus user-defined operator plugins.
4. **Query Optimization** - Rule-based optimizer generates hardware-aware plans; users can extend the rule engine.
5. **Adaptive Runtime** - Task-based scheduling keeps execution responsive under dynamic workloads.

## Quick Start

### Build & Tooling

#### Use the Nix flake
Install [Nix with flakes enabled](https://nixos.org/download/) and generate build links:

```shell
nix run .#clion-setup
```

Configure, build, and test through the Nix-wrapped helpers:

```shell
./.nix/nix-cmake.sh \
  -DCMAKE_BUILD_TYPE=Debug \
  -G Ninja \
  -S . -B cmake-build-debug

./.nix/nix-cmake.sh --build cmake-build-debug
./.nix/ctest --test-dir cmake-build-debug -j
```

#### Use the development container
Build or reuse the local development image (installs the current user inside the container to avoid permission issues):

```shell
./scripts/install-local-docker-environment.sh
```

Configure, build, and test by mounting the repository into the container:

```shell
docker run \
  --workdir $(pwd) \
  -v $(pwd):$(pwd) \
  nebulastream/nes-development:local \
  cmake -B cmake-build-debug

docker run \
  --workdir $(pwd) \
  -v $(pwd):$(pwd) \
  nebulastream/nes-development:local \
  cmake --build cmake-build-debug -j

docker run \
  --workdir $(pwd) \
  -v $(pwd):$(pwd) \
  nebulastream/nes-development:local \
  ctest --test-dir cmake-build-debug -j
```

### Execute Queries & Tests

#### Run systest for a query
Build the `systest` executable in your chosen build directory and execute a specific query. The example below targets the first query in the arithmetic function suite and stores artifacts in `cmake-build-debug/systest-run`:

```shell
cmake --build cmake-build-debug -j --target systest
cmake-build-debug/systest/systest -t nes-systests/function/arithmetical/FunctionAdd.test:1
```

Append worker configuration overrides after `--` (for example `-- --worker.default_query_execution.execution_mode=INTERPRETER`). See the [systest guide](docs/development/systests.md) for authoring custom `.test` files and the complete CLI reference.

#### Start the client with a worker
Build the client and worker binaries, start the worker in one terminal, and submit a short-lived query from another terminal using `nes-repl`:

```shell
cmake --build cmake-build-debug -j --target nes-single-node-worker nes-repl

# Terminal 1: start the worker (listens on localhost:8080 by default)
cmake-build-debug/nes-single-node-worker/nes-single-node-worker

# Terminal 2: prepare a tiny CSV and submit a query via nes-repl
printf '1\n2\n3\n' > demo-input.csv

cat > demo.sql <<'EOF'
CREATE LOGICAL SOURCE demo(value UINT64);
CREATE PHYSICAL SOURCE FOR demo TYPE File SET('./demo-input.csv' AS `SOURCE`.FILE_PATH, 'CSV' AS PARSER.`TYPE`, '\n' AS PARSER.TUPLE_DELIMITER, ',' AS PARSER.FIELD_DELIMITER);
CREATE SINK result(demo.value UINT64) TYPE File SET('./demo-output.csv' AS `SINK`.FILE_PATH, 'CSV' AS `SINK`.INPUT_FORMAT);
SELECT value FROM demo INTO result;
EOF

cmake-build-debug/nes-frontend/apps/nes-repl -s localhost:8080 < demo.sql
```

The generated CSV appears at `demo-output.csv`. Inspect or retire the query with additional `nes-cli` commands such as `status` or `stop` as needed.

For further information about our frontends, check out the [Frontend Reference](docs/nebulastream-frontend.md).

## Documentation
- Design proposals and architectural notes: [Design index](docs/design/README.md)
- Developer workflows and environment setup: [Development environment](docs/development/development.md), [Run workflows locally](docs/running_workflows_locally.md)
- Technical deep dives: [Dependency architecture](docs/technical/dependency.md), [Query engine task queue](docs/technical/QueryEngine_TaskQueue.md), [Watermarking trigger details](docs/technical/watermarking_progress_window_triggering.md)
- Organizational guidelines and processes: [Meetings overview](docs/organizational/meetings.md), [Nightly CI process](docs/organizational/processes/nightly_ci.md)

## Contributing & Quality
- Follow the [development guide](docs/development/development.md) for environment setup and tooling expectations.
- Code style is enforced with `clang-format`, `clang-tidy`, license checks, and pragma guards; run the `format` target before submitting patches.
- Review the Git workflow aids in [Useful git commands](docs/git/useful_commands.md), [PR checklist](docs/git/checklist_pr.md), and IDE tips in [CLion tricks](docs/git/clion_tricks.md).
- External contributors should work from personal forks and open pull requests referencing their forked branches; direct pushes to this repository are reserved for maintainers.
- Adhere to our [Code of Conduct](CODE_OF_CONDUCT.md).

### Build Types
- Debug - Compiles all logging and keeps asserts enabled; best during active development.
- RelWithDebInfo - Keeps warning-level logging and asserts for balanced debugging/performance.
- Release - Optimizes for throughput with error-level logging. Asserts are enabled.
- Benchmark - Strips logging and assertions for maximum performance; use only with well-tested queries because undefined behavior is not guarded.
