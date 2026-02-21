# NebulaStream Development Guide

## Prerequisites

These instructions require the devcontainer environment. Check that `NES_DEVCONTAINER=1` is set. If it is not, ask the user to open the project in the devcontainer before proceeding with build or test commands.

## Building

Configure and build using CMake presets. The first configure will fetch and build vcpkg dependencies (cached across rebuilds via Docker volumes). See `CMakePresets.json` for all available presets.

```bash
cmake --preset debug                          # Configure
cmake --build --preset debug                  # Build
cmake --build --preset debug --target <name>  # Build specific target
```

## Formatting

```bash
cmake --build --preset debug --target format
```

## Testing

```bash
ctest --preset debug              # Run all tests
ctest --preset debug -R <regex>   # Run tests matching a pattern
ctest --preset debug -j$(nproc)   # Run tests in parallel
```

## System Tests

System tests are declarative `.test` files in `nes-systests/` that test queries end-to-end. The `systest` binary is built as part of the project. See `docs/development/systests.md` for the full reference on writing and running system tests.

```bash
cmake-build-debug/nes-systests/systest -t nes-systests/operator/projection/Projection.test   # Run a test file
cmake-build-debug/nes-systests/systest -g Projection                                         # Run all tests in a group
cmake-build-debug/nes-systests/systest -g Projection -e large                                # Exclude a group
cmake-build-debug/nes-systests/systest -l                                                    # List all tests and groups
cmake-build-debug/nes-systests/systest -d -t <file>                                          # Debug mode with query plan dumps
```

## Logs

Systest writes detailed debug logs to a separate log file in `cmake-build-debug/nes-systests/`. A `latest.log` symlink always points to the most recent run. The terminal output only shows test results — check the log file for full engine output when debugging failures.

## Running the System

### Single-Node (Embedded REPL)

The easiest way to run NebulaStream is with the embedded REPL, which starts a single-node worker internally:

```bash
cmake-build-debug/nes-nebuli/nes-repl-embedded
```

Basic workflow inside the REPL:

```sql
CREATE LOGICAL SOURCE stream(id UINT64, value UINT64);
CREATE PHYSICAL SOURCE FOR stream TYPE Generator SET('CSV' as PARSER.`TYPE`, 'SEQUENCE UINT64 0 100 1' AS `SOURCE`.GENERATOR_SCHEMA);
CREATE SINK output(stream.id UINT64) TYPE File SET('out.csv' as `SINK`.FILE_PATH, 'CSV' as `SINK`.INPUT_FORMAT);
SELECT id FROM stream INTO output;
SHOW QUERIES;
DROP QUERY WHERE ID = '<query-id>';
```

### Distributed Mode

Start a worker separately, then connect with the distributed REPL or CLI:

```bash
# Terminal 1: Start a worker
cmake-build-debug/nes-single-node-worker/nes-single-node-worker --grpc=localhost:8080

# Terminal 2: Connect with REPL
cmake-build-debug/nes-nebuli/nes-repl -s localhost:8080
```

### CLI (One-Shot Commands)

The CLI operates on YAML topology files for non-interactive use:

```bash
cmake-build-debug/nes-nebuli/nes-cli -t topology.yaml dump              # Validate and print topology
cmake-build-debug/nes-nebuli/nes-cli -t topology.yaml start             # Start query from topology
cmake-build-debug/nes-nebuli/nes-cli -t topology.yaml status <query-id> # Check query status
cmake-build-debug/nes-nebuli/nes-cli -t topology.yaml stop <query-id>   # Stop a query
```

See `docs/nebuli-frontend-reference.md` for the full frontend reference.
