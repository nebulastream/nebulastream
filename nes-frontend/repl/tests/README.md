# REPL Integration Tests

This directory contains integration tests for the NebulaStream REPL (`nes-repl`).

## Test Types

- **`.bats` files** (in `sql-file-tests/`): [BATS](https://github.com/bats-core/bats-core) (Bash Automated Testing System) tests. These run `nes-repl` non-interactively with SQL file input (`-f` flag) and assert on the JSON output. Each test spins up a Docker Compose cluster.

## Directory Structure

- `sql-file-tests/` -- BATS tests and SQL input files
- `topologies/` -- YAML topology definitions used by the tests
- `util/` -- Helper scripts (e.g., Docker Compose generation)
