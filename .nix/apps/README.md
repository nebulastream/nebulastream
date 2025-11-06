# Nix Distributed Usage

The Nix distributed runner uses a topology definition like this:

```yaml
workers:
  - host: "sink-node:9090"
    grpc: "sink-node:8080"
    capacity: 10000
  - host: "source-node:9090"
    grpc: "source-node:8080"
    capacity: 4
    downstream:
      - "sink-node:9090"
```

to launch local worker processes and run queries/tests against them.

In bash, you can directly invoke it like this (runs queries):

```bash
nix run .#nes-distributed -- --topology nes-systests/configs/topologies/two-node.yaml -- -g Join
```

This starts the workers (rewritten to 127.0.0.1), waits for health, runs the Join group of systests, and tears the cluster down.

Alternatively, run a specific test query only (e.g., Query 1 in JoinMultipleStreams):

```bash
nix run .#nes-distributed -- \
  --topology nes-systests/configs/topologies/two-node.yaml \
  -- -t nes-systests/operator/join/JoinMultipleStreams.test:1
```

By default, it picks a work directory under your local CMake build: `./cmake-build-*/nes-systests/working-dir` (e.g., `cmake-build-debug`).

Split lifecycle (optional): the one‑shot runner is recommended. You can keep the cluster up manually with `.#nes-distributed-up` and run and shutdown with `.#nes-distributed-run` and `.#nes-distributed-down`.

Disable rewriting with `--no-rewrite` if your topology already uses reachable addresses.
