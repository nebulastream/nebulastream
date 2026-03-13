# Recording Selection Frozen Corpus: XLarge Performance-Only Merged-Query DAG Mix Seeds 16-79

Generated on 2026-03-13 from `/home/adrian/workspace/nes-review`.

## Command

```bash
./cmake-build-debug/nes-benchmark/recording-selection-benchmark \
  --scenario-family dag-mix \
  --corpus-tier performance-only \
  --dag-families merged-pipeline,merged-union,merged-join \
  --dag-family-weights 5,4,2 \
  --network-topology random-connected \
  --corpus-only \
  --corpus-output nes-benchmark/out/recording-selection-dag-performance-only-xlarge-merged-query-corpus-seeds16-79.json \
  --first-seed 16 \
  --seed-count 64 \
  --min-fanout 16 \
  --max-fanout 40 \
  --hosts 12
```

## Effective Configuration

- benchmark: `recording-selection-benchmark`
- corpus mode: `--corpus-only`
- scenario family: `dag-mix`
- corpus tier: `performance-only`
- dag families: `merged-pipeline, merged-union, merged-join`
- dag family weights: `5, 4, 2`
- network topology: `random-connected`
- output: `nes-benchmark/out/recording-selection-dag-performance-only-xlarge-merged-query-corpus-seeds16-79.json`
- graph model:
  every scenario is a merged multi-query DAG with multiple real sink roots under a synthetic super-root
  synthetic super-root edges are plan edges but do not expose recording candidates
- tree height: `2` (retained only as benchmark metadata)
- min fanout / DAG size parameter: `16`
- max fanout / DAG size parameter: `40`
- first seed: `16`
- seed count: `64`
- worker hosts: `12` configured, effective per-instance range `4..13`
- quantile selection rule:
  performance-only tier emits one frozen row per solver-feasible deterministic heuristic profile:
  `performance-tight`, `performance-medium`, `performance-wide`, and `performance-relaxed`
  budgets are interpolated between a relaxed maintenance-optimal reference and a replay-tight feasible reference
- quantile values actually used in this corpus: `0.25, 0.45, 0.70, 0.85`

## Result Summary

- schema version: `3`
- instance count: `3708`
- unique base shapes: `1539`
- seed range: `16..79`
- size-parameter range: `16..40`
- host-count range: `4..13`
- node-count range: `18..58`
- median node count: `38`
- median host count: `8`
- baseline modes:
  - `performance-only`: `3708`
- scenarios:
  - `dag-merged-pipeline-recording-selection`: `1516`
  - `dag-merged-union-recording-selection`: `1144`
  - `dag-merged-join-recording-selection`: `1048`
- corpus buckets:
  - `performance-tight`: `1499`
  - `performance-medium`: `1129`
  - `performance-wide`: `564`
  - `performance-relaxed`: `516`
- constraints bind:
  - `true`: `1804`
  - `false`: `1904`
- binding by bucket:
  - `performance-tight`: `1023/1499` bind
  - `performance-medium`: `653/1129` bind
  - `performance-wide`: `88/564` bind
  - `performance-relaxed`: `40/516` bind

## Integrity

- SHA-256: `ad193d8f4163bfe8da4c7ca10770d233b7d0b1309e645a9467e80ca115eadb6d`
