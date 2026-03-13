# NES Benchmark

`nes-benchmark` contains reproducible experiment setups that exercise individual optimizer and replay components without requiring a full system test.

## Recording Selection

`recording-selection-benchmark` now supports two frozen corpus tiers:

- `exact`: deterministic trees and small irregular DAGs that still fit exhaustive cut enumeration.
- `performance-only`: larger irregular DAGs that keep deterministic budgets and solver metrics but skip exact optimality enumeration.

In both modes, the benchmark freezes per-instance budgets into a deterministic corpus and runs the production `RecordingBoundarySolver` on that corpus.

The default workflow remains intentionally exact instead of large: the exhaustive baseline becomes intractable quickly, so the default corpus still uses height-2 k-ary trees and sweeps fan-out from `2` to `16`. For `--scenario-family dag-mix`, add `--corpus-tier mixed` to emit both the small exact-DAG tier and the larger performance-only tier in the same frozen corpus. The performance-only tier now preserves all four heuristic budget regimes per base DAG when they are solver-feasible: `performance-tight`, `performance-medium`, `performance-wide`, and `performance-relaxed`. Those budgets are derived from both a relaxed maintenance-optimal reference and a replay-tight feasible reference so tighter buckets actually exercise binding replay constraints instead of replaying the unconstrained solution.

The `dag-mix` generator can now be restricted to specific workload families with `--dag-families` and weighted with `--dag-family-weights`. Available families are `diamond`, `join`, `pipeline`, `union`, `multi-join`, `join-union-hybrid`, `shared-subplan`, `branching-pipeline`, `reconverging-fanout`, `merged-pipeline`, `merged-union`, and `merged-join`. Host routes can be generated over `line`, `star`, `tree`, `ring`, or `random-connected` topologies via `--network-topology`.

The `merged-*` families build merged multi-query DAGs with multiple real sink roots under a synthetic super-root. Those synthetic root edges are part of the plan topology seen by the solver but deliberately do not expose recording candidates, which matches how the production recording-selection phase represents merged replay graphs.

### Paper-Style Plot

Use the emitted CSV to build a two-panel figure:

1. Main panel:
   x-axis: `feasible_cut_count`
   y-axis: normalized maintenance cost (`selected_over_best`, `avg_over_best`, `worst_over_best`)
   lines: `selected`, `average feasible cut`, `worst feasible cut`
2. Companion panel:
   x-axis: `feasible_cut_count`
   y-axis: `solver_time_ns_mean`

For the paper figure, aggregate rows with the same `feasible_cut_count` across seeds and plot the median. If desired, add a shaded interquartile band around each line. The bundled plotter uses a base-2 logarithmic x-axis so the wider feasible-cut sweep remains readable in a single-column figure.

The bundled plotter emits a serif PDF sized for a single-column paper figure and labels the two panels as `(a)` and `(b)`.

### Flake Workflow

Run the benchmark directly through the flake:

```bash
nix run .#recording-selection-benchmark -- \
  --output /tmp/recording-selection.csv
```

The flake app reuses the local `cmake-build-debug` tree through `./.nix/nix-cmake.sh`, so repeated runs stay incremental instead of rebuilding a full release package from scratch. By default it builds with `-j35`; override that with `NES_BENCHMARK_BUILD_JOBS` if needed.

The CSV contains one row per frozen corpus instance with solver timing plus corpus metadata such as `benchmark_mode`, `baseline_mode`, `corpus_bucket`, `budget_profile`, `scenario`, `host_count`, `candidate_options`, and `constraints_bind`. Exact-only columns such as `selected_over_best` and `optimal_hit` are populated only when `baseline_mode=exact`.

The default corpus remains the exact height-2 k-ary tree sweep. For an irregular workload mix that includes shared-leaf diamonds, joins, unions, longer pipelines, heterogeneous host counts, and multiple candidate options per edge, use `--scenario-family dag-mix`. Add `--corpus-tier mixed` to append the larger performance-only tier.

For shadow-price experiments, the benchmark can also sweep replay warm-start and replay step scales over the same frozen corpus. Sweep rows add `replay_initial_price_scale`, `replay_step_scale`, `storage_step_scale`, `shadow_price_iterations`, `solver_succeeded`, and `optimal_hit` columns.

For convergence experiments, the benchmark can emit one row per shadow-price iteration with replay and storage-budget satisfaction percentages for the currently selected cut.

Write the generated corpus to JSON so later benchmark runs reuse the exact same instances:

```bash
nix run .#recording-selection-benchmark -- \
  --corpus-output /tmp/recording-selection-corpus.json \
  --corpus-only \
  --seed-count 16 \
  --min-fanout 2 \
  --max-fanout 16
```

Generate a mixed irregular DAG corpus with both exact and performance-only tiers:

```bash
nix run .#recording-selection-benchmark -- \
  --scenario-family dag-mix \
  --corpus-tier mixed \
  --corpus-output /tmp/recording-selection-dag-corpus.json \
  --corpus-only \
  --seed-count 16 \
  --min-fanout 2 \
  --max-fanout 8 \
  --hosts 5
```

Generate a mixed DAG corpus that emphasizes richer join-heavy families on a non-line host topology:

```bash
nix run .#recording-selection-benchmark -- \
  --scenario-family dag-mix \
  --corpus-tier mixed \
  --dag-families multi-join,join-union-hybrid,shared-subplan \
  --dag-family-weights 3,2,1 \
  --network-topology random-connected \
  --corpus-output /tmp/recording-selection-dag-join-heavy.json \
  --corpus-only \
  --seed-count 16 \
  --min-fanout 2 \
  --max-fanout 8 \
  --hosts 6
```

Run the benchmark against that frozen corpus:

```bash
nix run .#recording-selection-benchmark -- \
  --corpus-input /tmp/recording-selection-corpus.json \
  --output /tmp/recording-selection.csv
```

The old adaptive setup is still available for comparison only:

```bash
nix run .#recording-selection-benchmark -- \
  --legacy-adaptive \
  --output /tmp/recording-selection-legacy.csv
```

Render the paper-ready figure through the flake so the plotting dependencies are pinned:

```bash
nix run .#recording-selection-plot -- \
  /tmp/recording-selection.csv \
  /tmp/recording-selection.pdf
```

For the mixed DAG corpus, keep the exact-only paper plot and group by topology family instead of feasible-cut count:

```bash
nix run .#recording-selection-plot -- \
  /tmp/recording-selection.csv \
  /tmp/recording-selection-topologies.pdf \
  --group-by scenario
```

Render the categorical scatter plot bucketed by feasible fraction:

- `tight`: `1%` to `<5%`
- `medium`: `5%` to `<20%`
- `loose`: `20%` to `50%`
- `wide`: `>50%`

Rows below `1%` feasible are omitted from this scatter plot and reported in a small footer on the figure.

```bash
nix run .#recording-selection-scatter-plot -- \
  /tmp/recording-selection.csv \
  /tmp/recording-selection-scatter.pdf
```

Or bucket the exact-only scatter by topology family:

```bash
nix run .#recording-selection-scatter-plot -- \
  /tmp/recording-selection.csv \
  /tmp/recording-selection-scatter-topologies.pdf \
  --group-by scenario
```

Render the shadow-price heatmap from a sweep CSV. By default the sweep focuses on `constraints_bind=1` rows so non-binding cases do not dominate the aggregate:

```bash
nix run .#recording-selection-benchmark -- \
  --corpus-input /tmp/recording-selection-corpus.json \
  --shadow-price-sweep \
  --output /tmp/recording-selection-shadow-price.csv

nix run .#recording-selection-shadow-price-plot -- \
  /tmp/recording-selection-shadow-price.csv \
  /tmp/recording-selection-shadow-price.pdf
```

The default sweep grid is:

- replay initial price scale: `0.25,0.5,1,2,4`
- replay step scale: `0.25,0.5,1,2,4`
- storage step scale: `1`
- shadow-price iterations: `32`

Override any of those with `--replay-initial-price-scales`, `--replay-step-scales`, `--storage-step-scale`, and `--shadow-price-iterations`. Add `--include-nonbinding-scenarios` to the benchmark or `--include-nonbinding` to the plotter if you want the heatmap to include non-binding corpus rows.

Render the shadow-price convergence plot. This mode expects exactly one replay warm-start scale and one replay step scale, then aggregates replay and storage-budget satisfaction by iteration:

```bash
nix run .#recording-selection-benchmark -- \
  --corpus-input /tmp/recording-selection-corpus.json \
  --shadow-price-convergence \
  --replay-initial-price-scales 1 \
  --replay-step-scales 1 \
  --output /tmp/recording-selection-convergence.csv

nix run .#recording-selection-convergence-plot -- \
  /tmp/recording-selection-convergence.csv \
  /tmp/recording-selection-convergence.pdf
```

Render the per-instance visualization report. The benchmark writes a JSON artifact that preserves the topology, DAG, solver-selected boundary, and, for exact rows only, the exact baseline boundary. The report renderer turns that into a paginated grid PDF. Performance-only rows stay in the grid but omit the baseline panel.

```bash
nix run .#recording-selection-benchmark -- \
  --corpus-input /tmp/recording-selection-corpus.json \
  --report-output /tmp/recording-selection-report.json

nix run .#recording-selection-report -- \
  /tmp/recording-selection-report.json \
  /tmp/recording-selection-report.pdf
```

Or do both in one step:

```bash
nix run .#recording-selection-paper-figure -- \
  /tmp/recording-selection.pdf \
  --seed-count 16 \
  --min-fanout 2 \
  --max-fanout 16
```

Or generate the scatter plot directly from a fresh benchmark run:

```bash
nix run .#recording-selection-scatter-figure -- \
  /tmp/recording-selection-scatter.pdf \
  --seed-count 16 \
  --min-fanout 2 \
  --max-fanout 16
```

Or generate the shadow-price heatmap directly from a fresh sweep:

```bash
nix run .#recording-selection-shadow-price-figure -- \
  /tmp/recording-selection-shadow-price.pdf \
  --corpus-input /tmp/recording-selection-corpus.json
```

Or generate the convergence figure directly:

```bash
nix run .#recording-selection-convergence-figure -- \
  /tmp/recording-selection-convergence.pdf \
  --corpus-input /tmp/recording-selection-corpus.json \
  --replay-initial-price-scales 1 \
  --replay-step-scales 1
```

Or generate the visualization report directly from a benchmark run:

```bash
nix run .#recording-selection-report-figure -- \
  /tmp/recording-selection-report.pdf \
  --corpus-input /tmp/recording-selection-corpus.json
```
