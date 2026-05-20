---
name: verify-changes
description: Tiered build/test workflow for the NebulaStream codebase. Use when the user asks to verify, check, compile, build, or test changes. Pick the smallest tier that fits the change. Tier 1 (compile-check) builds the `systest` target. Tier 2 (functional) runs the systests. Tier 3 (final) runs the full ctest suite. Always use the `debug-none-local` preset (Debug, no sanitizer, libstdc++) with 8 parallel jobs unless the user requests otherwise.
---

# Verify changes (NebulaStream)

Three tiers, escalating in cost. **Pick the smallest tier that exercises the change** — do not skip up tiers without reason, and do not skip down past what's needed.

| Tier | Use when | Time |
|---|---|---|
| 1 — Compile | Quick "does it build?" check after any edit. | ~30 s incremental, minutes cold |
| 2 — Systests | Functional verification — covers most user-visible behaviour. | ~2.5 min |
| 3 — Full ctest | Final gate after a larger set of changes (refactor, multi-file feature, before PR). | ~2 min after build |

Always work from the repo root (`/workspaces/devcontainer1`). The build directory is `cmake-build-debug-none-local/`.

## Tier 1 — Compile check (`systest` target)

The `systest` executable transitively depends on most of the system, so it's a good single target for a "does this still compile?" check.

```bash
cmake --build cmake-build-debug-none-local --target systest -j 8
```

If configuration is missing, configure first:

```bash
cmake --preset debug-none-local
```

## Tier 2 — Run systests (functional verification)

Runs all systests except the `large` group (long-running benchmarks). Worker config: 4 threads, 4 KB buffers, 400 MB pool (102 400 buffers × 4 KB).

```bash
./cmake-build-debug-none-local/nes-systests/systest/systest \
  --exclude-groups large \
  -- --worker.query_engine.number_of_worker_threads=4 \
     --worker.number_of_buffers_in_global_buffer_manager=102400 \
     --worker.default_query_execution.operator_buffer_size=4096
```

Notes:
- The disable-config already excludes `large` by default; passing `--exclude-groups large` makes it explicit.
- Worker-config options after `--` use snake_case (`number_of_worker_threads`, not `numberOfWorkerThreads`) despite what `--help` shows.
- Buffer math: `number_of_buffers_in_global_buffer_manager × operator_buffer_size = total memory`. Adjust either side to retune.
- Test data path is baked in at configure time — `--data` is not needed.

## Tier 3 — Full ctest suite (final gate)

**Run Tier 2 first.** Tier 3 deliberately skips the `Systest`-labeled ctests because they just re-invoke the same `systest` binary that Tier 2 runs directly — running both is redundant and slow. So the workflow is: Tier 2 covers the systests, Tier 3 covers everything else.

Build everything, then run ctest with the same label filter CI uses:

```bash
cmake --build cmake-build-debug-none-local -j 8
ctest --test-dir cmake-build-debug-none-local -j 8 \
  -LE "Systest|DockerCompose" \
  --output-on-failure --timeout 2400
```

`-LE "Systest|DockerCompose"`:
- excludes 21 `Systest`-labeled wrapper tests (already covered by Tier 2)
- excludes 4 `DockerCompose`-labeled tests that need a Docker daemon and CI-only docker-in-docker plumbing

This mirrors `.github/workflows/build_and_test.yml:91`, where CI splits tests into three separate jobs: unit tests with this same `-LE` filter, then `-L Systest`, then `-L DockerCompose`.

## Choosing a preset

Default to `debug-none-local`: Debug, no sanitizer, libstdc++, amd64. Other presets exist in `CMakePresets.json` (`debug-address-libcxx`, `debug-thread-libcxx`, etc.) — only switch on explicit user request, since each preset has its own build directory and re-configures from scratch.

## Long-output tip

Both Tier 2 and Tier 3 produce hundreds of lines. For Tier 3, prefer running `ctest` in the background and using a Monitor that filters for `Failed|tests passed|tests failed|Total Test`. For Tier 2, the systest binary already prints a final `All queries passed.` / failure summary — `tail`-ing the last 100 lines is enough.
