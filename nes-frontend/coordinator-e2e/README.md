# Coordinator e2e tests for the review of #1577

End-to-end tests from a user's seat for `ys/coordinator-bridge` (#1577). Workers are real `nes-single-node-worker`
processes on localhost; the coordinator is reached only through `nes-repl` and `nes-cli`; every verdict is
something a user can see (statement output, exit code, rows in a file sink).

This branch is one commit on top of `1cb7ad0730` and adds only this directory. It is not wired into CMake or CI.

| File | What it holds | On `1cb7ad0730` |
|---|---|---|
| `findings.bats` | one test per finding of the review, numbered the same; each asserts what we would expect | all 14 fail; a failing test prints what happened |
| `works.bats` | what held up: exact results over 3 workers, a worker that comes up late, `DROP QUERY` / `DROP WORKER`, concurrent `nes-cli` | all 11 pass |
| `lib.bash` | starting and killing workers, SQL and setup snippets, waiting on sink files | |

## Running

In the NES development container:

```sh
cmake --build <build-dir> --target nes-single-node-worker nes-repl nes-cli
NES_BUILD=<build-dir> bats nes-frontend/coordinator-e2e
```

One finding only: `NES_BUILD=<build-dir> bats -f '\[1\]' nes-frontend/coordinator-e2e/findings.bats`.

- Each test gets its own temporary directory, its own catalog (`nes-repl --db`) and its own `XDG_STATE_HOME`, so
  `nes-cli` never touches `~/.local/state/nebucli`.
- Workers listen on localhost ports from 20100 upwards, 100 per test. Run the files one after the other, not with
  `bats -j`.
- Needs bats >= 1.5, jq and GNU coreutils, all in the development image.
