# Statistic coordinator: open points

What remains after the fixes to collecting, watching and deregistering statistics on
`statistic-coordinator-port-rebase` (`e803f2fcd7`, `f96cb774c7`, `22c81a2344`, `d9324c7a5f`).

## Problems

- [ ] **A submit that times out leaves its query running untracked.**
  The statistic service submits a statistic's query and waits up to 30 s for it to run
  (`nes-coordinator/statistics/src/hosting.rs:32`, `:73`). When the wait times out, the coordinator answers with
  `WaitTimeout` but keeps the query (`nes-coordinator/coordinator/src/request_handler.rs:395`, `:410`), and the error
  names no query. The service treats the timeout as a failed deployment and frees the key, so the query comes up
  unregistered and the next collect for the key deploys a second one.
  Options:
  - Let the coordinator's timeout error carry the query id, so the service can stop the query. *(recommended:
    small change)*
  - Submit without waiting and poll until the query runs; stop it on timeout.
  - Let the coordinator drop a create whose wait timed out. This changes the behaviour for every client.

- [ ] **Deregistering forgets a statistic only on the coordinator.**
  The worker's statistic store is a process-wide static without retention
  (`nes-statistics/src/StatisticStore/AbstractStatisticStore.cpp:24`). The store operator handler's `stop` does
  nothing (`nes-physical-operators/include/Statistics/StatisticStoreOperatorHandler.hpp:32`), and nothing outside the
  store calls `deleteStatistics` (`nes-statistics/include/StatisticStore/AbstractStatisticStore.hpp:47`). Every window
  of a deregistered statistic stays in the worker's memory.
  Options:
  - The writer's handler deletes its statistic's windows when its query stops. Worker-local, no new RPC.
    *(recommended)*
  - The coordinator sends an explicit delete to the worker after stopping the query.

  Related, probably a separate issue: running statistics grow without bound as well, since the store has no
  retention at all.

## Smaller gaps

- [ ] **Disconnected watchers linger.** A watch's forwarding task (`nes-coordinator/statistics/src/grpc.rs:266`)
  notices that its client is gone only when it next sends. For a statistic whose condition rarely holds, dead watchers
  and their tasks pile up until it reports or is deregistered.
- [ ] **A panicking stop blocks the key.** If the task stopping a deregistered statistic's query panics
  (`nes-coordinator/statistics/src/service.rs:486`), the statistic stays in the stopping state, and every later collect
  or deregister for its key is refused. The deployment task has a guard for this (`ReservationGuard`, `service.rs:156`);
  the stop task needs one too.

## Verification

- [ ] **Build and run the C++ side.** The `nes-statistic-cli` change (`nes-frontend/statistic-cli/StatisticCli.cpp`)
  is only syntax-checked against the generated code, and the REPL tests driving the CLI
  (`nes-frontend/repl/tests/embedded.bats`: collect, get, deregister, watch) have not run against these commits.

## Accepted for now

- **One shape per statistic.** A statistic's key is its metric, domain and window size. Collecting it with another
  condition, slide or time characteristic is refused, so two consumers cannot use different conditions on one
  statistic at once. Lifting that means either per-watcher conditions evaluated on the coordinator, or one build query
  with a reporting branch per condition on the worker.
- **Status codes are uneven.** A get for a statistic that is still deploying answers "statistic key is not
  registered", and an unknown key is `FAILED_PRECONDITION` for a get but `NOT_FOUND` for a watch
  (`grpc.rs:146`, `:261`).
- **Unused trigger parameter.** No caller passes a trigger to `collect_new_statistic` (`service.rs:242`).
- **A pending skip count can be lost.** If a statistic is deregistered while a slow watcher has missed reports, the
  watcher's stream ends without learning how many.
