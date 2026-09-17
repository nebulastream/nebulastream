# Coordinator Simulation Tests

The simulation tests check how the coordinator behaves as workers and queries change over time.  
They create repeatable scenarios that include normal operation and disruptions such as worker failures and network problems.  
After each scenario, the tests check whether the coordinator and workers reach the expected state.  
They do not check the data produced by queries.

The scenarios are defined in the `tests/simulation/configs` directory.  
The tests run only with the madsim cfg:

```sh
RUSTFLAGS="--cfg madsim --cfg tokio_unstable" cargo test -p simulation --test simulation
```

A failed run prints the command that repeats it with the same seed.
