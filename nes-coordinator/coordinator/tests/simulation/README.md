# Coordinator Simulation Tests

The simulation tests check how the coordinator behaves as workers and queries change over time.  
They create repeatable scenarios that include normal operation and disruptions such as worker failures and network problems.  
After each scenario, the tests check whether the coordinator and workers reach the expected state.  
They do not check the data produced by queries.  

The scenarios are defined in the `configs` directory.  
A failed run reports a seed that can be used to repeat the same scenario.