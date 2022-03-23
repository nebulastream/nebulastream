
## What is the purpose of the change

*(For example: This pull request makes task deployment go through the gRPC server, rather than through CAF. That way we avoid CAF's problems them on each deployment.)*


## Brief change log

*(for example:)**
  - *The TaskInfo is stored in the RPC message*
  - *Deployments RPC transmits the following information:....*
  - *NodeEngine does the following: ...*


## Verifying this change

*(Please pick either of the following options)*

This change is a trivial rework / code cleanup without any test coverage.

*(or)*

This change is already covered by existing tests, such as *(please describe tests)*.

*(or)*

This change added tests and can be verified as follows:

*(example:)*
  - *Added integration tests for end-to-end deployment with large payloads (100MB)*
  - *Extended integration test for recovery after master failure*
  - *Added test that validates that TaskInfo is transferred only once across recoveries*
  - *Manually verified the change by running a 4 node cluser with 2 Coordinators and 4 NodeEngines, a stateful streaming program, and killing one Coordinators and two NodeEngines during the execution, verifying that recovery happens correctly.*

## Does this pull request potentially affect one of the following parts:

  - Dependencies (does it add or upgrade a dependency): (yes / no)
  - The compiler: (yes / no / don't know)
  - The threading model: (yes / no / don't know)
  - The Runtime per-record code paths (performance sensitive): (yes / no / don't know)
  - The network stack: (yes / no / don't know)
  - Anything that affects deployment or recovery: Coordinator (and its components), NodeEngine (and its components): (yes / no / don't know)

## Documentation

  - Does this pull request introduce a new feature? (yes / no)
  - If yes, how is the feature documented? (not applicable / docs / JavaDocs / not documented)

## Issue Closed by this pull request:

This PR closes #<issue number>
