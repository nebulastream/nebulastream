## Purpose of the Change and Brief Change Log
**(for example:)** 
This pull request makes task deployment go through the gRPC server, rather than through CAF. That way we avoid CAF's problems them on each deployment.
- *The TaskInfo is stored in the RPC message*
- *Deployments RPC transmits the following information:....*
- *NodeEngine does the following: ...*

## Verifying this change
This change is tested by
*(for example:)*
- *Added integration tests for end-to-end deployment with large payloads (100MB)*
- *Extended integration test for recovery after master failure*
- *Added test that validates that TaskInfo is transferred only once across recoveries*
- *Manually verified the change by running a 4 node cluser with 2 Coordinators and 4 NodeEngines, a stateful streaming program, and killing one Coordinators and two NodeEngines during the execution, verifying that recovery happens correctly.*

## Does this pull request potentially affect one of the following parts/components:
*(remove inapplicable items. Feel free to state if you are unsure about potential impact.)*

- Dependencies (does it add or upgrade a dependency)
- Deployment of the NodeEngine (e.g. its configuration)
- Deployment of the Coordinator (e.g. its configuration)
- The query manager
- The runtime per-record code paths (performance sensitive)
- The network stack

## Documentation
- The change is reflected in the documentation.
- All necessary methods (no getter, setter, or constructor) have a documentation.

## Issue Closed by this pull request:

This PR closes #<issue number>
