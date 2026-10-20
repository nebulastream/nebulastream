# Bridge

The bridge connects the Rust coordinator with NebulaStream’s C++ components.  
It allows calls in both directions and translates the information exchanged between them.  
It connects four parts of the system: C++ frontends, the C++ SQL planner, the Rust catalog, and embedded C++ workers.  
For the overall architecture and request flow, see the [coordinator README](../README.md).  

## Frontend Access

The bridge provides C++ frontends with an interface to the Rust coordinator.  
Through this interface, a frontend can start a coordinator, submit SQL statements, and receive their results.  
Results include the statement’s output and, when a query is created, information about that query.

The coordinator’s lifetime is tied to the handle exposed through the bridge.  
Releasing the coordinator handle begins shutdown of its Rust runtime.

## Planning

The Rust coordinator uses the bridge to invoke the C++ SQL planner.  
The bridge passes the SQL statement and planning settings to the planner, then returns the planned statement in a form the coordinator can use.  
For queries, the returned information includes the execution fragments, their assigned workers, and the sources and sinks used by the query.  
The bridge connects the two components while keeping planning decisions within the planner.

## Catalog Access During Planning

The C++ planner uses catalog information to understand a statement and decide where a query can run.  
Through the bridge, it can look up sources, sinks, models, workers, and network connections.

See the [model README](../model/README.md) for the catalog’s operations and transaction model.

## Embedded Workers

For embedded deployments, the bridge connects the Rust controller to C++ workers running in the same process.  
It provides access to worker creation and the operations needed to start, stop, and inspect query fragments.  
The bridge passes the controller’s start, stop, and status requests to the embedded C++ worker, then returns the worker’s results to the controller.  
The task hierarchy and worker communication behavior are described in the [controller README](../controller/README.md).

## Results and Errors

Rust and C++ use different representations for statements, query plans, catalog entries, and worker status.  
The bridge translates these representations as information crosses the language boundary.  
It also carries error information between the two sides so callers can identify and handle failures.  

## Testing

The bridge provides support for testing the C++ planner against the Rust catalog without starting a complete coordinator.

These tests use the same catalog interface as normal planning, allowing them to check that planning and catalog access work together across the language boundary.