# Bridge

The bridge connects the Rust coordinator with NebulaStream’s C++ components.  
It allows calls in both directions and translates the information exchanged between them.  
It connects three parts of the system: C++ frontends, the C++ SQL planner, and embedded C++ workers.  
For the overall architecture and request flow, see the [coordinator README](../README.md).  

## Frontend Access

The bridge provides C++ frontends with an interface to the Rust coordinator.  
Through this interface, a frontend can start a coordinator, submit SQL statements, and receive their results.  
Results include the statement’s output and, when a query is created, information about that query.

The coordinator’s lifetime is tied to the handle exposed through the bridge.  
Releasing the coordinator handle begins shutdown of its Rust runtime.

## Planning

The Rust coordinator uses the bridge to invoke the C++ SQL planner.  
The coordinator makes the planner once at start, with the optimizer settings and the default host, and an invalid optimizer option fails the start.  
For each SQL statement, the request handler reads a snapshot of the catalog in the statement's transaction and passes it to the planner with the SQL text.  
The snapshot holds the logical and shared physical sources, the shared sinks, the models, the active workers, and the network links between them.  
The planner reads nothing else, so no transaction and no catalog call crosses the language boundary.

The planner returns the statement in a form the coordinator can use.  
For queries, the returned information includes the execution fragments, their assigned workers, and the sources and sinks used by the query.  
The optimizer also creates sources and sinks that a query defines inline, and those of the network channels between workers.  
The planner gives them the next identifiers of the snapshot and returns them, and the coordinator inserts them together with the query.  
An EXPLAIN returns its plan only, so it leaves the catalog unchanged.

See the [model README](../model/README.md) for the catalog’s operations and transaction model.

## Embedded Workers

For embedded deployments, the bridge connects the Rust controller to C++ workers running in the same process.  
It provides access to worker creation and the operations needed to start, stop, and inspect query fragments.  
The bridge passes the controller’s start, stop, and status requests to the embedded C++ worker, then returns the worker’s results to the controller.  
The task hierarchy and worker communication behavior are described in the [controller README](../controller/README.md).

## Results and Errors

Rust and C++ use different representations for statements, query plans, catalog snapshots, and worker status.  
The bridge translates these representations as information crosses the language boundary.  
It also carries error information between the two sides so callers can identify and handle failures.  

## Testing

The planning tests plan their setup SQL and build a catalog snapshot from the planned statements, so they run the C++ planner without a coordinator.  
The coordinator tests start a coordinator through the bridge and submit SQL to it, as a frontend does.
