# The Problem
- describe the current state of the system
- explain why the system's current state requires a change
- specify the concrete problems and enumerate them as P1, P2, ...

As a user, I would like to be able to make requests to NebulaStream from a client over the network.
NebulaStream had a REST API and some gRPC services available to make requests from a client, but NebulaStream public has removed some of these. The old endpoints were removed for various reasons, and now the following service is all that remains supported:
```
service WorkerRPCService {
  rpc RegisterQuery (RegisterQueryRequest) returns (RegisterQueryReply) {}
  rpc UnregisterQuery (UnregisterQueryRequest) returns (google.protobuf.Empty) {}

  rpc StartQuery (StartQueryRequest) returns (google.protobuf.Empty) {}
  rpc StopQuery (StopQueryRequest) returns (google.protobuf.Empty) {}

  rpc RequestQuerySummary (QuerySummaryRequest) returns (QuerySummaryReply) {}
  rpc RequestQueryLog (QueryLogRequest) returns (QueryLogReply) {}
}
```
- P1: From this service, it is hard to inspect much of the running system without existing knowledge of the state. Most of these requests require a queryId as one of the properties of the Request objects that are passed. Without being the one who started the query, existing queryIds are not known to a client. 
- P2: This service does not provide clear usage documentation. While the intention of some of the requests are clear from the name, RequestQuerySummary and RequestQueryLog are rather open to interpretation. The best example of this is the QueryLog, since even when looking at the properties of the returned object, it is not clear as to what the timestamps mean, or the expected Error Messages.
- P3: Another challenge worth noting, is that when using gRPC, clients are harder to debug since the messages are serialized and hard to interpret until passed to the correct deserializer.
- P4: It is not possible to meet the following needs of the NEEDMI use case without endpoints for the following:
    1. A healthcheck / heartbeat.
    1. List all existing sources and sinks.
    1. Add/Remove a Tee for all events from a source or sink on demand for visuals in the UI.
    1. Retrieving the logical query plan for any query as submitted and after optimization.
    1. Add/Remove a Tee at any logical operator of a query to inspect events at that point.
    


# Goals
- describe goals/requirements that the proposed solution must fulfill
- enumerate goals as G1, G2, ...
- describe how the goals G1, G2, ... address the problems P1, P2, ...

- G1: Extend the available endpoints to make them usable to a client that knows nothing about the system state can first learn enough about system state in order to make all possible requests. This should solve P1
- G2: Create documentation for all endpoints to explain their use-cases to sole P2.
- G3: Provide a way to generate client code for the endpoints without checking out the NebulaStream repo.

# Non-Goals
- list what is out of scope and why
- enumerate non-goals as NG1, NG2, ...

# (Optional) Solution Background
- describe relevant prior work, e.g., issues, PRs, other projects

# Our Proposed Solution
- start with a high-level overview of the solution
- explain the interface and interaction with other system components
- explain a potential implementation
- explain how the goals G1, G2, ... are addressed
- use diagrams if reasonable

# Proof of Concept
- demonstrate that the solution should work
- can be done after the first draft

# Alternatives
- discuss alternative approaches A1, A2, ..., including their advantages and disadvantages

# (Optional) Open Questions
- list relevant questions that cannot or need not be answered before merging
- create issues if needed

# (Optional) Sources and Further Reading
- list links to resources that cover the topic

# (Optional) Appendix
- provide here nonessential information that could disturb the reading flow, e.g., implementation details
