|                                              | Current Status                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | Processes that should happen                                                                                                                                                                                                                                                                                                                                                                                                              |
|----------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| <b>Stop</b> Query Request                    | Remove Query from Global Query Plan                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | Remove query from global query plan <br> Retain query for <b>Restart</b>                                                                                                                                                                                                                                                                                                                                                                  |
| <b>Fail</b> Query Request                    | Remove Query from Global Query Plan                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | Remove query from global query plan <br> Retain query for <b>Restart</b>                                                                                                                                                                                                                                                                                                                                                                  |
| Run Query Request (<b>Add</b> Query Request) | Add Updated Query Plan (Input Query Plan) <br> Update Query Status (Optimizing)<br> Type Inference Phase for Query Plan <br>Memory Layout Phase for Query Plan<br>Query Rewrite Phase for Query Plan<br>Add Updated Query Plan (Query Rewrite Phase)<br>Type Inference Phase for Query Plan <br> Signature Inference Phase <br> Topology specific query rewrite phase <br> Add updated Query Plan (Topology Specific Query Rewrite Phase) <br> Type Inference Phase <br> Origin Id Inference Phase <br> Memory Layout Phase <br> Add Update Query Plan (Execute Query Plan) <br> Add Query Plan to Global Query Plan <br> | Add query to Global Query Plan <br> Go through query optimization steps: <br> Set Query Status to Optimizing, <br> Type Inference Phase, <br> Memory Layout Phase, <br> Query Rewrite Phase, <br> Signature Inference Phase, <br> Topology specific Query Rewrite Phase, <br> Origin Id Inference Phase, <br> Add Updated Query Plan (Execute Query Plan) <br> Add final Query Plan to Global Query Plan <br> Set Query Status to Running |
| <b>Update</b> Query Request                  |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | Check if query is running <br> <b>Stop</b> Query Request <br> <b>Add</b> Query to Global Query Plan                                                                                                                                                                                                                                                                                                                                       |
| <b>Delete</b> Query Request                  |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | If running, trigger <b>Stop</b> Query Request <br> Remove query entry from query Catalog                                                                                                                                                                                                                                                                                                                                                  |
| <b>Restart</b> Query Request                 |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | Check if query is not running <br> Trigger <b>Add</b> Query Request                                                                                                                                                                                                                                                                                                                                                                       |

<b>Stop Request</b>

```mermaid
sequenceDiagram

    Request Queue ->> Executor: Stop Query Request
    Executor ->> Request Queue: Remove Stop Request
    Executor ->> Query Catalog Service: Obtain query status
    Query Catalog Service -->> Executor: Return query status
    alt Registered || Optimizing || Deployed || Running || Restarting || <br> Migrating || MarkedForSoftStop || SoftStopTriggered || SoftStopCompleted
        Executor ->> Query Catalog Service: set Status MarkedForHardStop
        Executor ->> Global Query Plan: Remove Query
        alt ok
            Global Query Plan -->> Executor: Query Successfully removed
            Executor ->> Undeployment Phase: Trigger undeployment of involved query operations
            Undeployment Phase -->> Executor: Successfully undeployed query
            Executor ->> Query Catalog Service: Set status Stopped
        else Error1 
            Global Query Plan -->> Executor: Query ID not found in SQP
            Undeployment Phase -->> Executor: Query ID not found in SQP
        else Error2 
            Global Query Plan -->> Executor: Could not remove Query Operators
            Undeployment Phase -->> Executor: Could not remove Query Operators
        end
    else MarkedForHardStop || Stopped 
        Executor ->> Executor: Query already stopped, no further action needed
    else Error
        Executor ->> Query Catalog Service: Set status MarkedForFailure
        Executor ->> Request Queue: Add Fail Request
    end
```

Currently, failure to remove a query just throws errors. 

<b>Fail Request</b>

```mermaid
sequenceDiagram

    Request Queue ->> Executor: Fail Query Request
    Executor ->> Request Queue: Remove Fail Request
    Executor ->> Undeployment Phase: Trigger undeployment of involved <br> query operations with Failure Flag
    alt ok
        Undeployment Phase -->> Executor: Succesfull undeployment
    else QueryUndeploymentException
        Undeployment Phase -->> Executor: Failed to undeploy query
    end
    Executor ->> Query Catalog Service: Set status Failed
```

What todo when undeployment failed? Retry request n times and then fail 
//todo update with appropriate status

if entry not found, don't do anything global query plan
if entry in global execution plan, remove it

if failure for sqp, fail entire sqp

<b>Update Request</b>

```mermaid
sequenceDiagram

Request Queue->>Executor: Update Query Request
Executor ->> Request Queue: Remove Update Request
Executor->>Query Catalog Service: set Status Updating
Executor->>Executor: Execute Stop Query Request
alt ok
    Executor->>Executor: Execute Add Query Request
else Error
    Executor->>Query Catalog Service: set Status Failed
    Executor->>Request Queue: Add Fail Request
end
```

<b>Delete Request</b>

```mermaid
sequenceDiagram

    Request Queue ->> Executor: Delete Query Request
    Executor ->> Query Catalog Service: Obtain query status
    Query Catalog Service -->> Executor: Return query status
    opt Registered || Optimizing <br> || Deployed || Running <br>|| Restarting || Migrating <br>|| MarkedForSoftStop <br>|| SoftStopTriggered <br>|| SoftStopCompleted
        Executor ->> Executor: Execute Stop Query Request
    end
    Executor ->> Query Catalog Service: Delete query entry
    Query Catalog Service -->> Executor: Successfully deleted query
```

<b>Restart Request</b>

```mermaid
sequenceDiagram

    Request Queue ->> Executor: Restart Query Request
    Executor ->>Query Catalog Service: Obtain query status
    Query Catalog Service -->> Executor: Return query status
    opt Registered || Optimizing <br> || Deployed || Running <br>|| Restarting || Migrating <br>|| MarkedForSoftStop <br>|| SoftStopTriggered <br>|| SoftStopCompleted
        Executor ->> Executor: Execute Stop Query Request
    end
    Executor->>Query Catalog Service: Set Status to Restarting
    Executor->>Executor: Execute Add Query Request
```

<b>Add Request</b>

```mermaid
sequenceDiagram

    Request Queue ->> Executor: Add Query Request
    Executor ->> Query Catalog Service: Add Query, <br> set Status Registered
    Query Catalog Service -->> Executor: Successfully registered query
    Executor ->> Query Catalog Service: Set Status to Optimizing
    Executor ->> Optimizer: Start Optimization Process: Type Inference Phase, Memory Layout Phase, <br> Query Rewrite Phase, Signature Inference Phase
    Optimizer -->> Executor: Optimized Query Plan
    Executor ->> Query Catalog Service: Add updated query plan
    Executor ->> Global Execution Plan: Topology specific Query Rewrite Phase
    Global Execution Plan -->> Executor: Topology specific Query Plan
    Executor->>Query Catalog Service: Update query plan
    Executor->>Executor: Execute Type Inference, <br> Origin Id Inference, <br> and Memory Layout Phases <br> for new query Plan
    Executor->>Query Catalog Service: Update query plan <br> and set status to Deployed
    Executor ->> Global Query Plan: Add query plan
    Global Query Plan -->> Executor: Successfully added new Query
    Executor ->> Query Catalog Service: Set Status to Running

```
Not sure if deployed is right?
Error handling still mostly missing
