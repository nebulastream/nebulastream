# Request Handler

The request handler processes requests submitted to the coordinator and controls when their results are returned.  
For the surrounding request flow, see the [coordinator README](../../../README.md).

## Completing a Request

A request contains an operation and an optional wait condition.  
The operation determines the result, while the wait condition determines when that result is returned.  
Wait conditions apply only to compatible results.  
If a condition does not apply to a result, the result is returned without waiting.  

## Waiting for Queries

The terminal query states are:

- `Completed`
- `Stopped`
- `Failed`

### Waiting for a State

A caller can wait for one or more queries to reach a specified lifecycle state, such as `Running`.  
The handler returns once every query has reached that state, progressed beyond it, or entered a terminal state.  
If a newly created query stops or fails before reaching the specified state, the request returns an early-termination error.  

### Waiting for Termination

A caller can instead wait until every affected query has entered a terminal state.  
For a drop request, this delays the reply until all affected queries have finished.  
For a query retrieval request, it returns their final states rather than an intermediate state.

## Polling Query Status

A caller can poll a query to wait for an up-to-date status from its query fragments.  
The poll completes after every active fragment has reported its status following the request.  
Fragments that have already terminated, or whose workers are unavailable, do not prevent the poll from completing.

## Timeouts

A caller can limit how long the request handler waits.

If the queries have not reached the requested state or terminated before that time expires, the request returns a timeout error.  
The error includes the latest observed result when available.  

A status poll behaves differently.  
If its time limit expires, the latest available status is returned successfully.