# Request Handler

The request handler processes requests submitted to the coordinator and controls when their results are returned.  
For the surrounding request flow, see the [coordinator README](../README.md).

## Completing a Request

A create, drop, or other statement is applied to the catalog and its result is returned at once.  
Only a watch waits: a read of queries with a wait condition.  
A caller that wants to block on a create or drop sends the statement first and then a watch on the affected queries.  
A watch that matches no query returns at once.

## Wait Conditions

The terminal query states are `Completed`, `Stopped` and `Failed`.  
A watch waits for one of three conditions:

- **Running**: every query is running or has terminated.
- **Terminated**: every query has terminated.
- **Fresh**: every fragment's status was reported after the request.
  A running fragment counts once it has reported; one that is still starting keeps the watch waiting.
  A fragment that has terminated, or whose worker is not active, does not.
  The status can change again after the reply.

A query that stops or fails also satisfies Running, so the caller checks the returned states.

## Timeouts

A caller can limit how long a watch waits.  
When the time expires first, the watch returns the queries as last read and marks the reply as expired.  
If the catalog cannot be read at that point, the watch returns a catalog error instead, and the caller can send it again.

## Client

`Client` is the typed side of the request channel.  
It sends the statement and the watch, and turns the returned rows into a result:

- `await_running` and `await_completed` fail when a query stops or fails first.
- `await_terminated` accepts any terminal state, for queries that the caller has dropped.
- Every expired wait except `await_fresh` fails with the code `QueryWaitTimeout` and the rows as last read.
