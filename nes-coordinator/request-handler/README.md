# Request Handler

The request handler processes requests submitted to the coordinator and controls when their results are returned.  
For the path of a request through the coordinator, see the [coordinator README](../README.md).

## Completing a Request

A create, drop, or other statement is applied to the catalog and its result is returned at once.  
Only a watch waits: a read of queries with a wait condition.  
To block on a create or drop, a caller sends the statement first and then a watch on the affected queries:

```
client             request handler           catalog             controller
   │── CREATE QUERY ─────▶│                     │                     │
   │                      │── insert, commit ──▶│                     │
   │                      │── intent ────────────────────────────────▶│
   │◀───────── created ───│                     │                     │
   │                      │                     │                     │ starts the fragments
   │                      │                     │◀ fragment states ───│
   │── watch Running ────▶│                     │                     │
   │                      │── read ids ────────▶│                     │
   │                      │ park                │                     │
   │                      │◀──────────────────────────────── state ───│
   │                      │── read rows ───────▶│                     │
   │                      │ condition holds     │                     │
   │◀──────────── rows ───│                     │                     │
```

The queries of a watch are fixed when it is handled, so a query created later is not part of the wait.  
A caller can limit how long a watch waits.  
A watch ends in one of these ways:

```
watch ─┬─ first catalog read fails ──────────▶ the read error
       ├─ matches no query ──────────────────▶ no rows
       └─ parked, checked at once, after every successful statement, on every controller notification,
          at least once per poll interval, and at the deadline
            ├─ condition holds ──────────────▶ the rows
            ├─ deadline passed ──────────────▶ the rows of the last read, marked expired
            └─ deadline passed, read fails ──▶ CatalogUnavailable, and the caller can send the watch again
```

## Wait Conditions

The terminal query states are `Completed`, `Stopped` and `Failed`.  
A watch waits for one of three conditions:

- **Running**: every query is running or has terminated.
- **Terminated**: every query has terminated.
- **Fresh**: every fragment's status was reported after the request.
  The status can change again after the reply.

A query that stops or fails also satisfies Running, so the caller checks the returned states.

For Fresh, each fragment keeps the watch waiting as follows:

| fragment state             | worker of its host                          | last report         | keeps waiting |
|----------------------------|---------------------------------------------|---------------------|---------------|
| Completed, Stopped, Failed | any                                         | any                 | no            |
| Pending, Started, Running  | Pending, Unreachable, Removed, or no worker | any                 | no            |
| Running                    | Active                                      | at or after request | no            |
| Running                    | Active                                      | before, or none     | yes           |
| Pending, Started           | Active                                      | any                 | yes           |

## Client

`Client` is the typed side of the request channel.  
It sends the statement and the watch, and turns rows that end a wait early or late into errors:

| method             | waits until | a watched query has no row                    | a row is Stopped or Failed | the timeout passes        |
|--------------------|-------------|-----------------------------------------------|----------------------------|---------------------------|
| `await_running`    | Running     | error `query 7 does not exist or was dropped` | `EarlyTermination`         | `WaitTimeout`             |
| `await_completed`  | Terminated  | error `query 7 does not exist or was dropped` | `EarlyTermination`         | `WaitTimeout`             |
| `await_terminated` | Terminated  | the rows without it                           | the rows                   | `WaitTimeout`             |
| `await_fresh`      | Fresh       | the rows without it                           | the rows                   | the rows of the last read |

The last three columns are checked from left to right: when a query failed and the timeout passed, the wait returns `EarlyTermination`.  
`WaitTimeout` is returned with the code `QueryWaitTimeout`.
