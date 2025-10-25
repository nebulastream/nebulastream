# Task Queues in the Query Engine
Our query engine uses queues to perform work stealing and to manage the work load of the engine. 
This document describes the current implementation of the different queues and how it makes sure that no deadlock occurs.

## Scope of Implementation
Our current implementation of the query engine uses multiple queues to distribute the work load among the worker threads.
It does not perform any prioritization of tasks in terms of latency or throughput, but prevents deadlocks that will bring down the system to a halt.

## The Problem
The internal TaskQueue used within the QueryEngine is bounded, this can cause (frequent) deadlocks, which bring down
the entire system.

There are two contributor to the problem:
1. Worker threads may create more tasks than they consume, e.g., during a join probe or triggering a window **(P1)**.
2. Sources can flood the task queue with new tasks, e.g., when a source is faster than the worker threads **(P2)**.
   
Both problems can cause the task queue to fill up and block worker threads from emitting new tasks before picking up new work.

## Continuation (Solution to P1)
Using a bounded queue is safe if we can guarantee, that we can handle the case where the queue is full and are not
required to block for capacity to make progress.

All writes to the `TaskQueue` require the worker thread to handle the scenario that a write did not succeed.
The simplest solution is instead of dispatching the task to other threads, the current thread just stores its current
task (which cannot make progress right now) and continue working on the task it was about to submit.

However, continuation is not always possible, especially when tasks impose dependencies onto each other.
An example is the `PendingPipelineStop`, which can only make progress if all tasks in the queue are handled before the `PendingPipelineStop`.
A `PendingPipelineStop` "emits" itself, if the target node is still referenced, continuation here would attempt to repeat
the check, effectively blocking without making process.

Therefore, our solution is to replace the task with one task from the queue and work on this new task instead.
Thus, we ensure that the worker thread picks up new work and the queue does not block.

### TwoQueues (Solution to P2)

The second cause of deadlocks is the possibility for sources to completely flood the work queue.
To solution to this problem is effectively prioritizing old work before allowing new work to enter.
We solve this by using two queues, one of them is solely used by sources and other outside components to create more work.
1. `TaskQueue`: stores tasks that are emitted by worker threads
2. `AdmissionQueue`: stores tasks that are emitted by sources or query start / stops

If sources saturate the `AdmissionQueue`, they will block until the queue has free slots again. Meanwhile, worker threads remain unaffected.
If the internal worker thread queue is empty a worker thread will pickup work from the source queue, allowing sources to insert more work.


