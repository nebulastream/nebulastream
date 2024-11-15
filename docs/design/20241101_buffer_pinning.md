# The Problem
Currently, buffers are acquired via `BufferManager::getBufferBlocking()`.
When the system runs out of buffers, 
the system deadlocks because tasks that already hold some buffers won't release them,
until they get all the buffers they need (**P1**).

Hiding this mechanism behind the current `BufferManager` and spilling buffers that are still used is not possible,
because the current `TupleBuffer` implies a valid memory address to which data can be written or read from (**P2**).

Even if we'd change `TupleBuffer::getBuffer` so that it always returns a valid memory address, loading the `TupleBuffer`
from disk if needed, we wouldn't know when the buffer could get spilled again(**P3**).

# Goals
The system should spill buffers, 
preferably ones that are not needed in the near future,
to disk safely, freeing up memory for other tasks.

To spill safely, we need to start differentiating between pinned Buffers,
which are guaranteed to be in-memory, and buffers which content might be spilled to disk at the moment.

We should design the interface to this mechanism in a way, that
- G1: When the system runs out of memory, it spills buffers that are expected not to be accessed in the near future,
- G2: Components cannot accidentally try to access memory regions whose content was spilled or could get spilled at any moment,
- G3: It's as easy as possible to mark a buffer as spillable or pin it, to encourage the usage of this mechanism,
- G4: A non-spilled spillable buffer can be converted to a pinned buffer with minimal overhead, to encourage the usage of this mechanism.

# Non-Goals
- NG1: Validate whether components are using too many pinned buffers, this needs to be caught in the PR Review process
- NG2: Fairness, components are inherently unequal and if a component requires more pinned buffers it should get them at
the expense of one that doesn't
- NG3: Providing the `BufferManager` more information about who is using the buffer to improve allocation, this will be the topic of a future design document
- NG4: Robustness through workload shedding, we assume we have enough disk space for all the buffers we'd want to spill.
Also, this would require further discussion about how shedding policies can and should be affected by the user.
- NG5: Fault tolerance, as forcing a buffer to disk conflicts with G3 and G4 and should be provided through a different interface.

# Solution Background
[A previous design document](https://github.com/nebulastream/nebulastream-public/pull/342) discussed both the current state
of the buffer manager, the issue of deadlocks around buffer acquisition and potential solutions.

# Our Proposed Solution
We introduce a separate type `SpillableBuffer`, that does not provide direct access to the data in-memory (G2).
Instead, it can be turned back into a `TupleBuffer`, that now represents a pinned buffer and should be renamed to `PinnedBuffer`.
`SpillableBuffers` are a logical handle to a buffers data, but the physical location of the data is not fixed.

All of the buffer types should retain their reference counting semantics as disk space is also a resource we need to 
recycle.

When creating a `SpillableBuffer` from a `PinnedBuffer`, the data is not moved directly to disk.
It's only marked as a buffer that the `BufferManager` could safely move to disk (or to another memory address).

The `BufferManager` can now, either when a new buffer is requested but there is no free memory, 
or preemptively in a regularly scheduled task, move the buffer to disk (G1).

Buffers will never be spilled directly when marking them as spillable, because this would create unnecessary I/O.
Ideally, the system never runs out of memory and buffers never need to get spilled.
But, if the system needs to spill it should be able to spill as many buffers as possible.
So, marking something as spillable should be ideally just toggling a flag in terms of runtime overhead (G3, G4). 

But, just a boolean flag in a `TupleBuffer` would make it easy to write unsafe code, where a component tries to 
access buffer without checking if its pinned before.

Using separate types ensures that only the memory of pinned buffers can be accessed.

Also, this allows us to implement a simple form of pointer swizzling, using the same space in the MemorySegment currently used
for the memory pointer for a pointer to persistent storage as well.

This requires us to introduce two new flags: first, spilled, second, pinned status.
The spilled flag is used to check whether the pointer is pointing to memory or disk.
The pinned status indicates whether the current location is guaranteed, or can be moved to memory/disk when required.

To convert a `SpillableBuffer` to a `PinnedBuffer` or vice versa, we use move semantics to not only reduce unnecessary copies,
but also to make it easy for clang-tidy to catch accidental used of not-safe-to-use buffers (G3/G4).

```c++
std::queue<SpillableBuffer> toProcess;
std::queue<SpillableBuffer> output;

while (!toProcess.isEmpty()) {
    SpillableBuffer current = toProcess.front();
    toProcess.pop();
    
    PinnedBuffer workingBuffer = PinnedBuffer(std::move(current))
    {   
        //In a future hardening mode, the conversion should check reference count
        //In release mode, no runtime checks are done to ensure that there is only one owner
        uint8_t* bufferMemory = workingBuffer.getUniqueBufferOrFail();
        //do something on the memory
    }
    SpillableBuffer processed = SpillableBuffer(std::move(workingBuffer));
    
    //Illegal access to a possibly invalid memory address caught by clang-tidy
    workingBuffer.getBuffer()
    
    
    output.push(processed);
}
```

Turning a `SpillableBuffer` into a `PinnedBuffer` can in the worst case be blocking.
But, to reduce the probability that pinning a buffer requires blocking for IO, we should always spill a larger amount of 
buffers asynchronously, return this first successfully freed buffer and keep the others buffers freed to serve other requests without blocking again.

When turning a `PinnedBuffer` into a `SpillableBuffer`, we need to ensure that a buffer is not spilled, when there is still
a `PinnedBuffer` reference on it.
There are two ways we could implement this:
1. Have two reference counters in the data structure, one for `SpillableBuffer` one for `PinnedBuffer`.
2. Assume that when converting a spillable buffer to a pinned buffer there is only one reference. 
    Add an assert for this in hardening mode.

The first option is more flexible and ergonomic, the second one has a slightly lower overhead.


### Spilling Policies
Currently, a queue of available buffers is kept in the `BufferManager`.
We could also keep a queue of buffers that are to be spilled, but while enqueuing or popping is O(1), removing a buffer that is somewhere inside the queue is not.
Because we want pinning an unspilled spillable buffer to be just like toggling a flag, we could just leave pinned buffers
in the spillable queue and check the pinned flag when they are popped.
But, then we might need to pop an arbitrary amount of buffers to find a spillable one.

This would just be a worse implementation of an LRU cache replacement policy.
We should implement LRU directly and later evaluate to replace it with CLOCK.

### Where to spill to
Spilling buffers appends them to files of a fixed maximum size (e.g. 128MB, the largest size a single extend in ext4 can be).
This ensures that buffers are always written sequentially.
Optimizing for sequential reads is not a goal of this design document (NG3).
Because buffers do not live indefinitely, we just free the blocks when they are moved to memory again and delete the file
when the whole extend was used once.
This allows us to leave defragmentation to the file system.

### Spilling implementation
When a worker thread requests a buffer, but none are available, it initiates the spilling of buffers.
We will spill using io_uring, which allows us to do IO asynchronously with as few syscall as possible.
Because we keep the `BufferManager` passive for now, we need to distribute the work of spilling over the worker threads.
The goal is to batch as much as possible, without blocking the worker thread more than is necessary to free the memory it needs.

#### Spilling Steps
These requests are put into io_urings submission queue.
For now, we will use the manual `io_uring_submit` syscall to signal the OS that it can poll the queue.
Then, we wait with `io_uring_wait_cqe` until some disk write is completed.
We read as much from the queue as we can without blocking again and 
turn the add the memory segments freed to the `availableBuffers` queue except one, which we turn into a `PinnedBuffer` and return.

Future requests first try to retrieve a free buffer in the `availableBuffers` queue, and then peek the completion queue
for any completions from a previous spilling.
When a completion is found, again, as many as possible are retrieved, and the appropriate memory segments are turned added
to the `availableBuffers` queue, except one that is returned as a `PinnedBuffer`.

For now, we use io_uring interrupt based completion, but polling based solutions could be explored and benchmarked.

#### Spilling Thread Model
Since requests can come in concurrently, and the worker threads handle the spilling themselves, we need to ensure thread safety.
The `availableBuffers` queue is already multi-consumer-multi-producer implementation and we should use it as much as possible
to avoid unnecessary synchronisation.
But, io_urings ring buffers are not synchronized, so we will use one lock in total for both ring buffer.
We will also have an additional atomic flag that indicates whether a thread is currently spilling disks.
If that is the case, other threads will wait for a conditional variable, that the spilling thread will invoke when
it has moved memory segments to `availableBuffers`.

Should there be more waiting requests than newly freed segments, the thread of a left-over thread acquires the lock,
and checks whether there are outstanding completions.
If so, it will wait on the completion queue and then process as many as possible.
If not, it starts the process again.

This way, we ensure that the limit of the ring buffer is never exceeded, but there is as little locking as possible.

# Alternatives
### Active BufferManager:
If spilling on-buffer-request causes too much blocking, the `BufferManager` should get its own thread(-pool).
Then it can continuously spill buffers so that there is always a constant pool of free in-memory buffers available.

### Async Interface
For components, as the sources, we should in the future also provide an async interface that instead of blocking 
for completion events uses the polling of the event loop.

### Virtual-Memory Assisted Buffer Management:
As introduced by Leis, one could also use virtual memory facilities of the OS instead of pointer swizzling.
To be efficient, extensive hinting needs to be used, and ideally a special kernel module is loaded.
The biggest touted advantage is better handling of non-tree graph structures where pointer swizzling is supposedly more difficult to implement.
Another use case for which Leis evaluated this approach in Leanstore is transactional workloads on B-Trees.
I don't believe we should use this approach because:

1. We don't have large non-tree graph structures between buffers, because we don't support graph queries at the moment and its a low priority for the future,
2. Even if we would want to support it at some point, I think it's reasonable to not optimize NES for terrabytes of graph data,
3. We don't expect to revisit buffers lots of times like you would in a transactional database,
4. Relying on OS-specific syscalls for hinting reduces portability, is incompatible with unikernels which we are more likely to prioritize than graph data,
and is difficult to test and control for,
5. We don't expect data to stay on a system for a long time because we are a streaming system and faster access in OLTP workloads is not applicable.

Furthermore, this DD does not prohibit anyone from implementing virtual memory assisted buffer management behind the interface.
So, it could still be used as a backing implementation instead of swizzling to benchmark the impact of the different approaches.

### CAR: Clock with Adaptive Replacement
Is a more advanced cache replacement policy that also takes into account the recency and frequency of accesses.
In the future, we could evaluate whether this policy performs better than normal CLOCK.

# Sources and Further Reading
[Viktor Leis](https://dl.acm.org/doi/pdf/10.1145/3588687) lined out different approaches to buffer management in a relational database and 
introduces virtual memory assisted buffer management approaches.

### Guaranteed Spilled Buffers
While we are at it, we could also add a `SpilledBuffer`, which in turns guarantees that data is on the disk.
I decided against it, because the primary use case for that, fault-tolerance, is a different mechanism with a fundamentally different
goal than spilling to disk to free up memory.
It should be handled by its own semantics, operators and data structures.

