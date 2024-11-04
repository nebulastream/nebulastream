# The Problem
NebulaStream promises to be able to cope with millions of heterogenous sources as one of its main selling points.

Currently, each source that is started runs in an individual thread. 
That is, starting 1000 sources means starting 1000 threads just for the sources. 
Since the threads are running permanently, this leads to a massive oversubscription of the CPU and causes large overhead due to context switching.
Given that each thread allocates state (a private stack of several MBs), this is expensive. 
Therefore, this approach does not scale with an increasing number of sources (**P1**).

**P2**: Currently, sources read external data into the system while additionally parsing that data to conform to the schema of the logical source.
This leads to harder-to-understand code and implementation and violates basic OOP principles like separation of concerns.

# Goals
Redesign data ingestion for NES, such that the following goals are reached.

- A single node worker should be able to handle thousands of concurrent sources without becoming a bottleneck (**G1**, scalability).
- The registration, starting, stopping and unregistration of sources at runtime should be possible (**G2**, dynamicity).
- The implementation should conform to the ongoing efforts refactoring the [description and construction of sources](https://github.com/nebulastream/nebulastream-public/blob/main/docs/design/20240702_sources_and_sinks.md), and their [interaction with the query manager]().
- **G3**: errors should be handled transparently as described in this [DD](https://github.com/nebulastream/nebulastream-public/blob/main/docs/design/20240711_error_handling.md)
- **G4 (Maintainability)**: the implementation of new sources is as easy as possible by providing a small, concise interface.
- **G5 (Compatibility)**: sources can still be implemented with the current threading model as a fallback/baseline.

G1 and G2 address P1
- describe goals/requirements that the proposed solution must fulfill
- enumerate goals as G1, G2, ...
- describe how the goals G1, G2, ... address the problems P1, P2, ...

# Non-Goals
- **NG1**: a complete vision or implementation on how the sources interact with the `BufferManager`, and what policies the `BufferManager` should implement to facilitate fairness and performance.
- **NG2**: a complete vision or implementation on how to handle parsing tuples out of the bytes that the source emits.
- **NG3**: a complete vision or implementation on how to handle source sharing.
- **NG4**: a complete vision or implementation on how to handle data ingestion via internal sources (e.g., when intermediate data is shuffled around between nodes).

# Solution Background
Most software systems depend on external data in some way:
- Web servers - clients connect via network and issue requests 
- DBMSs - large volumes of historical data are scanned from disk or network, originating from potentially multiples queries
- SPEs - sources continuously ingest data (think thousands of sources, potentially from thousands of different queries)
All of them need some way to map compute resources (threads) to these concurrently running I/O operations.

## Threads
There are two naive threading models to manage data ingestion:
1. Single-threaded I/O that switches between all tasks/connections/sources
2. A single thread per task/connection/source (as currently implemented in NES)

The former leads to serial execution, limiting throughput. 
The latter leads to a lot of overhead in the case of very large numbers of connections/queries and therefore to a term called *oversubscription* of the CPU.

From "C++11 - Concurrency in Action": *"When you have an application that needs to handle a lot of network connections, it’s often tempting to handle each connection on a separate thread, because this can make the network communication easier to think about and easier to program.
This works well for low numbers of connections (and thus low numbers of threads).
Unfortunately, as the number of connections rises, this becomes less suitable; the large numbers of threads consequently consume large numbers of operating system resources and potentially cause a lot of context switching (when the number of threads exceeds the available hardware concurrency), impacting performance.
In the extreme case, the operating system may run out of resources for running new threads before its capacity for network connections is exhausted.
In applications with very large numbers of network connections, it’s therefore common to have a small number of threads (possibly only one) handling the connections, each thread dealing with multiple connections at once."*

To solve these problems, web servers and query engines use separate thread pools for I/O and compute.
This decoupling prevents tasks that are CPU-bound to hog threads and being unable to respond to external requests. 
At the same time, threads that do blocking I/O calls (e.g., asking for a disk page, making a request to S3, etc.) stall compute threads or other threads that could issue I/O requests the meantime.

## Async I/O
Now that we have a separate thread pool for I/O operations, we need to define operations and decide how to schedule them on this pool.
With the assumption that we primarily do I/O when dealing with sources, we are **waiting** for something to happen in the background for most of the time.
At some point in each source, we call a client like `doRequest()` and then we are blocking on this call.
The CPU does not have any insight information on what we are waiting on, so if we are unlucky and the CPU does not give another thread a time slice, we block the CPU with our I/O.

An improvement to this would be to to put the thread to sleep by using mechanisms like `std::future` and have another thread run while we wait on the I/O to complete.
However, this still has the thread where the call was invoked from occupied, so this exact thread is not free to use for other tasks.
If we assume a limited pool of threads, which we strive to have, we could still end up in the situation where all available I/O threads are sleeping, waiting for blocking I/O to complete.
During this time, no other I/O tasks can make progress.

It would be nice to have a mechanism to pause/resume a function waiting for external I/O **without** occupying a thread while waiting for data to arrive.
That's what coroutines give us from C++20 onwards.
They enable us to suspend and resume functions while they are running, preserving their state.
C++20 introduced three new keywords, namely `co_await`, `co_yield` and `co_return`.
`co_await` pauses execution and awaits another coroutine that it calls (lower-level code), `co_yield` yields a value to the caller without returning to the caller (think python generators), `co_return` signals termination of the coroutine.
Coroutines provide an elegant way to implement async I/O while allowing it to look like sequential execution.
Before coroutines existed, one had to use chains of callbacks that were invoked when asynchronous operations returned to the calling function.

Utilizing these mechanisms of pausing/resuming execution, we are now able to wait for external events to happen in the background, without occupying a thread (like a TCP socket filling a buffer, or a disk page to be copied into memory).
An important thing to note is that we should avoid running blocking operations by yielding control regularly.
Otherwise, we block a thread, possibly preventing other async operations from making progress.
To the contrary, if we are able to isolate I/O-bound operations properly from CPU-bound operations, an enormous amount of concurrent tasks can be scheduled on relatively few threads. 

An async runtime manages these ongoing tasks. 
It has a fixed-size thread pool and polls tasks that are ready to make progress.
This is typically done via syscalls like `epoll`, allowing the kernel to notify when something happened (i.e., data has arrived), queueing the coroutine to be picked up by a thread and resumed.
We do not rely on spin waiting, nor do we need a timer or regularly wake up to check if something is ready.

When resumed, we can deal with the arrived data, e.g., by giving it out to other components for further processing.
Potentially, the task can then make a new request before yielding control again.

Libraries that help implementing async I/O are `boost::asio` (CPP) and `tokio.rs` (Rust).
They provide executors, thread pools, and low-level I/O primitives like `boost::asio::posix::stream_descriptor` and `boost::asio::ip::tcp::socket`.
Example of an asynchronous TCP server that handles concurrent connections:
```cpp
// Loop until the connection is closed
for (;;;)
{
  // Allocate a buffer that data can be written to in the background
  auto buffer = new boost::array<char, 8192>;

  // Receive some more data. When control resumes at the following line,
  // n represents the number of bytes read.
  // This does not block! 
  // Instead, state is saved and control is suspended to let other tasks make progress
  size_t n = co_await  socket_->async_read_some(boost::asio::buffer(buffer), use_awaitable);

  // Handle the data we have received
  ...
}
```
In this example, each time we invoke the `async_read_some` function, we yield control to resume when data is available.

In summary, systems using async I/O get the following benefits:
- The number of threads (and therefore the overhead) is kept small
- I/O operations can be interleaved with other I/O operations 
- I/O operations can be interleaved with computation (CPU-bound tasks)

# Our Proposed Solution
The proposed solution depends on four design decisions:
1. We redesign sources to make asynchronous calls inside coroutines to allow efficient ingestion on relatively few threads.
2. We remove parsing from the sources to separate CPU-bound and I/O-bound workloads.
3. We introduce a centralized `SourceServer` that orchestrates the execution of all sources on a thread pool, the handling of errors, and the adding and removal of sources.
4. We propose to have sources receive buffers from a separate buffer pool to prevent deadlocks (to be discussed in the DD for the buffer manager).

## 1) Sources
From "C++11 - Concurrency in Action": *"If your thread is blocked waiting for external input, it can't proceed [...] It's therefore undesirable to block on external input from a thread that also performs tasks that other threads may be waiting for."*
We need asynchronous I/O requests and the removal of parsing logic because if we desire to reduce the number of threads significantly when dealing with large numbers of sources, we can not afford to make blocking calls or do CPU-intensive work on these few threads.
On a high level, the source should perform the following tasks:
- Connect to an external system (Kafka, MQTT, etc.), device (disk, memory, network), or handle a protocol (TCP, UDP, etc.)
- Read data from there into a `TupleBuffer` (which should be called `ByteBuffer` or `ReadBuffer` at this point as we do not deal with tuples on a logical level yet?)
- Resolve errors as specified from the outside by retrying and recovering, dropping data, escalating the error to the outside
- Disconnect and close the link to the external system or device

Each source defines how to read data into `TupleBuffer`s asynchronously by utilizing `boost::asio` I/O objects or similar.
From the docs of ClickHouse: *For byte-oriented input/output, there are ReadBuffer and WriteBuffer abstract classes [...] Read/WriteBuffers only deal with bytes.*
Instead, parsing should be done as the first part of a pipeline.
Furthermore, pushing certain operations to parsing will be possible, like filtering unparsed data, etc.
Other components need to handle parsing from now on, which results in a set of new challenges: writing raw data into buffers leads to additional complexity for downstream components.
Buffers are not self-contained and have tuples potentially spanning over subsequent buffers, which leads to problems when picking up these buffers from multiple threads.



To put 1) into practice, we employ a `SourceServer` that orchestrates all registered physical sources on a single worker node.
The server runs an event loop provided by `boost::asio::io_context` and schedules coroutines onto threads of its fixed-size thread pool.
It deals with the addition and removal of sources, propagation of errors to the `QueryManager` and thread synchronization when writing results downstream.

## 4) Buffer Management
We do not concern the `SourceServer` directly with the management of buffers, as this is the task of the `BufferManager`. 
Still, the `BufferManager` has to be aware of the sources and their buffer usage.

### Ideas for the `BufferManager`
For flow control (handling speed differences between producer and consumer, and among different producers), the following questions arise with respect to buffers:
1. **Slow Source, Fast Downstream**: *How do we keep the latency in check when a source takes a very long time to fill a buffer?*

This will be the general case where the system is not overwhelmed with external data, and I see two approaches here:
- *Demand-based*: If the `QueryManager` (?) sees that the task queue is empty, it may ask the `SourceServer` to release a partially filled buffer.
Instead of issuing new requests or waiting for more data, the coroutine driving the source could yield control and give out the buffer, asking the `BufferManager` for a new one.
This approach would require pipelines to have some kind of backwards control flow to the `SourceServer`, but it would give a consistent approach for controlling the latency.
- *Timeout-based*: After some time has passed, the source releases its buffer, writing it downstream.
Here, we get another hyperparameter for the system. 
Global timeouts are not suitable as each query might have different latency requirements.
Also, the timeout set might not even be reflected in the final latency (it only reflects on the latency of the source).
Therefore, I propose the implementation of a demand-based solution.

2. **Fast Source, Slow Downstream**: *How do we apply backpressure to help the system under high load?*

If we work with a fixed-size buffer pool, we can apply backpressure to a source by rejecting its request to acquire a buffer.
This could be done by returning an error and forcing the source to try again later.
The source could then set a timeout and be woken up by the I/O executor at a later point to try again.
Or, we could use a thread synchronization primitive like a condition variable or some other event-based mechanism to allow the source to continue when a buffer is available.
In the meantime, the external system will be throttled.

3. **Fast Sources, Slow Sources**: *How do we handle speed differences between sources and prevent fast sources from stealing all resources?*

Speed differences can be defined as differences in buffer requests per unit time.
When downstream tasks are generally reading and thus releasing the buffers faster than the sources can fill new buffers, case 1) applies and the speed differences do not lead to problems. 
By contrast, when a source is producing and thus requesting buffers at a faster rate than downstream tasks can release them, case 2) applies.
In a temporary case, enough buffers are available and the buffer manager will happily give them out to fast producers so the available buffers will be automatically be distributed relative to the demand.
Problems arise when a single or few sources take so many buffers that others are impacted and have buffer requests denied.
Therefore, in general, we always want to guarantee **each source having a single buffer available that it can fill in the background (double buffering)**. 
This is the minimum required to effectively utilize async I/O.
`BufferManager` needs to make sure it holds a reserve of a single buffer per source at all times.
Implementation-wise, it could either draw from a global pool for I/O but keep a reserve, or have local pools for each source that it can redistribute based on demand.

The previous necessitate the assumption/invariant that more buffers exist than the number of sources registered.
When a source wants to be registered and this would violate the requirement, performance will degrade.


- start with a high-level overview of the solution
- explain the interface and interaction with other system components
- explain a potential implementation
- explain how the goals G1, G2, ... are addressed
- use diagrams if reasonable

# Proof of Concept

# Alternatives
- discuss alternative approaches A1, A2, ..., including their advantages and disadvantages

# Open Questions
- How to enforce async I/O, or how to fallback to baseline when to async I/O is possible 
- How to deal with source sharing?
- How to deal with internal sources?


- list relevant questions that cannot or need not be answered before merging
- create issues if needed

# Sources and Further Reading
- https://www.boost.org/doc/libs/1_86_0/doc/html/boost_asio.html
- https://stackoverflow.com/questions/8546273/is-non-blocking-i-o-really-faster-than-multi-threaded-blocking-i-o-how

# (Optional) Appendix
- provide here nonessential information that could disturb the reading flow, e.g., implementation details

