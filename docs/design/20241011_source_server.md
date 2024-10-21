# The Problem
**P1**: Currently, each source that is started consumes an individual thread. 
That is, starting 1000 sources means starting 1000 threads just for the sources. 
Given that each thread allocates (and consumes) state, this approach does not scale with an increasing number of sources.

**P2**: Currently, each source gets a fixed number of buffers. 
Since the number of buffers NebulaStream can allocate is limited by the amount of memory available, starting too many source could lead to the occupation all available buffers.
For example, say NebulaStream has enough memory for 100 buffers. 
Each source takes 5 buffers. 
If we start 20 sources, all 100 buffers are taken by the sources.
Therefore, NebulaStream can not allocate any buffers for data processing, leading to a deadlock (no progress possible). 

**P3**: Currently, sources read external data into the system while additionally parsing that data to conform to the schema of the logical source.
This leads to harder-to-understand code and implementation and violates basic OOP principles like separation of concerns.

# Goals
- describe goals/requirements that the proposed solution must fulfill
- enumerate goals as G1, G2, ...
- describe how the goals G1, G2, ... address the problems P1, P2, ...

# Non-Goals
- list what is out of scope and why
- enumerate non-goals as NG1, NG2, ...

# (Optional) Solution Background
Most software systems depend on external data in some way.
- Web servers: Clients connect via network and issue requests 
- Database Systems: Clients issue queries or transactions to process data-at-rest
- Stream Processing Engines: Clients issue long-running queries on unbounded streams
All of them need some way to map resources like threads and buffers to these concurrently running operations.

## Threads
There are two naive threading models to manage data ingestion:
1. Have a single thread perform the work
2. Have a single thread per operation/connection perform the work (as currently implemented in NES)
The former leads to serial execution, limiting throughput. 
The latter leads to a lot of overhead in the case of very large numbers of connections/queries.
To solve these problems, web servers and (some) query engines use separate thread pools for I/O and compute.
This decoupling prevents tasks that are CPU-bound to hog threads and being unable to respond to external requests. 
At the same time, threads that do blocking I/O calls (e.g., asking for a disk page, making a request to S3, etc.), they stall compute threads or other threads that could issue I/O requests the meantime.

### Async I/O
Every connection to the outside world (in our case sources) defines asynchronous operations that are able to yield control when they have to wait for something on the outside to happen (like a TCP socket filling a buffer, or a disk page to be copied into memory).
In contrast to synchronous I/O that blocks the calling thread until the operation is finished, asynchronous operations return a handle to the caller when they yield control.
An async runtime manages these handles of the ongoing I/O operations. 
It has a fixed-size thread pool and polls tasks that are ready to make progress.
This is typically done via syscalls like `epoll`, allowing the kernel to notify when something happened (i.e., data has arrived).
Then, a completion handler (some function object) can be dispatched to deal with the arrived data.
Potentially, the task can then make a new request before yielding control again.
Doing it this way, systems using this concept get the following benefits:
- The number of threads (and therefore the overhead) is kept small
- I/O operations can be interleaved with other I/O operations 
- I/O operations can be interleaved with computation (CPU-bound tasks)
Implementing it like this, long-running I/O operations do not slow down data processing or other I/O operations running concurrently.

Libraries that help implementing async I/O are `boost::asio` (CPP) and `tokio.rs` (Rust).
They provide executors, thread pools, and low-level I/O primitives like `boost::asio::posix::stream_descriptor` and `boost::asio::ip::tcp::socket`.
Example of an asynchronous TCP server that handles concurrent connections:
```cpp
// Loop until the connection is closed
for (;;;)
{
  // Allocate a buffer that data can be written to in the background
  buffer_.reset(new boost::array<char, 8192>);

  // Receive some more data. When control resumes at the following line,
  // n represents the number of bytes read.
  // This does not block! 
  // Instead, state is saved and control is suspended to let other tasks make progress
  size_t n = co_await  socket_->async_read_some(boost::asio::buffer(*buffer_), use_awaitable);

  // Handle the data we have received
  ...
}
```
In this example, each time we invoke the `async_read_some` function, we yield control to resume when data is available.


# Our Proposed Solution
The proposed solution depends on three key design decisions:
1. To handle a large number of concurrently running sources on relatively few threads, sources should make asynchronous calls to the outside.
2. I/O threads/buffers should be kept separate from computation threads/buffers to prevent deadlocks.
3. Sources have a single task and they should do this task well: ingest raw bytes into the systems buffers as fast as possible

## 1) Async I/O
We need asynchronous I/O requests because if we desire to reduce the number of threads significantly when dealing with large numbers of sources, we can not afford to make blocking calls on these few threads.
To put 1) into practice, we employ a `SourceServer` that orchestrates all registered physical sources on a single worker node.
Each source defines how to read data into `TupleBuffer`s asynchronously by utilizing `boost::asio` I/O objects or similar.
The server runs an event loop provided by `boost::asio::io_context` and schedules coroutines onto threads of it's fixed-size thread pool.
It deals with the adding and removal of sources, propagation of errors to the `QueryManager` and thread synchronization when writing results downstream.

## 2) I/O and compute separation
For 2), we create the threads internally in the `SourceServer`, as it runs the event loop and knows when and how to schedule threads for execution.
However, we do not concern the `SourceServer` directly with the management of buffers, as this is the task of the `BufferManager`.
We could, however, track statistics on how fast each source produces data (like currently implemented), and hint this to the buffer manager to help it out fairly distributing resources.
For flow control (handling speed differences between producer and consumer), the following questions arise with respect to buffers:
- **Fast Source, Slow Downstream**: How do we apply backpressure to help the system under high load?
If we work with a fixed-size buffer pool, we can apply backpressure to a source by rejecting a request to a buffer.
This could be done by returning an error and forcing the source to try again later.
The source could then set a timeout and be woken up by the I/O executor at a later point to try again.
In the meantime, the external system will be throttled.
Still, if a single source is able to request all buffers, this would then slow down all sources.
Therefore, we need to make sure not all buffers from the pool can be allocated by a single source (e.g., a maximum of N - NUM_SOURCES with N being the number of buffers).
- **Slow Source, Fast Downstream**: How do we keep the latency in check when a source takes a very long time filling a buffer? 
I see several possibilities here:
- Demand-based: If a downstream pipeline sees that the task queue is empty, it may ask the `SourceServer` to release a partially filled buffer.
Instead of issuing new requests or waiting for more data, the coroutine driving the source could yield control and give out the buffer, asking the `BufferManager` for a new one.
This approach would require pipelines having access to the `SourceServer`, but it would give a consistent approach for controlling the latency.
- Timeout-based: After some time is passed, the source releases its buffer, writing it downstream.
Here, we get another hyperparameter for the system. 
Global timeouts are not suitable as each query might have different latency requirements.
Also, the timeout set might not even reflect in the final latency (it only reflects on the latency of the source).

## 3) Move parsing out of the sources
Having parsers inside the sources mixes logic and code for retrieving external data (I/O) with the interpretation of the raw bytes ingested (compute).
Instead, parsing should be done as the first part of a pipeline.
This enables the sources to concern themselves with the single simple task of reading data into `TupleBuffer`s and handing them out to consumers.

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
- https://www.boost.org/doc/libs/1_86_0/doc/html/boost_asio.html

# (Optional) Appendix
- provide here nonessential information that could disturb the reading flow, e.g., implementation details
