# The Problem
**P1**: Currently, each source that is started consumes an individual thread. 
That is, starting 1000 sources means starting 1000 threads just for the sources. 
Given that each thread allocates (and consumes) state, this approach does not scale well with an increasing number of sources.

**P2**: Currently, each source gets a fixed number of buffers. 
Since the number of buffers NebulaStream can allocate is limited by the amount of memory available, starting too many source could lead to the occupation all available buffers.
For example, say NebulaStream has enough memory for 100 buffers. 
Each source takes 5 buffers. 
If we start 20 sources, all 100 buffers are taken by the sources.
Therefore, NebulaStream can not allocate any buffers for data processing, leading to a deadlock (no progress possible). 

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
// Create the objects needed to receive a request on the connection.
buffer_.reset(new boost::array<char, 8192>);
request_.reset(new request);

// Loop until a complete request (or an invalid one) has been received.
do
{
// Receive some more data. When control resumes at the following line,
// the ec and length parameters reflect the result of the asynchronous
// operation.
yield socket_->async_read_some(boost::asio::buffer(*buffer_), *this);

// Parse the data we just received.
boost::tie(valid_request_, boost::tuples::ignore)
    = request_parser_.parse(*request_, buffer_->data(), buffer_->data() + length);

// An indeterminate result means we need more data, so keep looping.
} while (boost::indeterminate(valid_request_));
```
In this example, each time we invoke the `async_read_some` function, we yield control to resume when data is available.


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
