# The Problem
A central component within NebulaStream is the Buffer Manager that provides buffers to components. As NebulaStream is a data processing system, every operation requires a buffer to make progress. Thus, the fair, efficient, and effective management of buffers is an important feature. 

## Current Implementation
The current implementation provides two types of buffer managers that are statically configured. In particular, their size is determined via a startup configuration and cannot be changed during runtime. In addition, the buffer size is static and fix too.

### Global Buffer manager
The global buffer manager allocates at startup at many buffers as specified by
`numberOfBuffersInGlobalBufferManager:` of size `bufferSizeInBytes:`. This represents a "soft" upper bound of the memory requirements of NebulaStream. It is soft because there are other ways of allocating memory:
- The network manager has its own buffer pool
- The buffer manager has the option of allocate an unpooled buffer which is mainly used by the reconfiguration tasks such that they do not need to block
- Some components like GRPC or other libraries might allocate memory by their own

There is only one global buffer pool and every component needs to create one of the two possible pools to actually get buffers.

#### Fixed-size buffer pool
This buffer pool moves a specified number of buffers from the global to its local pool (they are no longer available in the global pool then only if the pool returns them on shutdown). The special property of this pool is that it is blocking. Thus, if all buffers are exhausted, the getBuffer call blocks until a in-flight buffer returns. This buffer type is currently used for the sources as we do not want that a fast source could exhaust all buffer. The parameter to set this is 
`numberOfBuffersInSourceLocalBufferPool`.


#### Local buffer pool
This buffer pool moves also a specific number of buffers from the global to its local pool. However, if all buffers are exhausted, the getBuffer call is redirected to the global buffer pool and answered from there. This behavior is currently intended for the processing threads as those should use their local buffers first (for better locality/caching) but should not stall if all buffers are in-flight. The parameter is set by `numberOfBuffersPerWorker`.

#### Return policy
Every buffer returns after all links to it go out of scope (it is destructed) via a call back to the buffer manager it originates from.

#### State
TODO: find out how state is handled

### Problems with the current implementation:

**P1:** All sources get the same amount of buffers regardless if they are faster or more important than others.

**P2:** All operators get the same amount of buffers regardless if they are stateless (which might require only a few buffer) or state-full which require many more to hold the state.

**P3:** The assignment of buffers to components is statically configured at startup and cannot be changed, which is not flexible enough for a very dynamic system like NebulaStream.

**P4:** The current design might lead to many unnecessary waiting time as statically configured resources are maybe not used by one component but another would require more. On the other hand, it currently also can load to unfairness if all resources are exhausted by one component. 

**P5:** The current deadlock mechanism is very rough and just throws an error to the last requester of a buffer. 

# Goals and Non-Goals
We want to have a memory management in NebulaStream with the following properties:

**G1:** The buffer distribution should be fair. Fairness means no component can consume all resources. 

**G2:** The buffer distribution should be efficient such that if resources are in the system available, we should make progress, and thus we should not be blocked by maybe sub-optimal static assignments (P1,P2,P3)


We do not:

**NG1:** We design this buffer management for the processing and source 

# Our Proposed Solution

# Alternatives

# Appendix

# Sources and Further Readings
