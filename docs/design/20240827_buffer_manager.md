# The Problem
A central component within NebulaStream is the Buffer Manager that provides buffers to components. As NebulaStream is a data processing system, every operation requires a buffer to make progress. Thus, the fair, efficient, and effective management of buffers is an important feature. 

## Current Implementation
The current implementation provides two types of buffer managers that are statically configured. In particular, their size is determined via a startup configuration and cannot be changed during runtime. In addition, the buffer size is static and fix too.

### Global Buffer manager
The global buffer manager allocates at startup as many buffers as specified by
`numberOfBuffersInGlobalBufferManager:` of size `bufferSizeInBytes:`. This represents a "soft" upper bound of the memory requirements of NebulaStream. It is soft because there are other ways of allocating memory:
- The network manager has its own buffer pool
- The buffer manager has the option of allocate an unpooled buffer which is mainly used by the reconfiguration tasks such that they do not need to block
- Some components like GRPC or other libraries might allocate memory by their own

There is only one global buffer pool and every component needs to create one of the two possible pools to actually get buffers.

#### Fixed-size buffer pool
This buffer pool moves a specified number of buffers from the global to its local pool (they are no longer available in the global pool then only if the pool returns them on shutdown). The special property of this pool is that it is blocking. Thus, if all buffers are exhausted, the getBuffer call blocks until an in-flight buffer returns. This buffer type is currently used for the sources as we do not want that a fast source could exhaust all buffer. The parameter to set this is 
`numberOfBuffersInSourceLocalBufferPool`.

#### Local buffer pool
This buffer pool moves also a specific number of buffers from the global to its local pool. However, if all buffers are exhausted, the getBuffer call is redirected to the global buffer pool and answered from there. This behavior is currently intended for the processing threads as those should use their local buffers first (for better locality/caching) but should not stall if all buffers are in-flight. The parameter is set by `numberOfBuffersPerWorker`. However, the later re-allocation of buffers from the global pool could lead to a dead-lock.

#### Return policy
Every buffer returns after all links to it go out of scope (it is destructed) via a call back to the buffer manager it originates from.

#### Number of Buffer Mangers
Currently, we have one global buffer manger and :
- every query starts one per source
- every worker thread has one in the worker context
- Does also every processing pipeline has one?

#### State
As far as I can see, the state management does not currently use the buffer manager at all and just allocates by its own. This is out of scope here but should be fixed.
### Problems with the current implementation:

**P1:** All sources get the same amount of buffers regardless if they are faster or more important than others.

**P2:** All operators get the same amount of buffers regardless if they are stateless (which might require only a few buffer) or state-full which require many more to hold the state.

**P3:** The assignment of buffers to components is statically configured at startup and cannot be changed, which is not flexible enough for a very dynamic system like NebulaStream.

**P4:** The current design might lead to many unnecessary waiting time as statically configured resources are maybe not used by one component but another would require more. On the other hand, it currently also can lead to unfairness if all resources are exhausted by one component. 

**P5:** The current deadlock mechanism is very rough and just throws an error to the last requester of a buffer. 

# Goals and Non-Goals
We want to have a memory management in NebulaStream with the following properties:

**G1:** The buffer distribution should be fair. Fairness means no component can consume all resources and all can make progress.

**G2:** The buffer distribution should be efficient such that if resources are in the system available, we should make progress, and thus we should not be blocked by maybe sub-optimal static assignments (P1,P2,P3, P4)

**NG1:** We do not consider a file-backuped buffer pools

# How others solve the problem
Other systems like DuckDB or Clickhouse do not have this hard restriction on memory as they backup their buffers on disk. This means that their problem boils down to a classic eviction problem where a sub-set of all buffers (written to files) are loaded into memory and if a new buffer is required, an eviction policy like LFU or LRU is used to evict a buffer and load the required one. Our problem is different in the way that we only work on in-memory buffer and memory is restricted. Thus, we cannot use the virtually unlimited amount of space from a disk.


# Possible Solution -- One for all types
Get rid of the separation of sources and processing is not an option for a streaming system as, compared to a pull-based db, a streaming system has handle the data that is pushed into it. The case that a very fast source could consume all buffers in the system and thus no one makes progress is real and needs to be avoided.  

The anticipated solution changes the source as well as the processing buffer:

## Source buffer
The anticipated solution changes the one-buffer-manager-per-source-per-query to one buffer manager for all sources. This avoids that a fast source can block the processing but still allows that a fast source may consume all buffers such that the other sources cannot make progress. To prevent this, we introduce a delay for the `getBuffer()` call that delays the buffer delivery based on the buffers which that source already have. This can be implemented on two ways:
1. in the buffer manager that book-keeps which source has how many buffers
2. in the source itself that transfers its buffer count in the call `getBuffer(size_t alreadyAquiredBuffers)`

Optimization 1: We could define a lower bound e.g., 5% of the buffers, that can be allocated by a source without a delay
Optimization 2: We can define an upper bound on the number of buffers such that no source is allowed to allocation more than 10% of the buffer.
Optimization 3: A central book-keeping on the buffer manger would enable a huge range of research and possible strategies to test

### Pros: 
- The buffer wastage cause of static parameters, i.e., buffers keep unused by slow sources, is no longer possible 
- Fairness is improved as the delay and the upper bound make sure no source can consume all buffers
- The lower bound allows for a fast start of the source
- The buffer would be shared among all sources of all queries
- This design fits with the anticipated redesign of having one thread pool for all sources instead of one thread per source

### Cons:
- This kills locality. Before, with a source local buffer pool every source, we would use the same buffers, and thus they might be cached. Caching is still possible if only a few sources actually acquire buffers but less likely
- The actual buffer allocation might take more time now due to the artificial delay 

## Processing buffer
We can implement the processing buffers the same way as the source buffers. However, it could be that the processing buffers might benefit more from caching and thus would benefit from a fix pool that can be reused. This has to be seen.


## Open problems
- How to size the processing and source buffers initially? Maybe have a migration possibility between the buffers? 
- 