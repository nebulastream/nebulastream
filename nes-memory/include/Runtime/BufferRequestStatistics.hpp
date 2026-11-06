/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <source_location>
#include <string>
#include <utility>
#include <vector>

namespace NES
{

/// Counts pooled and unpooled buffer requests of one BufferManager.
/// Every thread records into its own shard, so recording does not contend across threads.
class BufferRequestStatistics
{
public:
    struct SizeSummary
    {
        size_t min;
        size_t max;
        double avg;
        double median;
    };

    /// Unpooled buffers handed out at one call site and not released yet. Shared with their recycle callbacks.
    struct LiveBuffers
    {
        std::atomic<uint64_t> buffers{0};
        std::atomic<uint64_t> peakBuffers{0};
        std::atomic<size_t> bytes{0};
        std::atomic<size_t> peakBytes{0};
        void add(size_t bufferBytes);
        void remove(size_t bufferBytes);
    };

    struct CallSite
    {
        /// file:line of the getUnpooledBuffer call
        std::string location;
        uint64_t requests = 0;
        uint64_t failed = 0;
        /// requested size -> number of requests
        std::map<size_t, uint64_t> requestedSizeCounts;
        uint64_t peakLiveBuffers = 0;
        /// aligned bytes
        size_t peakLiveBytes = 0;
    };

    struct Snapshot
    {
        uint64_t pooledRequests = 0;
        uint64_t pooledFailed = 0;
        uint64_t unpooledRequests = 0;
        uint64_t unpooledFailed = 0;
        std::optional<SizeSummary> unpooledRequestedSize;
        /// requested size -> number of requests, ascending by size
        std::map<size_t, uint64_t> unpooledRequestedSizeCounts;
        /// bytes written into a pooled buffer when it was recycled -> number of buffers, ascending by bytes
        std::map<size_t, uint64_t> pooledWrittenBytesCounts;
        /// descending by number of requests
        std::vector<CallSite> unpooledCallSites;
    };

    void recordPooledRequest(bool served);
    /// Returns the live buffers of the call site, which the unpooled buffer updates when it is handed out and released.
    std::shared_ptr<LiveBuffers> liveUnpooledBuffers(const std::source_location& caller);
    void recordUnpooledRequest(size_t requestedSize, bool served, const std::source_location& caller);
    void recordPooledFill(size_t writtenBytes);
    [[nodiscard]] Snapshot snapshot() const;

private:
    struct Shard
    {
        std::atomic<uint64_t> pooledRequests{0};
        std::atomic<uint64_t> pooledFailed{0};
        std::atomic<uint64_t> unpooledRequests{0};
        std::atomic<uint64_t> unpooledFailed{0};
        std::mutex sizesMutex;
        /// requested size -> number of requests; exact median without storing every request
        std::map<size_t, uint64_t> unpooledSizeCounts;
        std::map<size_t, uint64_t> pooledWrittenBytesCounts;
        struct CallSiteCounts
        {
            std::string location;
            std::shared_ptr<LiveBuffers> live;
            uint64_t failed = 0;
            std::map<size_t, uint64_t> requestedSizeCounts;
        };
        /// keyed by the source_location's file name pointer and line, which are cheap to compare
        std::map<std::pair<const char*, uint_least32_t>, CallSiteCounts> unpooledCallSites;
    };

    Shard& localShard();

    static inline std::atomic<uint64_t> nextInstanceId{0};
    /// Shards are looked up by instance id, not address, as a new BufferManager can reuse a destroyed one's address.
    const uint64_t instanceId = nextInstanceId.fetch_add(1);
    mutable std::mutex shardsMutex;
    std::vector<std::unique_ptr<Shard>> shards;
    /// Live buffers are shared across shards, as a buffer can be released on another thread than the one requesting it.
    std::mutex liveMutex;
    std::map<std::string, std::shared_ptr<LiveBuffers>> liveUnpooledBuffersPerLocation;
};

/// Bytes in use by one BufferManager and their peaks. Shared with the recycle callbacks of unpooled buffers, which can outlive it.
class BufferMemoryInUse
{
public:
    struct Peaks
    {
        size_t pooledBytes;
        /// bytes of the allocated unpooled chunks
        size_t unpooledChunkBytes;
        /// bytes of the unpooled buffers that are not released yet; a chunk stays allocated until its last buffer is released
        size_t unpooledLiveBytes;
        /// pooled and unpooled chunk bytes at the same time
        size_t combinedBytes;
    };

    void addPooled(size_t bytes);
    void removePooled(size_t bytes);
    void addUnpooledChunk(size_t bytes);
    void removeUnpooledChunk(size_t bytes);
    void addUnpooledLive(size_t bytes);
    void removeUnpooledLive(size_t bytes);
    [[nodiscard]] Peaks peaks() const;

private:
    struct Counter
    {
        std::atomic<size_t> current{0};
        std::atomic<size_t> peak{0};
        void add(size_t bytes);
        void remove(size_t bytes);
    };

    Counter pooled;
    Counter unpooledChunk;
    Counter unpooledLive;
    Counter combined;
};

}
