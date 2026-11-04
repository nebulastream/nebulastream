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

#include <Runtime/BufferRequestStatistics.hpp>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <utility>

namespace NES
{

BufferRequestStatistics::Shard& BufferRequestStatistics::localShard()
{
    thread_local std::unordered_map<uint64_t, Shard*> shardPerInstance;
    if (const auto it = shardPerInstance.find(instanceId); it != shardPerInstance.end())
    {
        return *it->second;
    }
    const std::scoped_lock lock(shardsMutex);
    auto* shard = shards.emplace_back(std::make_unique<Shard>()).get();
    shardPerInstance.emplace(instanceId, shard);
    return *shard;
}

void BufferRequestStatistics::recordPooledRequest(const bool served)
{
    auto& shard = localShard();
    shard.pooledRequests.fetch_add(1, std::memory_order_relaxed);
    if (not served)
    {
        shard.pooledFailed.fetch_add(1, std::memory_order_relaxed);
    }
}

void BufferRequestStatistics::recordUnpooledRequest(const size_t requestedSize, const bool served)
{
    auto& shard = localShard();
    shard.unpooledRequests.fetch_add(1, std::memory_order_relaxed);
    if (not served)
    {
        shard.unpooledFailed.fetch_add(1, std::memory_order_relaxed);
    }
    const std::scoped_lock lock(shard.sizesMutex);
    ++shard.unpooledSizeCounts[requestedSize];
}

BufferRequestStatistics::Snapshot BufferRequestStatistics::snapshot() const
{
    Snapshot result;
    std::map<size_t, uint64_t> sizeCounts;
    {
        const std::scoped_lock lock(shardsMutex);
        for (const auto& shard : shards)
        {
            result.pooledRequests += shard->pooledRequests.load(std::memory_order_relaxed);
            result.pooledFailed += shard->pooledFailed.load(std::memory_order_relaxed);
            result.unpooledRequests += shard->unpooledRequests.load(std::memory_order_relaxed);
            result.unpooledFailed += shard->unpooledFailed.load(std::memory_order_relaxed);
            const std::scoped_lock sizesLock(shard->sizesMutex);
            for (const auto& [size, count] : shard->unpooledSizeCounts)
            {
                sizeCounts[size] += count;
            }
        }
    }
    if (sizeCounts.empty())
    {
        return result;
    }

    uint64_t total = 0;
    double sum = 0;
    for (const auto& [size, count] : sizeCounts)
    {
        total += count;
        sum += static_cast<double>(size) * static_cast<double>(count);
    }

    /// Median is the mean of the elements at ranks (total-1)/2 and total/2 (equal for odd totals).
    const uint64_t lowerRank = (total - 1) / 2;
    const uint64_t upperRank = total / 2;
    std::optional<size_t> lower;
    std::optional<size_t> upper;
    uint64_t seen = 0;
    for (const auto& [size, count] : sizeCounts)
    {
        seen += count;
        if (not lower && seen > lowerRank)
        {
            lower = size;
        }
        if (seen > upperRank)
        {
            upper = size;
            break;
        }
    }

    result.unpooledRequestedSize = SizeSummary{
        .min = sizeCounts.begin()->first,
        .max = sizeCounts.rbegin()->first,
        .avg = sum / static_cast<double>(total),
        .median = (static_cast<double>(*lower) + static_cast<double>(*upper)) / 2.0};
    result.unpooledRequestedSizeCounts = std::move(sizeCounts);
    return result;
}

void BufferMemoryInUse::Counter::add(const size_t bytes)
{
    const auto now = current.fetch_add(bytes, std::memory_order_relaxed) + bytes;
    auto previousPeak = peak.load(std::memory_order_relaxed);
    while (now > previousPeak && not peak.compare_exchange_weak(previousPeak, now, std::memory_order_relaxed))
    {
    }
}

void BufferMemoryInUse::Counter::remove(const size_t bytes)
{
    current.fetch_sub(bytes, std::memory_order_relaxed);
}

void BufferMemoryInUse::addPooled(const size_t bytes)
{
    pooled.add(bytes);
    combined.add(bytes);
}

void BufferMemoryInUse::removePooled(const size_t bytes)
{
    pooled.remove(bytes);
    combined.remove(bytes);
}

void BufferMemoryInUse::addUnpooledChunk(const size_t bytes)
{
    unpooledChunk.add(bytes);
    combined.add(bytes);
}

void BufferMemoryInUse::removeUnpooledChunk(const size_t bytes)
{
    unpooledChunk.remove(bytes);
    combined.remove(bytes);
}

void BufferMemoryInUse::addUnpooledLive(const size_t bytes)
{
    unpooledLive.add(bytes);
}

void BufferMemoryInUse::removeUnpooledLive(const size_t bytes)
{
    unpooledLive.remove(bytes);
}

BufferMemoryInUse::Peaks BufferMemoryInUse::peaks() const
{
    return Peaks{
        .pooledBytes = pooled.peak.load(std::memory_order_relaxed),
        .unpooledChunkBytes = unpooledChunk.peak.load(std::memory_order_relaxed),
        .unpooledLiveBytes = unpooledLive.peak.load(std::memory_order_relaxed),
        .combinedBytes = combined.peak.load(std::memory_order_relaxed)};
}

}
