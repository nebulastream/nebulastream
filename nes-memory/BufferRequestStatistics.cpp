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

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <source_location>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#include <fmt/format.h>

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

std::shared_ptr<BufferRequestStatistics::LiveBuffers> BufferRequestStatistics::liveUnpooledBuffers(const std::source_location& caller)
{
    auto& shard = localShard();
    const std::scoped_lock lock(shard.sizesMutex);
    auto& callSite = shard.unpooledCallSites[{caller.file_name(), caller.line()}];
    if (not callSite.live)
    {
        std::string_view file = caller.file_name();
        if (const auto repoPath = file.find("/nes-"); repoPath != std::string_view::npos)
        {
            file.remove_prefix(repoPath + 1);
        }
        callSite.location = fmt::format("{}:{}", file, caller.line());
        const std::scoped_lock liveLock(liveMutex);
        auto& live = liveUnpooledBuffersPerLocation[callSite.location];
        if (not live)
        {
            live = std::make_shared<LiveBuffers>();
        }
        callSite.live = live;
    }
    return callSite.live;
}

void BufferRequestStatistics::recordUnpooledRequest(const size_t requestedSize, const bool served, const std::source_location& caller)
{
    auto& shard = localShard();
    shard.unpooledRequests.fetch_add(1, std::memory_order_relaxed);
    if (not served)
    {
        shard.unpooledFailed.fetch_add(1, std::memory_order_relaxed);
    }
    const std::scoped_lock lock(shard.sizesMutex);
    ++shard.unpooledSizeCounts[requestedSize];
    auto& callSite = shard.unpooledCallSites[{caller.file_name(), caller.line()}];
    ++callSite.requestedSizeCounts[requestedSize];
    if (not served)
    {
        ++callSite.failed;
    }
}

void BufferRequestStatistics::recordPooledFill(const size_t writtenBytes)
{
    auto& shard = localShard();
    const std::scoped_lock lock(shard.sizesMutex);
    ++shard.pooledWrittenBytesCounts[writtenBytes];
}

BufferRequestStatistics::Snapshot BufferRequestStatistics::snapshot() const
{
    Snapshot result;
    std::map<size_t, uint64_t> sizeCounts;
    std::map<std::string, CallSite> callSites;
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
            for (const auto& [bytes, count] : shard->pooledWrittenBytesCounts)
            {
                result.pooledWrittenBytesCounts[bytes] += count;
            }
            for (const auto& [key, counts] : shard->unpooledCallSites)
            {
                auto& callSite = callSites[counts.location];
                callSite.location = counts.location;
                callSite.failed += counts.failed;
                for (const auto& [size, count] : counts.requestedSizeCounts)
                {
                    callSite.requests += count;
                    callSite.requestedSizeCounts[size] += count;
                }
                callSite.peakLiveBuffers = counts.live->peakBuffers.load(std::memory_order_relaxed);
                callSite.peakLiveBytes = counts.live->peakBytes.load(std::memory_order_relaxed);
            }
        }
    }
    for (auto& [location, callSite] : callSites)
    {
        result.unpooledCallSites.push_back(std::move(callSite));
    }
    std::ranges::sort(result.unpooledCallSites, std::ranges::greater{}, &CallSite::requests);
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

void BufferRequestStatistics::LiveBuffers::add(const size_t bufferBytes)
{
    const auto nowBuffers = buffers.fetch_add(1, std::memory_order_relaxed) + 1;
    auto previousPeakBuffers = peakBuffers.load(std::memory_order_relaxed);
    while (nowBuffers > previousPeakBuffers && not peakBuffers.compare_exchange_weak(previousPeakBuffers, nowBuffers, std::memory_order_relaxed))
    {
    }
    const auto nowBytes = bytes.fetch_add(bufferBytes, std::memory_order_relaxed) + bufferBytes;
    auto previousPeakBytes = peakBytes.load(std::memory_order_relaxed);
    while (nowBytes > previousPeakBytes && not peakBytes.compare_exchange_weak(previousPeakBytes, nowBytes, std::memory_order_relaxed))
    {
    }
}

void BufferRequestStatistics::LiveBuffers::remove(const size_t bufferBytes)
{
    buffers.fetch_sub(1, std::memory_order_relaxed);
    bytes.fetch_sub(bufferBytes, std::memory_order_relaxed);
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
