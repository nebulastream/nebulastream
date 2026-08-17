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

#include <BufferStatisticListener.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <mutex>
#include <stop_token>
#include <string>
#include <utility>
#include <variant>
#include <Runtime/BufferProviderStatisticListener.hpp>
#include <Sources/InProcessFeed.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <fmt/format.h>
#include <Thread.hpp>

namespace NES
{

namespace
{
/// Lowers 'target' to 'observed' if that is smaller. There is no atomic fetch_min, and relaxed ordering is
/// enough: the value is only read by the flushing thread, which tolerates seeing a slightly stale minimum.
void lowerTo(std::atomic<size_t>& target, const size_t observed)
{
    auto current = target.load(std::memory_order_relaxed);
    while (observed < current && !target.compare_exchange_weak(current, observed, std::memory_order_relaxed))
    {
    }
}
}

BufferStatisticListener::BufferStatisticListener(
    const std::string& feedName, const size_t feedCapacity, const std::chrono::milliseconds flushInterval)
    : flushInterval(flushInterval), feed(InProcessFeedRegistry::instance().getOrCreate(feedName, feedCapacity))
{
    NES_INFO(
        "Publishing buffer statistics into the in-process feed '{}' with a capacity of {} rows, one row every {}",
        feedName,
        feed->capacity(),
        flushInterval);
}

void BufferStatisticListener::onEvent(BufferEvent event)
{
    std::visit(
        Overloaded{
            [this](const PooledBufferAcquired& acquired)
            {
                pooledAcquired.fetch_add(1, std::memory_order_relaxed);
                pooledAvailable.store(acquired.availableAfter, std::memory_order_relaxed);
                lowerTo(pooledAvailableMin, acquired.availableAfter);
            },
            [this](const PooledBufferRecycled& recycled)
            {
                pooledRecycled.fetch_add(1, std::memory_order_relaxed);
                pooledAvailable.store(recycled.availableAfter, std::memory_order_relaxed);
                lowerTo(pooledAvailableMin, recycled.availableAfter);
            },
            [this](const PooledBufferRequestFailed&) { pooledRequestFailures.fetch_add(1, std::memory_order_relaxed); },
            [this](const UnpooledBufferAllocated& allocated)
            {
                unpooledAllocated.fetch_add(1, std::memory_order_relaxed);
                unpooledBytesRequested.fetch_add(allocated.requestedBytes, std::memory_order_relaxed);
                unpooledBytesInUse.store(allocated.unpooledBytesInUse, std::memory_order_relaxed);
            },
            [this](const UnpooledBufferRequestFailed& failed)
            {
                unpooledRequestFailures.fetch_add(1, std::memory_order_relaxed);
                unpooledBytesInUse.store(failed.unpooledBytesInUse, std::memory_order_relaxed);
            },
            [this](const UnpooledChunkAllocated& chunk)
            {
                unpooledChunksAllocated.fetch_add(1, std::memory_order_relaxed);
                unpooledBytesInUse.store(chunk.unpooledBytesInUse, std::memory_order_relaxed);
            },
            [this](const UnpooledChunkReleased& chunk)
            {
                unpooledChunksReleased.fetch_add(1, std::memory_order_relaxed);
                unpooledBytesInUse.store(chunk.unpooledBytesInUse, std::memory_order_relaxed);
            },
            [this](const BufferPoolCreated& created)
            {
                pooledTotal.store(created.numberOfBuffers, std::memory_order_relaxed);
                pooledAvailable.store(created.numberOfBuffers, std::memory_order_relaxed);
            },
            /// The pool is gone, but the counters of the interval it died in are still worth publishing, so
            /// nothing is reset here.
            [](const BufferPoolDestroyed&) {}},
        std::move(event));
}

std::string BufferStatisticListener::flushRow(const std::chrono::system_clock::time_point now)
{
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - lastFlushAt);
    lastFlushAt = now;

    const auto available = pooledAvailable.load(std::memory_order_relaxed);
    const auto observedMin = pooledAvailableMin.exchange(NO_OBSERVATION, std::memory_order_relaxed);

    return fmt::format(
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count(),
        elapsed.count(),
        pooledTotal.load(std::memory_order_relaxed),
        available,
        /// An interval without a single acquire or recycle never observed a fill level, so the gauge is the
        /// best answer we have for its minimum.
        observedMin == NO_OBSERVATION ? available : observedMin,
        pooledAcquired.exchange(0, std::memory_order_relaxed),
        pooledRecycled.exchange(0, std::memory_order_relaxed),
        pooledRequestFailures.exchange(0, std::memory_order_relaxed),
        unpooledAllocated.exchange(0, std::memory_order_relaxed),
        unpooledBytesRequested.exchange(0, std::memory_order_relaxed),
        unpooledBytesInUse.load(std::memory_order_relaxed),
        unpooledChunksAllocated.exchange(0, std::memory_order_relaxed),
        unpooledChunksReleased.exchange(0, std::memory_order_relaxed),
        unpooledRequestFailures.exchange(0, std::memory_order_relaxed),
        feed->droppedRows());
}

void BufferStatisticListener::threadRoutine(const std::stop_token& token)
{
    std::unique_lock lock(flushMutex);
    while (!token.stop_requested())
    {
        /// An interruptible timed wait: returns early when the worker shuts down, so teardown does not have
        /// to sit out a whole flush interval.
        flushWakeUp.wait_for(lock, token, flushInterval, [&token] { return token.stop_requested(); });
        if (token.stop_requested())
        {
            break;
        }
        /// A row is published even when nothing happened, so that the series stays regular and an idle engine
        /// is visible as zeros rather than as a gap.
        feed->tryPush(flushRow(std::chrono::system_clock::now()));
    }
}

void BufferStatisticListener::start()
{
    flushThread = Thread("buffer-stats", [this](const std::stop_token& stopToken) { threadRoutine(stopToken); });
}

}
