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
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <stop_token>
#include <string>
#include <string_view>
#include <Runtime/BufferProviderStatisticListener.hpp>
#include <Sources/InProcessFeed.hpp>
#include <Thread.hpp>

namespace NES
{

/// Publishes what the buffer providers are doing as a regular time series of CSV rows in a named
/// 'InProcessFeed', which an 'InProcess' source turns into an ordinary stream. Buffer pool pressure
/// thereby becomes something NebulaStream can query itself.
///
/// Unlike the query engine's task events, buffer events fire on every buffer handoff, which is far too
/// often to publish one row each. 'onEvent' therefore only bumps relaxed atomic counters, and a dedicated
/// thread emits exactly one row per flush interval. That has two consequences worth knowing:
///  - The row rate is bounded by the wall clock rather than by the event rate, so the query that reads the
///    feed cannot amplify its own buffer usage into more rows. No self-exclusion is needed, unlike in
///    'TaskStatisticListener'.
///  - Individual events are not observable in the published stream. Any other
///    'BufferProviderStatisticListener' still sees them one by one.
struct BufferStatisticListener final : BufferProviderStatisticListener
{
    /// Column names of the published rows, in order. A logical source reading the feed has to declare
    /// exactly these fields, all UINT64.
    static constexpr std::string_view SCHEMA
        = "ts_us,interval_ms,pooled_total,pooled_available,pooled_available_min,pooled_acquired,pooled_recycled,"
          "pooled_request_failures,unpooled_allocated,unpooled_bytes_requested,unpooled_bytes_in_use,"
          "unpooled_chunks_allocated,unpooled_chunks_released,unpooled_request_failures,rows_dropped";

    BufferStatisticListener(const std::string& feedName, size_t feedCapacity, std::chrono::milliseconds flushInterval);
    ~BufferStatisticListener() override = default;

    BufferStatisticListener(const BufferStatisticListener&) = delete;
    BufferStatisticListener& operator=(const BufferStatisticListener&) = delete;
    BufferStatisticListener(BufferStatisticListener&&) = delete;
    BufferStatisticListener& operator=(BufferStatisticListener&&) = delete;

    /// Runs on whichever thread touched a buffer. Does nothing but bump relaxed atomics, so it neither
    /// blocks nor allocates nor drops.
    void onEvent(BufferEvent event) override;

    /// Starts the thread that publishes one row per flush interval. Must be called after construction.
    void start();

    /// Renders the counters accumulated since the previous call as one CSV row and resets them. Public so
    /// that a test can drive a flush without a thread.
    std::string flushRow(std::chrono::system_clock::time_point now);

private:
    /// Sentinel for 'pooledAvailableMin': no acquire or recycle was observed in the current interval.
    static constexpr size_t NO_OBSERVATION = static_cast<size_t>(-1);

    void threadRoutine(const std::stop_token& token);

    std::chrono::milliseconds flushInterval;
    std::shared_ptr<InProcessFeed> feed;

    /// Latched from 'BufferPoolCreated', so a consumer sees the pool's dimensions on every row.
    std::atomic<size_t> pooledTotal{0};

    /// Gauges: the value most recently observed, not reset on flush.
    std::atomic<size_t> pooledAvailable{0};
    std::atomic<size_t> unpooledBytesInUse{0};

    /// The low water mark of the pool within the current interval. Reset to 'no observation' on flush,
    /// where the flush then falls back to 'pooledAvailable'.
    std::atomic<size_t> pooledAvailableMin{NO_OBSERVATION};

    /// Counters, reset on flush.
    std::atomic<size_t> pooledAcquired{0};
    std::atomic<size_t> pooledRecycled{0};
    std::atomic<size_t> pooledRequestFailures{0};
    std::atomic<size_t> unpooledAllocated{0};
    std::atomic<size_t> unpooledBytesRequested{0};
    std::atomic<size_t> unpooledChunksAllocated{0};
    std::atomic<size_t> unpooledChunksReleased{0};
    std::atomic<size_t> unpooledRequestFailures{0};

    /// When the previous row was published, so that a row can report the interval it actually covers rather
    /// than the configured one. Only touched by the flushing thread.
    std::chrono::system_clock::time_point lastFlushAt = std::chrono::system_clock::now();

    /// Lets the flushing thread wait for the next interval while still reacting promptly to a stop request.
    std::mutex flushMutex;
    std::condition_variable_any flushWakeUp;

    /// Must be declared last so it is destroyed first, ensuring the thread stops before the feed goes away
    Thread flushThread;
};

}
