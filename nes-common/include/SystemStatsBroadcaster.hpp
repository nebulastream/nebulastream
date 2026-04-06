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
#include <deque>
#include <memory>
#include <mutex>
#include <vector>

#include <WorkerLocalSingleton.hpp>
#include <SingleNodeWorkerRPCService.pb.h>

namespace NES
{

/// A single subscriber's bounded event queue. Non-blocking push, blocking pop with timeout.
class SystemStatsSubscriber
{
public:
    explicit SystemStatsSubscriber(size_t capacity = 4096) : maxSize(capacity) { }

    /// Non-blocking push. Returns false (drops event) if queue is full or closed.
    bool tryPush(const SystemStatEvent& event)
    {
        if (closed.load(std::memory_order_relaxed))
        {
            return false;
        }
        std::lock_guard lock(mutex);
        if (queue.size() >= maxSize)
        {
            return false; // queue full — drop
        }
        queue.push_back(event);
        cv.notify_one();
        return true;
    }

    /// Blocking pop with deadline. Returns false if closed or timed out.
    bool tryPopUntil(std::chrono::steady_clock::time_point deadline, SystemStatEvent& event)
    {
        std::unique_lock lock(mutex);
        cv.wait_until(lock, deadline, [this] { return !queue.empty() || closed.load(std::memory_order_relaxed); });
        if (queue.empty())
        {
            return false;
        }
        event = std::move(queue.front());
        queue.pop_front();
        return true;
    }

    /// Signal this subscriber to stop. Wakes any blocked pop.
    void close()
    {
        closed.store(true, std::memory_order_relaxed);
        cv.notify_all();
    }

    [[nodiscard]] bool isClosed() const { return closed.load(std::memory_order_relaxed); }

private:
    size_t maxSize;
    std::deque<SystemStatEvent> queue;
    std::atomic<bool> closed{false};
    std::mutex mutex;
    std::condition_variable cv;
};

/// Thread-safe broadcast hub for system stat events.
/// Constructed as a WorkerLocalSingleton before the SingleNodeWorker so that
/// GrpcStreamSink can access it via SystemStatsBroadcaster::instance().
class SystemStatsBroadcaster : public WorkerLocalSingleton<SystemStatsBroadcaster>
{
public:
    SystemStatsBroadcaster() = default;

    /// Push an event to all active subscribers. Non-blocking.
    /// If no subscribers, this is a no-op (zero overhead).
    void broadcast(const SystemStatEvent& event);

    /// Create a new subscriber. The subscriber receives all future events.
    std::shared_ptr<SystemStatsSubscriber> subscribe();

    /// Remove a subscriber. Called when a gRPC client disconnects.
    void unsubscribe(const std::shared_ptr<SystemStatsSubscriber>& subscriber);

private:
    std::mutex mutex;
    std::vector<std::shared_ptr<SystemStatsSubscriber>> subscribers;
};

}
