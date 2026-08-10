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

#include <cstddef>
#include <memory>
#include <stop_token>
#include <string>
#include <variant>
#include <Listeners/StatisticListener.hpp>
#include <Listeners/SystemEventListener.hpp>
#include <Sources/InProcessFeed.hpp>
#include <folly/MPMCQueue.h>
#include <QueryEngineStatisticListener.hpp>
#include <QueryId.hpp>
#include <Thread.hpp>

namespace NES
{

/// Publishes the query engine's task events as CSV rows into a named 'InProcessFeed', which an 'InProcess'
/// source turns into an ordinary stream. Engine statistics thereby become something NebulaStream can query
/// itself, rather than an offline trace file.
///
/// 'onEvent' runs on the engine's worker threads, so it does no more than filter and enqueue; formatting
/// happens on a dedicated thread. Both stages are bounded and drop rather than block, following
/// 'GoogleEventTracePrinter'.
struct TaskStatisticListener final : StatisticListener
{
    TaskStatisticListener(const std::string& feedName, size_t feedCapacity);
    ~TaskStatisticListener() override = default;

    TaskStatisticListener(const TaskStatisticListener&) = delete;
    TaskStatisticListener& operator=(const TaskStatisticListener&) = delete;
    TaskStatisticListener(TaskStatisticListener&&) = delete;
    TaskStatisticListener& operator=(TaskStatisticListener&&) = delete;

    void onEvent(Event event) override;
    void onEvent(SystemEvent event) override;

    /// Starts the thread that formats queued events. Must be called after construction.
    void start();

private:
    static constexpr size_t QUEUE_LENGTH = 4096;

    /// Tells the formatting thread to ignore everything belonging to a query, see 'onEvent(SystemEvent)'.
    struct ExcludeQuery
    {
        QueryId queryId = INVALID_QUERY_ID;
    };

    /// Only the events that end up as rows travel through the queue, plus the exclusion markers. Routing
    /// the markers through the same queue keeps them ordered against the events they suppress, and leaves
    /// the exclusion set owned exclusively by the formatting thread.
    using QueuedEvent = std::variant<TaskExecutionStart, TaskEmit, TaskExecutionComplete, TaskExpired, ExcludeQuery>;

    void threadRoutine(const std::stop_token& token);

    std::string feedName;
    std::shared_ptr<InProcessFeed> feed;
    folly::MPMCQueue<QueuedEvent> events{QUEUE_LENGTH};

    /// Must be declared last so it is destroyed first, ensuring the thread stops before the feed goes away
    Thread formattingThread;
};

}
