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
#include <stop_token>
#include <string>
#include <variant>
#include <Feeds/EventFeed.hpp>
#include <Identifiers/Identifiers.hpp>
#include <folly/MPMCQueue.h>
#include <QueryEngineStatisticListener.hpp>
#include <Thread.hpp>

namespace NES
{

/// Publishes the query engine's task events as CSV rows into this worker's task event feed, which an
/// 'EngineEvents' source turns into an ordinary stream. Engine statistics thereby become something
/// NebulaStream can query itself, rather than an offline trace file.
///
/// 'onEvent' runs on the engine's worker threads, so it does no more than filter and enqueue; formatting
/// happens on a dedicated thread. Both stages are bounded and drop rather than block, following
/// 'GoogleEventTracePrinter'.
///
/// A query that reads the feed is reported like any other, so its own tasks show up in the rows it reads.
/// This does not feed back without bound, because the source only emits a buffer per flush interval, and
/// the few tasks each buffer causes become rows of a later buffer. Analyses that are not interested in the
/// observer's own cost can filter its query id out.
struct TaskStatisticListener final : QueryEngineStatisticListener
{
    TaskStatisticListener(const Host& host, size_t feedCapacity);
    ~TaskStatisticListener() override = default;

    TaskStatisticListener(const TaskStatisticListener&) = delete;
    TaskStatisticListener& operator=(const TaskStatisticListener&) = delete;
    TaskStatisticListener(TaskStatisticListener&&) = delete;
    TaskStatisticListener& operator=(TaskStatisticListener&&) = delete;

    void onEvent(Event event) override;

    /// Starts the thread that formats queued events. Must be called after construction.
    void start();

private:
    static constexpr size_t QUEUE_LENGTH = 4096;

    /// Only the events that end up as rows travel through the queue.
    using QueuedEvent = std::variant<TaskExecutionStart, TaskEmit, TaskExecutionComplete, TaskExpired>;

    void threadRoutine(const std::stop_token& token);

    Host host;
    EventFeedProducer feed;
    folly::MPMCQueue<QueuedEvent> events{QUEUE_LENGTH};

    /// Must be declared last so it is destroyed first, ensuring the thread stops before the feed goes away
    Thread formattingThread;
};

}
