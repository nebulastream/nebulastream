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

#include <TaskStatisticListener.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <Feeds/EventFeed.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <fmt/format.h>
#include <folly/MPMCQueue.h>
#include <QueryEngineStatisticListener.hpp>
#include <Thread.hpp>

namespace NES
{

namespace
{
constexpr std::chrono::milliseconds READ_RETRY{100};
/// Log every nth dropped event to avoid clogging the log when the queue is full
constexpr uint64_t DROP_LOG_INTERVAL = 100;

void warnOnOverflow(const bool writeFailed)
{
    if (writeFailed) [[unlikely]]
    {
        static std::atomic<uint64_t> droppedCount{0};
        /// Log first drop immediately, then every DROP_LOG_INTERVAL
        if (const uint64_t dropped = droppedCount.fetch_add(1, std::memory_order_relaxed) + 1;
            dropped == 1 || dropped % DROP_LOG_INTERVAL == 0)
        {
            NES_WARNING("Task statistics queue full, {} events dropped so far", dropped);
        }
    }
}

uint64_t timestampToMicroseconds(const ChronoClock::time_point& timestamp)
{
    return std::chrono::duration_cast<std::chrono::microseconds>(timestamp.time_since_epoch()).count();
}

/// Renders one row of the schema (event_type VARSIZED, ts_us UINT64, thread_id UINT64, query_id INT64,
/// pipeline_id UINT64, task_id UINT64, tuples UINT64).
/// Columns that an event does not carry are zeroed.
std::string
formatRow(const std::string_view eventType, const EventBase& event, const uint64_t pipelineId, const uint64_t taskId, const size_t tuples)
{
    return fmt::format(
        "{},{},{},{},{},{},{}",
        eventType,
        timestampToMicroseconds(event.timestamp),
        event.threadId.getRawValue(),
        event.queryId.getRawValue(),
        pipelineId,
        taskId,
        tuples);
}
}

TaskStatisticListener::TaskStatisticListener(const Host& host, const size_t feedCapacity)
    : host(host), feed(EventFeedRegistry::instance().create(host, FeedName::ENGINE_EVENTS, feedCapacity))
{
    NES_INFO("Publishing the task events of worker {}, buffered up to {} rows", host.getRawValue(), feed->capacity());
}

void TaskStatisticListener::onEvent(Event event)
{
    /// Nothing reads the feed until a query attaches to it, and until then formatting and queueing events
    /// would only produce rows to be dropped again.
    if (!feed->hasConsumer())
    {
        return;
    }

    /// Everything that does not become a row is discarded here, on the worker thread, so that the queue is
    /// not filled with events nobody reads.
    auto queued = std::visit(
        Overloaded{
            [](const TaskExecutionStart& taskStart) { return std::optional<QueuedEvent>{taskStart}; },
            [](const TaskEmit& taskEmit) { return std::optional<QueuedEvent>{taskEmit}; },
            [](const TaskExecutionComplete& taskComplete) { return std::optional<QueuedEvent>{taskComplete}; },
            [](const TaskExpired& taskExpired) { return std::optional<QueuedEvent>{taskExpired}; },
            [](const auto&) { return std::optional<QueuedEvent>{}; }},
        std::move(event));

    if (queued.has_value())
    {
        /// 'write' rather than 'writeIfNotFull', which may wait for a concurrent read to finish. A worker thread
        /// must not wait here, and this path is allowed to drop.
        warnOnOverflow(!events.write(std::move(queued.value())));
    }
}

void TaskStatisticListener::threadRoutine(const std::stop_token& token)
{
    while (!token.stop_requested())
    {
        QueuedEvent event;
        if (!events.tryReadUntil(std::chrono::steady_clock::now() + READ_RETRY, event))
        {
            continue;
        }

        feed->push(
            std::visit(
                Overloaded{
                    [](const TaskExecutionStart& taskStart)
                    {
                        return formatRow(
                            "TASK_START",
                            taskStart,
                            taskStart.pipelineId.getRawValue(),
                            taskStart.taskId.getRawValue(),
                            taskStart.numberOfTuples);
                    },
                    [](const TaskEmit& taskEmit)
                    {
                        return formatRow(
                            "TASK_EMIT",
                            taskEmit,
                            taskEmit.fromPipeline.getRawValue(),
                            taskEmit.taskId.getRawValue(),
                            taskEmit.numberOfTuples);
                    },
                    [](const TaskExecutionComplete& taskComplete)
                    {
                        return formatRow(
                            "TASK_DONE", taskComplete, taskComplete.pipelineId.getRawValue(), taskComplete.taskId.getRawValue(), 0);
                    },
                    [](const TaskExpired& taskExpired)
                    {
                        return formatRow(
                            "TASK_EXPIRED", taskExpired, taskExpired.pipelineId.getRawValue(), taskExpired.taskId.getRawValue(), 0);
                    }},
                event));
    }
}

void TaskStatisticListener::start()
{
    formattingThread = Thread("task-stats", [this](const std::stop_token& stopToken) { threadRoutine(stopToken); });
}

}
