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
#include <unordered_set>
#include <utility>
#include <variant>
#include <Listeners/SystemEventListener.hpp>
#include <Sources/InProcessFeed.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <folly/MPMCQueue.h>
#include <QueryEngineStatisticListener.hpp>
#include <QueryId.hpp>
#include <Thread.hpp>

namespace NES
{

namespace
{
constexpr std::chrono::milliseconds READ_RETRY{100};
/// Log every nth dropped event to avoid clogging the log when the queue is full
constexpr uint64_t DROP_LOG_INTERVAL = 100;

/// A query that reads an InProcess source observes the events its own execution produces, which never
/// settles: every buffer it emits creates tasks, which create the events that fill the next buffer.
/// The submitted plan is the earliest place where such a query can be recognised. Matching on the plan's
/// debug rendering is what makes this cheap; a dedicated marker on the descriptor would be less brittle.
/// The comparison is case-insensitive because a source type reaches the descriptor as an 'Identifier',
/// which canonicalises it to upper case.
constexpr std::string_view IN_PROCESS_SOURCE_MARKER = "SOURCETYPE: INPROCESS";

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

/// Renders one row of the schema
/// (event_type, ts_us, thread_id, query_id, pipeline_id, task_id, tuples).
/// Columns that an event does not carry are zeroed.
std::string
formatRow(const std::string_view eventType, const EventBase& event, const uint64_t pipelineId, const uint64_t taskId, const size_t tuples)
{
    return fmt::format(
        "{},{},{},{},{},{},{}",
        eventType,
        timestampToMicroseconds(event.timestamp),
        event.threadId.getRawValue(),
        event.queryId.getLocalQueryId().getRawValue(),
        pipelineId,
        taskId,
        tuples);
}
}

TaskStatisticListener::TaskStatisticListener(const std::string& feedName, const size_t feedCapacity)
    : feedName(feedName), feed(InProcessFeedRegistry::instance().getOrCreate(feedName, feedCapacity))
{
    NES_INFO("Publishing task statistics into the in-process feed '{}' with a capacity of {} rows", feedName, feed->capacity());
}

void TaskStatisticListener::onEvent(Event event)
{
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
        warnOnOverflow(!events.writeIfNotFull(std::move(queued.value())));
    }
}

void TaskStatisticListener::onEvent(SystemEvent event)
{
    const auto* submit = std::get_if<SubmitQuerySystemEvent>(&event);
    if (submit == nullptr || toUpperCase(submit->query).find(IN_PROCESS_SOURCE_MARKER) == std::string::npos)
    {
        return;
    }

    NES_INFO("Excluding query {} from task statistics, it reads an InProcess source", submit->queryId);
    /// Submission is rare and happens before the query produces any event, so blocking here costs nothing
    /// and guarantees the marker is not dropped when the queue happens to be full.
    events.blockingWrite(QueuedEvent{ExcludeQuery{submit->queryId}});
}

void TaskStatisticListener::threadRoutine(const std::stop_token& token)
{
    /// Owned exclusively by this thread. The markers arrive through the same queue as the events they
    /// suppress, and a query is submitted before it can produce any event, so ordering is guaranteed.
    std::unordered_set<QueryId> excludedQueries;

    while (!token.stop_requested())
    {
        QueuedEvent event;
        if (!events.tryReadUntil(std::chrono::steady_clock::now() + READ_RETRY, event))
        {
            continue;
        }

        std::visit(
            Overloaded{
                [&](const ExcludeQuery& exclude) { excludedQueries.emplace(exclude.queryId); },
                [&](const TaskExecutionStart& taskStart)
                {
                    if (!excludedQueries.contains(taskStart.queryId))
                    {
                        feed->tryPush(formatRow(
                            "TASK_START",
                            taskStart,
                            taskStart.pipelineId.getRawValue(),
                            taskStart.taskId.getRawValue(),
                            taskStart.numberOfTuples));
                    }
                },
                [&](const TaskEmit& taskEmit)
                {
                    if (!excludedQueries.contains(taskEmit.queryId))
                    {
                        feed->tryPush(formatRow(
                            "TASK_EMIT",
                            taskEmit,
                            taskEmit.fromPipeline.getRawValue(),
                            taskEmit.taskId.getRawValue(),
                            taskEmit.numberOfTuples));
                    }
                },
                [&](const TaskExecutionComplete& taskComplete)
                {
                    if (!excludedQueries.contains(taskComplete.queryId))
                    {
                        feed->tryPush(formatRow(
                            "TASK_DONE", taskComplete, taskComplete.pipelineId.getRawValue(), taskComplete.taskId.getRawValue(), 0));
                    }
                },
                [&](const TaskExpired& taskExpired)
                {
                    if (!excludedQueries.contains(taskExpired.queryId))
                    {
                        feed->tryPush(formatRow(
                            "TASK_EXPIRED", taskExpired, taskExpired.pipelineId.getRawValue(), taskExpired.taskId.getRawValue(), 0));
                    }
                }},
            event);
    }
}

void TaskStatisticListener::start()
{
    formattingThread = Thread("task-stats", [this](const std::stop_token& stopToken) { threadRoutine(stopToken); });
}

}
