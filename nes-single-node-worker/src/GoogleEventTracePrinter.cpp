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

#include <GoogleEventTracePrinter.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <ios>
#include <stop_token>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <variant>
#include <unistd.h>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/SystemEventListener.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Strings.hpp>
#include <fmt/ostream.h>
#include <folly/MPMCQueue.h>
#include <QueryEngineStatisticListener.hpp>
#include <scope_guard.hpp>

namespace NES
{

constexpr uint64_t READ_RETRY_MS = 100;
constexpr uint64_t SYSTEM_THREAD = 0;
/// Log every nth dropped event to avoid clogging the log when the queue is full
constexpr uint64_t DROP_LOG_INTERVAL = 100;

namespace
{
/// Escape a string for JSON output (handles quotes in addition to special chars)
std::string escapeForJson(std::string_view str)
{
    const std::string result = escapeSpecialCharacters(str);
    /// Also escape double quotes for JSON
    std::string final;
    final.reserve(result.size());
    for (const char character : result)
    {
        if (character == '"')
        {
            final += "\\\"";
        }
        else
        {
            final += character;
        }
    }
    return final;
}
}

uint64_t GoogleEventTracePrinter::timestampToMicroseconds(const std::chrono::system_clock::time_point& timestamp)
{
    return std::chrono::duration_cast<std::chrono::microseconds>(timestamp.time_since_epoch()).count();
}

void GoogleEventTracePrinter::threadRoutine(const std::stop_token& token)
{
    const auto pid = getpid();
    std::ofstream file(outputPath, std::ios::out | std::ios::trunc);
    if (!file.is_open())
    {
        NES_ERROR("Failed to open trace file: {}", outputPath);
        return;
    }

    /// Write the trace header
    fmt::println(file, R"({{)");
    fmt::println(file, R"(  "traceEvents": [)");
    SCOPE_EXIT
    {
        /// Write the footer when the thread stops
        fmt::println(file, "");
        fmt::println(file, R"(  ])");
        fmt::println(file, R"(}})");
        file.close();
    };

    /// Helper to print comma before event (except first)
    auto printComma = [&file, firstEvent = true]() mutable
    {
        if (!firstEvent)
        {
            fmt::print(file, ",\n");
        }
        firstEvent = false;
    };

    while (!token.stop_requested())
    {
        CombinedEventType event = QueryStart{WorkerThreadId(0), QueryId(0)}; /// Will be overwritten

        if (!events.tryReadUntil(std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(READ_RETRY_MS), event))
        {
            continue;
        }

        std::visit(
            Overloaded{
                [&](const SubmitQuerySystemEvent& submitEvent)
                {
                    printComma();
                    fmt::print(
                        file,
                        R"(    {{"args":{{"query":"{}"}},"cat":"system","name":"Submit Query {}","ph":"i","pid":{},"tid":{},"ts":{}}})",
                        escapeForJson(submitEvent.query),
                        submitEvent.queryId,
                        pid,
                        SYSTEM_THREAD,
                        timestampToMicroseconds(submitEvent.timestamp));
                },
                [&](const StartQuerySystemEvent& startEvent)
                {
                    printComma();
                    fmt::print(
                        file,
                        R"(    {{"cat":"system","name":"Start Query {}","ph":"i","pid":{},"tid":{},"ts":{}}})",
                        startEvent.queryId,
                        pid,
                        SYSTEM_THREAD,
                        timestampToMicroseconds(startEvent.timestamp));
                },
                [&](const StopQuerySystemEvent& stopEvent)
                {
                    printComma();
                    fmt::print(
                        file,
                        R"(    {{"cat":"system","name":"Stop Query {}","ph":"i","pid":{},"tid":{},"ts":{}}})",
                        stopEvent.queryId,
                        pid,
                        SYSTEM_THREAD,
                        timestampToMicroseconds(stopEvent.timestamp));
                },
                [&](const QueryStart& queryStart)
                {
                    printComma();
                    fmt::print(
                        file,
                        R"(    {{"cat":"query","name":"Query {}","ph":"B","pid":{},"tid":{},"ts":{}}})",
                        queryStart.queryId,
                        pid,
                        queryStart.threadId.getRawValue(),
                        timestampToMicroseconds(queryStart.timestamp));

                    /// Track active query with thread ID
                    activeQueries.emplace(queryStart.queryId, std::make_pair(queryStart.timestamp, queryStart.threadId));
                },
                [&](const QueryFail& queryFail)
                {
                    /// Use the thread ID from the QueryFail event to ensure matching begin/end pairs
                    auto it = activeQueries.find(queryFail.queryId);
                    uint64_t originalTid
                        = (it != activeQueries.end()) ? it->second.second.getRawValue() : queryFail.threadId.getRawValue(); /// fallback

                    printComma();
                    fmt::print(
                        file,
                        R"(    {{"cat":"query","name":"Query {}","ph":"E","pid":{},"tid":{},"ts":{}}})",
                        queryFail.queryId,
                        pid,
                        originalTid,
                        timestampToMicroseconds(queryFail.timestamp));

                    /// Remove from active queries
                    activeQueries.erase(queryFail.queryId);
                },
                [&](const QueryStopRequest& queryStopRequest)
                {
                    printComma();
                    fmt::print(
                        file,
                        R"(    {{"cat":"query","name":"QueryStopRequest {}","ph":"i","pid":{},"tid":{},"ts":{}}})",
                        queryStopRequest.queryId,
                        pid,
                        queryStopRequest.threadId.getRawValue(),
                        timestampToMicroseconds(queryStopRequest.timestamp));
                },
                [&](const QueryStop& queryStop)
                {
                    /// Use the thread ID from the QueryStop event to ensure matching begin/end pairs
                    auto it = activeQueries.find(queryStop.queryId);
                    uint64_t originalTid
                        = (it != activeQueries.end()) ? it->second.second.getRawValue() : queryStop.threadId.getRawValue(); /// fallback

                    printComma();
                    fmt::print(
                        file,
                        R"(    {{"cat":"query","name":"Query {}","ph":"E","pid":{},"tid":{},"ts":{}}})",
                        queryStop.queryId,
                        pid,
                        originalTid,
                        timestampToMicroseconds(queryStop.timestamp));

                    /// Remove from active queries
                    activeQueries.erase(queryStop.queryId);
                },
                [&](const PipelineStart& pipelineStart)
                {
                    printComma();
                    fmt::print(
                        file,
                        R"x(    {{"args":{{"pipeline_id":{}}},"cat":"pipeline","name":"Pipeline {} (Query {})","ph":"B","pid":{},"tid":{},"ts":{}}})x",
                        pipelineStart.pipelineId.getRawValue(),
                        pipelineStart.pipelineId,
                        pipelineStart.queryId,
                        pid,
                        pipelineStart.threadId.getRawValue(),
                        timestampToMicroseconds(pipelineStart.timestamp));

                    /// Track active pipeline with thread ID
                    activePipelines.emplace(
                        pipelineStart.pipelineId, std::make_tuple(pipelineStart.queryId, pipelineStart.timestamp, pipelineStart.threadId));
                },
                [&](const PipelineStop& pipelineStop)
                {
                    /// Use the thread ID from the PipelineStart event to ensure matching begin/end pairs
                    auto it = activePipelines.find(pipelineStop.pipelineId);
                    uint64_t originalTid = (it != activePipelines.end()) ? std::get<2>(it->second).getRawValue()
                                                                         : pipelineStop.threadId.getRawValue(); /// fallback

                    printComma();
                    fmt::print(
                        file,
                        R"x(    {{"args":{{"pipeline_id":{}}},"cat":"pipeline","name":"Pipeline {} (Query {})","ph":"E","pid":{},"tid":{},"ts":{}}})x",
                        pipelineStop.pipelineId.getRawValue(),
                        pipelineStop.pipelineId,
                        pipelineStop.queryId,
                        pid,
                        originalTid,
                        timestampToMicroseconds(pipelineStop.timestamp));

                    /// Remove from active pipelines
                    activePipelines.erase(pipelineStop.pipelineId);
                },
                [&](const TaskExecutionStart& taskStart)
                {
                    printComma();
                    fmt::print(
                        file,
                        R"x(    {{"args":{{"pipeline_id":{},"task_id":{},"tuples":{}}},"cat":"task","name":"Task {} (Pipeline {}, Query {})","ph":"B","pid":{},"tid":{},"ts":{}}})x",
                        taskStart.pipelineId.getRawValue(),
                        taskStart.taskId.getRawValue(),
                        taskStart.numberOfTuples,
                        taskStart.taskId,
                        taskStart.pipelineId,
                        taskStart.queryId,
                        pid,
                        taskStart.threadId.getRawValue(),
                        timestampToMicroseconds(taskStart.timestamp));

                    /// Track this task for duration calculation
                    activeTasks.emplace(taskStart.taskId, taskStart.timestamp);
                },
                [&](const TaskExecutionComplete& taskComplete)
                {
                    uint64_t duration = 0;
                    auto it = activeTasks.find(taskComplete.taskId);
                    if (it != activeTasks.end())
                    {
                        duration = timestampToMicroseconds(taskComplete.timestamp) - timestampToMicroseconds(it->second);
                        activeTasks.erase(it);
                    }

                    printComma();
                    fmt::print(
                        file,
                        R"x(    {{"args":{{"pipeline_id":{},"task_id":{}}},"cat":"task","dur":{},"name":"Task {} (Pipeline {}, Query {})","ph":"E","pid":{},"tid":{},"ts":{}}})x",
                        taskComplete.pipelineId.getRawValue(),
                        taskComplete.taskId.getRawValue(),
                        duration,
                        taskComplete.taskId,
                        taskComplete.pipelineId,
                        taskComplete.queryId,
                        pid,
                        taskComplete.threadId.getRawValue(),
                        timestampToMicroseconds(taskComplete.timestamp));
                },
                [&](const TaskEmit& taskEmit)
                {
                    printComma();
                    fmt::print(
                        file,
                        R"x(    {{"args":{{"from_pipeline":{},"task_id":{},"to_pipeline":{},"tuples":{}}},"cat":"task","name":"Emit {}->{} (Task {}, Query {})","ph":"i","pid":{},"tid":{},"ts":{}}})x",
                        taskEmit.fromPipeline.getRawValue(),
                        taskEmit.taskId.getRawValue(),
                        taskEmit.toPipeline.getRawValue(),
                        taskEmit.numberOfTuples,
                        taskEmit.fromPipeline,
                        taskEmit.toPipeline,
                        taskEmit.taskId,
                        taskEmit.queryId,
                        pid,
                        taskEmit.threadId.getRawValue(),
                        timestampToMicroseconds(taskEmit.timestamp));
                },
                [&](const TaskExpired& taskExpired)
                {
                    printComma();
                    fmt::print(
                        file,
                        R"x(    {{"args":{{"pipeline_id":{},"task_id":{}}},"cat":"task","name":"Task Expired {} (Pipeline {}, Query {})","ph":"i","pid":{},"tid":{},"ts":{}}})x",
                        taskExpired.pipelineId.getRawValue(),
                        taskExpired.taskId.getRawValue(),
                        taskExpired.taskId,
                        taskExpired.pipelineId,
                        taskExpired.queryId,
                        pid,
                        taskExpired.threadId.getRawValue(),
                        timestampToMicroseconds(taskExpired.timestamp));

                    /// Remove from active tasks if present
                    activeTasks.erase(taskExpired.taskId);
                }},
            event);
    }
}

namespace
{
void warnOnOverflow(bool writeFailed)
{
    if (writeFailed) [[unlikely]]
    {
        static std::atomic<uint64_t> droppedCount{0};
        /// Log first drop immediately, then every DROP_LOG_INTERVAL
        if (uint64_t dropped = droppedCount.fetch_add(1, std::memory_order_relaxed) + 1; dropped == 1 || dropped % DROP_LOG_INTERVAL == 0)
        {
            NES_WARNING("Event queue full, {} events dropped so far", dropped);
        }
    }
}
}

void GoogleEventTracePrinter::onEvent(Event event)
{
    warnOnOverflow(
        !events.writeIfNotFull(std::visit([]<typename T>(T&& arg) { return CombinedEventType(std::forward<T>(arg)); }, std::move(event))));
}

void GoogleEventTracePrinter::onEvent(SystemEvent event)
{
    warnOnOverflow(
        !events.writeIfNotFull(std::visit([]<typename T>(T&& arg) { return CombinedEventType(std::forward<T>(arg)); }, std::move(event))));
}

GoogleEventTracePrinter::GoogleEventTracePrinter(const std::filesystem::path& path) : outputPath(path)
{
    NES_INFO("Will write Google Event Trace to: {}", path);
}

void GoogleEventTracePrinter::start()
{
    traceThread = Thread("event-trace", [this](const std::stop_token& stopToken) { threadRoutine(stopToken); });
}

}
