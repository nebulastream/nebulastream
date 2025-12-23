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
#include <Identifiers/Identifiers.hpp>
#include <Listeners/SystemEventListener.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <fmt/format.h>
#include <folly/MPMCQueue.h>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
#include <QueryEngineStatisticListener.hpp>

namespace NES
{

constexpr uint64_t READ_RETRY_MS = 100;

uint64_t GoogleEventTracePrinter::timestampToMicroseconds(const std::chrono::system_clock::time_point& timestamp)
{
    return std::chrono::duration_cast<std::chrono::microseconds>(timestamp.time_since_epoch()).count();
}

nlohmann::json GoogleEventTracePrinter::createTraceEvent(
    const std::string& name, Category cat, Phase phase, uint64_t timestamp, uint64_t dur, const nlohmann::json& args)
{
    nlohmann::json event;
    event["name"] = name;

    /// Convert category enum to string
    switch (cat)
    {
        case Category::Query:
            event["cat"] = "query";
            break;
        case Category::Pipeline:
            event["cat"] = "pipeline";
            break;
        case Category::Task:
            event["cat"] = "task";
            break;
        case Category::System:
            event["cat"] = "system";
            break;
    }

    /// Convert phase enum to string
    switch (phase)
    {
        case Phase::Begin:
            event["ph"] = "B";
            break;
        case Phase::End:
            event["ph"] = "E";
            break;
        case Phase::Instant:
            event["ph"] = "i";
            break;
    }

    event["ts"] = timestamp;
    event["pid"] = 1; /// Process ID
    event["tid"] = 1; /// Thread ID (will be overridden for specific events)

    if (dur > 0)
    {
        event["dur"] = dur;
    }

    if (!args.empty())
    {
        event["args"] = args;
    }

    return event;
}

void GoogleEventTracePrinter::writeTraceHeader()
{
    bool expected = false;
    if (headerWritten.compare_exchange_strong(expected, true))
    {
        file << "{\n";
        file << "  \"traceEvents\": [\n";
    }
}

void GoogleEventTracePrinter::writeTraceFooter()
{
    bool expected = false;
    if (footerWritten.compare_exchange_strong(expected, true))
    {
        file << "\n  ]\n";
        file << "}\n";
    }
}

void GoogleEventTracePrinter::threadRoutine(const std::stop_token& token)
{
    writeTraceHeader();

    bool firstEvent = true;

    /// Helper function to emit events with proper comma handling
    auto emit = [&](const nlohmann::json& evt)
    {
        if (!firstEvent)
        {
            file << ",\n";
        }
        firstEvent = false;
        file << "    " << evt.dump();
    };

    while (!token.stop_requested())
    {
        CombinedEventType event = QueryStart{WorkerThreadId(0), INVALID_LOCAL_QUERY_ID}; /// Will be overwritten

        if (!events.tryReadUntil(std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(READ_RETRY_MS), event))
        {
            continue;
        }

        std::visit(
            Overloaded{
                [&](const SubmitQuerySystemEvent& submitEvent)
                {
                    auto args = nlohmann::json::object();
                    args["query"] = submitEvent.query;

                    auto traceEvent = createTraceEvent(
                        fmt::format("Submit Query {}", submitEvent.queryId),
                        Category::System,
                        Phase::Instant,
                        timestampToMicroseconds(submitEvent.timestamp),
                        0,
                        args);
                    traceEvent["tid"] = 0; /// System thread

                    emit(traceEvent);
                },
                [&](const StartQuerySystemEvent& startEvent)
                {
                    auto traceEvent = createTraceEvent(
                        fmt::format("Start Query {}", startEvent.queryId),
                        Category::System,
                        Phase::Instant,
                        timestampToMicroseconds(startEvent.timestamp));
                    traceEvent["tid"] = 0; /// System thread

                    emit(traceEvent);
                },
                [&](const StopQuerySystemEvent& stopEvent)
                {
                    auto traceEvent = createTraceEvent(
                        fmt::format("Stop Query {}", stopEvent.queryId),
                        Category::System,
                        Phase::Instant,
                        timestampToMicroseconds(stopEvent.timestamp));
                    traceEvent["tid"] = 0; /// System thread

                    emit(traceEvent);
                },
                [&](const QueryStart& queryStart)
                {
                    auto traceEvent = createTraceEvent(
                        fmt::format("Query {}", queryStart.queryId),
                        Category::Query,
                        Phase::Begin,
                        timestampToMicroseconds(queryStart.timestamp));
                    traceEvent["tid"] = queryStart.threadId.getRawValue();

                    emit(traceEvent);

                    /// Track active query with thread ID
                    activeQueries.emplace(queryStart.queryId, std::make_pair(queryStart.timestamp, queryStart.threadId));
                    NES_DEBUG("Tracking query start: {} on thread {}", queryStart.queryId, queryStart.threadId.getRawValue());
                },
                [&](const QueryFail& queryFail)
                {
                    /// Use the thread ID from the QueryFail event to ensure matching begin/end pairs
                    auto it = activeQueries.find(queryFail.queryId);
                    uint64_t originalTid
                        = (it != activeQueries.end()) ? it->second.second.getRawValue() : queryFail.threadId.getRawValue(); /// fallback

                    auto traceEvent = createTraceEvent(
                        fmt::format("Query {}", queryFail.queryId),
                        Category::Query,
                        Phase::End,
                        timestampToMicroseconds(queryFail.timestamp));
                    traceEvent["tid"] = originalTid;

                    emit(traceEvent);

                    /// Remove from active queries
                    activeQueries.erase(queryFail.queryId);
                    NES_DEBUG("Query completed with a failure: {} on thread {}", queryFail.queryId, originalTid);
                },
                [&](const QueryStopRequest& queryStopRequest)
                {
                    auto traceEvent = createTraceEvent(
                        fmt::format("QueryStopRequest {}", queryStopRequest.queryId),
                        Category::Query,
                        Phase::Instant,
                        timestampToMicroseconds(queryStopRequest.timestamp));
                    traceEvent["tid"] = queryStopRequest.threadId.getRawValue();
                    emit(traceEvent);
                    NES_DEBUG("Query stop request: {} on thread {}", queryStopRequest.queryId, queryStopRequest.threadId);
                },
                [&](const QueryStop& queryStop)
                {
                    /// Use the thread ID from the QueryStop event to ensure matching begin/end pairs
                    auto it = activeQueries.find(queryStop.queryId);
                    uint64_t originalTid
                        = (it != activeQueries.end()) ? it->second.second.getRawValue() : queryStop.threadId.getRawValue(); /// fallback

                    auto traceEvent = createTraceEvent(
                        fmt::format("Query {}", queryStop.queryId),
                        Category::Query,
                        Phase::End,
                        timestampToMicroseconds(queryStop.timestamp));
                    traceEvent["tid"] = originalTid;

                    emit(traceEvent);

                    /// Remove from active queries
                    activeQueries.erase(queryStop.queryId);
                    NES_DEBUG("Query completed: {} on thread {}", queryStop.queryId, originalTid);
                },
                [&](const PipelineStart& pipelineStart)
                {
                    auto args = nlohmann::json::object();
                    args["pipeline_id"] = pipelineStart.pipelineId.getRawValue();

                    auto traceEvent = createTraceEvent(
                        fmt::format("Pipeline {} (Query {})", pipelineStart.pipelineId, pipelineStart.queryId),
                        Category::Pipeline,
                        Phase::Begin,
                        timestampToMicroseconds(pipelineStart.timestamp),
                        0,
                        args);
                    traceEvent["tid"] = pipelineStart.threadId.getRawValue();

                    emit(traceEvent);

                    /// Track active pipeline with thread ID
                    activePipelines.emplace(
                        pipelineStart.pipelineId, std::make_tuple(pipelineStart.queryId, pipelineStart.timestamp, pipelineStart.threadId));
                },
                [&](const PipelineStop& pipelineStop)
                {
                    auto args = nlohmann::json::object();
                    args["pipeline_id"] = pipelineStop.pipelineId.getRawValue();

                    /// Use the thread ID from the PipelineStart event to ensure matching begin/end pairs
                    auto it = activePipelines.find(pipelineStop.pipelineId);
                    uint64_t originalTid = (it != activePipelines.end()) ? std::get<2>(it->second).getRawValue()
                                                                         : pipelineStop.threadId.getRawValue(); /// fallback

                    auto traceEvent = createTraceEvent(
                        fmt::format("Pipeline {} (Query {})", pipelineStop.pipelineId, pipelineStop.queryId),
                        Category::Pipeline,
                        Phase::End,
                        timestampToMicroseconds(pipelineStop.timestamp),
                        0,
                        args);
                    traceEvent["tid"] = originalTid;

                    emit(traceEvent);

                    /// Remove from active pipelines
                    activePipelines.erase(pipelineStop.pipelineId);
                },
                [&](const TaskExecutionStart& taskStart)
                {
                    auto args = nlohmann::json::object();
                    args["pipeline_id"] = taskStart.pipelineId.getRawValue();
                    args["task_id"] = taskStart.taskId.getRawValue();
                    args["tuples"] = taskStart.numberOfTuples;

                    auto traceEvent = createTraceEvent(
                        fmt::format("Task {} (Pipeline {}, Query {})", taskStart.taskId, taskStart.pipelineId, taskStart.queryId),
                        Category::Task,
                        Phase::Begin,
                        timestampToMicroseconds(taskStart.timestamp),
                        0,
                        args);
                    traceEvent["tid"] = taskStart.threadId.getRawValue();

                    emit(traceEvent);

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

                    auto args = nlohmann::json::object();
                    args["pipeline_id"] = taskComplete.pipelineId.getRawValue();
                    args["task_id"] = taskComplete.taskId.getRawValue();

                    auto traceEvent = createTraceEvent(
                        fmt::format("Task {} (Pipeline {}, Query {})", taskComplete.taskId, taskComplete.pipelineId, taskComplete.queryId),
                        Category::Task,
                        Phase::End,
                        timestampToMicroseconds(taskComplete.timestamp),
                        duration,
                        args);
                    traceEvent["tid"] = taskComplete.threadId.getRawValue();

                    emit(traceEvent);
                },
                [&](const TaskEmit& taskEmit)
                {
                    auto args = nlohmann::json::object();
                    args["from_pipeline"] = taskEmit.fromPipeline.getRawValue();
                    args["to_pipeline"] = taskEmit.toPipeline.getRawValue();
                    args["task_id"] = taskEmit.taskId.getRawValue();
                    args["tuples"] = taskEmit.numberOfTuples;

                    auto traceEvent = createTraceEvent(
                        fmt::format(
                            "Emit {}->{} (Task {}, Query {})",
                            taskEmit.fromPipeline,
                            taskEmit.toPipeline,
                            taskEmit.taskId,
                            taskEmit.queryId),
                        Category::Task,
                        Phase::Instant,
                        timestampToMicroseconds(taskEmit.timestamp),
                        0,
                        args);
                    traceEvent["tid"] = taskEmit.threadId.getRawValue();

                    emit(traceEvent);
                },
                [&](const TaskExpired& taskExpired)
                {
                    auto args = nlohmann::json::object();
                    args["pipeline_id"] = taskExpired.pipelineId.getRawValue();
                    args["task_id"] = taskExpired.taskId.getRawValue();

                    auto traceEvent = createTraceEvent(
                        fmt::format(
                            "Task Expired {} (Pipeline {}, Query {})", taskExpired.taskId, taskExpired.pipelineId, taskExpired.queryId),
                        Category::Task,
                        Phase::Instant,
                        timestampToMicroseconds(taskExpired.timestamp),
                        0,
                        args);
                    traceEvent["tid"] = taskExpired.threadId.getRawValue();

                    emit(traceEvent);

                    /// Remove from active tasks if present
                    activeTasks.erase(taskExpired.taskId);
                }},
            event);
    }

    /// Write the footer when the thread stops
    writeTraceFooter();
}

void GoogleEventTracePrinter::onEvent(Event event)
{
    events.writeIfNotFull(std::visit([]<typename T>(T&& arg) { return CombinedEventType(std::forward<T>(arg)); }, std::move(event)));
}

void GoogleEventTracePrinter::onEvent(SystemEvent event)
{
    events.writeIfNotFull(std::visit([]<typename T>(T&& arg) { return CombinedEventType(std::forward<T>(arg)); }, std::move(event)));
}

GoogleEventTracePrinter::GoogleEventTracePrinter(const std::filesystem::path& path) : file(path, std::ios::out | std::ios::trunc)
{
    NES_INFO("Writing Google Event Trace to: {}", path);
}

void GoogleEventTracePrinter::start()
{
    traceThread = Thread("event-trace", [this](const std::stop_token& stopToken) { threadRoutine(stopToken); });
}

GoogleEventTracePrinter::~GoogleEventTracePrinter()
{
    if (file.is_open())
    {
        file.flush();
        file.close();
    }
}

void GoogleEventTracePrinter::flush()
{
    if (file.is_open())
    {
        file.flush();
        file.close();
    }
}

}
