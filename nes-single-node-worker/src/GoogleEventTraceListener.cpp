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

#include <GoogleEventTraceListener.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <ios>
#include <stop_token>
#include <utility>
#include <variant>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/SystemEventListener.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <Util/ThreadNaming.hpp>
#include <fmt/format.h>
#include <folly/MPMCQueue.h>
#include <QueryEngineStatisticListener.hpp>
#include <nlohmann/json.hpp>

namespace NES
{

namespace
{
/// Chrome tracing event types
constexpr std::string_view EVENT_BEGIN = "B";
constexpr std::string_view EVENT_END = "E";
constexpr std::string_view EVENT_INSTANT = "i";
constexpr std::string_view EVENT_COUNTER = "C";

/// Chrome tracing categories
constexpr std::string_view CATEGORY_QUERY = "query";
constexpr std::string_view CATEGORY_PIPELINE = "pipeline";
constexpr std::string_view CATEGORY_TASK = "task";
constexpr std::string_view CATEGORY_SYSTEM = "system";
}

uint64_t GoogleEventTraceListener::timestampToMicroseconds(const std::chrono::system_clock::time_point& timestamp)
{
    return std::chrono::duration_cast<std::chrono::microseconds>(timestamp.time_since_epoch()).count();
}

nlohmann::json GoogleEventTraceListener::createTraceEvent(const std::string& name, 
                                                         const std::string& cat, 
                                                         const std::string& ph, 
                                                         uint64_t ts, 
                                                         uint64_t dur,
                                                         const nlohmann::json& args)
{
    nlohmann::json event;
    event["name"] = name;
    event["cat"] = cat;
    event["ph"] = ph;
    event["ts"] = ts;
    event["pid"] = 1; // Process ID
    event["tid"] = 1; // Thread ID (will be overridden for specific events)
    
    if (dur > 0) {
        event["dur"] = dur;
    }
    
    if (!args.empty()) {
        event["args"] = args;
    }
    
    return event;
}

void GoogleEventTraceListener::writeTraceHeader()
{
    std::scoped_lock lock(fileMutex);
    if (!headerWritten) {
        file << "{\n";
        file << "  \"traceEvents\": [\n";
        headerWritten = true;
    }
}

void GoogleEventTraceListener::writeTraceFooter()
{
    std::scoped_lock lock(fileMutex);
    if (!footerWritten) {
        file << "\n  ]\n";
        file << "}\n";
        footerWritten = true;
    }
}

void GoogleEventTraceListener::threadRoutine(const std::stop_token& token)
{
    setThreadName("GoogleTrace");
    writeTraceHeader();
    
    bool firstEvent = true;
    
    while (!token.stop_requested())
    {
        CombinedEventType event = QueryStart{WorkerThreadId(0), QueryId(0)}; // Will be overwritten

        if (!events.tryReadUntil(std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(100), event))
        {
            continue;
        }
        
        std::scoped_lock lock(fileMutex);
        
        // Add comma separator for all events except the first
        if (!firstEvent) {
            file << ",\n";
        } else {
            firstEvent = false;
        }
        
        std::visit(
            Overloaded{
                [&](const SubmitQuerySystemEvent& submitEvent)
                {
                    auto args = nlohmann::json::object();
                    args["query"] = submitEvent.query;
                    
                    auto traceEvent = createTraceEvent(
                        fmt::format("Submit Query {}", submitEvent.queryId),
                        std::string(CATEGORY_SYSTEM),
                        std::string(EVENT_INSTANT),
                        timestampToMicroseconds(submitEvent.timestamp),
                        0,
                        args
                    );
                    traceEvent["tid"] = 0; // System thread
                    
                    file << "    " << traceEvent.dump();
                },
                [&](const StopQuerySystemEvent& stopEvent)
                {
                    auto traceEvent = createTraceEvent(
                        fmt::format("Stop Query {}", stopEvent.queryId),
                        std::string(CATEGORY_QUERY),
                        std::string(EVENT_END),
                        timestampToMicroseconds(stopEvent.timestamp)
                    );
                    traceEvent["tid"] = 0; // System thread
                    
                    file << "    " << traceEvent.dump();
                },
                [&](const QueryStart& queryStart)
                {
                    auto traceEvent = createTraceEvent(
                        fmt::format("Query {}", queryStart.queryId),
                        std::string(CATEGORY_QUERY),
                        std::string(EVENT_BEGIN),
                        timestampToMicroseconds(queryStart.timestamp)
                    );
                    traceEvent["tid"] = queryStart.threadId.getRawValue();
                    
                    file << "    " << traceEvent.dump();
                },
                [&](const QueryStop& queryStop)
                {
                    auto traceEvent = createTraceEvent(
                        fmt::format("Query {}", queryStop.queryId),
                        std::string(CATEGORY_QUERY),
                        std::string(EVENT_END),
                        timestampToMicroseconds(queryStop.timestamp)
                    );
                    traceEvent["tid"] = queryStop.threadId.getRawValue();
                    
                    file << "    " << traceEvent.dump();
                },
                [&](const PipelineStart& pipelineStart)
                {
                    auto args = nlohmann::json::object();
                    args["pipeline_id"] = pipelineStart.pipelineId.getRawValue();
                    
                    auto traceEvent = createTraceEvent(
                        fmt::format("Pipeline {} (Query {})", pipelineStart.pipelineId, pipelineStart.queryId),
                        std::string(CATEGORY_PIPELINE),
                        std::string(EVENT_BEGIN),
                        timestampToMicroseconds(pipelineStart.timestamp),
                        0,
                        args
                    );
                    traceEvent["tid"] = pipelineStart.threadId.getRawValue();
                    
                    file << "    " << traceEvent.dump();
                },
                [&](const PipelineStop& pipelineStop)
                {
                    auto args = nlohmann::json::object();
                    args["pipeline_id"] = pipelineStop.pipelineId.getRawValue();
                    
                    auto traceEvent = createTraceEvent(
                        fmt::format("Pipeline {} (Query {})", pipelineStop.pipelineId, pipelineStop.queryId),
                        std::string(CATEGORY_PIPELINE),
                        std::string(EVENT_END),
                        timestampToMicroseconds(pipelineStop.timestamp),
                        0,
                        args
                    );
                    traceEvent["tid"] = pipelineStop.threadId.getRawValue();
                    
                    file << "    " << traceEvent.dump();
                },
                [&](const TaskExecutionStart& taskStart)
                {
                    auto args = nlohmann::json::object();
                    args["pipeline_id"] = taskStart.pipelineId.getRawValue();
                    args["task_id"] = taskStart.taskId.getRawValue();
                    args["tuples"] = taskStart.numberOfTuples;
                    
                    auto traceEvent = createTraceEvent(
                        fmt::format("Task {} (Pipeline {}, Query {})", taskStart.taskId, taskStart.pipelineId, taskStart.queryId),
                        std::string(CATEGORY_TASK),
                        std::string(EVENT_BEGIN),
                        timestampToMicroseconds(taskStart.timestamp),
                        0,
                        args
                    );
                    traceEvent["tid"] = taskStart.threadId.getRawValue();
                    
                    file << "    " << traceEvent.dump();
                    
                    // Track this task for duration calculation
                    std::scoped_lock taskLock(activeTasksMutex);
                    activeTasks[taskStart.taskId] = taskStart.timestamp;
                },
                [&](const TaskExecutionComplete& taskComplete)
                {
                    uint64_t duration = 0;
                    {
                        std::scoped_lock taskLock(activeTasksMutex);
                        auto it = activeTasks.find(taskComplete.taskId);
                        if (it != activeTasks.end()) {
                            duration = timestampToMicroseconds(taskComplete.timestamp) - timestampToMicroseconds(it->second);
                            activeTasks.erase(it);
                        }
                    }
                    
                    auto args = nlohmann::json::object();
                    args["pipeline_id"] = taskComplete.pipelineId.getRawValue();
                    args["task_id"] = taskComplete.taskId.getRawValue();
                    
                    auto traceEvent = createTraceEvent(
                        fmt::format("Task {} (Pipeline {}, Query {})", taskComplete.taskId, taskComplete.pipelineId, taskComplete.queryId),
                        std::string(CATEGORY_TASK),
                        std::string(EVENT_END),
                        timestampToMicroseconds(taskComplete.timestamp),
                        duration,
                        args
                    );
                    traceEvent["tid"] = taskComplete.threadId.getRawValue();
                    
                    file << "    " << traceEvent.dump();
                },
                [&](const TaskEmit& taskEmit)
                {
                    auto args = nlohmann::json::object();
                    args["from_pipeline"] = taskEmit.fromPipeline.getRawValue();
                    args["to_pipeline"] = taskEmit.toPipeline.getRawValue();
                    args["task_id"] = taskEmit.taskId.getRawValue();
                    args["tuples"] = taskEmit.numberOfTuples;
                    
                    auto traceEvent = createTraceEvent(
                        fmt::format("Emit {}->{} (Task {}, Query {})", taskEmit.fromPipeline, taskEmit.toPipeline, taskEmit.taskId, taskEmit.queryId),
                        std::string(CATEGORY_TASK),
                        std::string(EVENT_INSTANT),
                        timestampToMicroseconds(taskEmit.timestamp),
                        0,
                        args
                    );
                    traceEvent["tid"] = taskEmit.threadId.getRawValue();
                    
                    file << "    " << traceEvent.dump();
                },
                [&](const TaskExpired& taskExpired)
                {
                    auto args = nlohmann::json::object();
                    args["pipeline_id"] = taskExpired.pipelineId.getRawValue();
                    args["task_id"] = taskExpired.taskId.getRawValue();
                    
                    auto traceEvent = createTraceEvent(
                        fmt::format("Task Expired {} (Pipeline {}, Query {})", taskExpired.taskId, taskExpired.pipelineId, taskExpired.queryId),
                        std::string(CATEGORY_TASK),
                        std::string(EVENT_INSTANT),
                        timestampToMicroseconds(taskExpired.timestamp),
                        0,
                        args
                    );
                    traceEvent["tid"] = taskExpired.threadId.getRawValue();
                    
                    file << "    " << traceEvent.dump();
                    
                    // Remove from active tasks if present
                    std::scoped_lock taskLock(activeTasksMutex);
                    activeTasks.erase(taskExpired.taskId);
                }
            },
            event);
    }
    
    writeTraceFooter();
}

void GoogleEventTraceListener::onEvent(Event event)
{
    events.writeIfNotFull(std::visit([]<typename T>(T&& arg) { return CombinedEventType(std::forward<T>(arg)); }, std::move(event)));
}

void GoogleEventTraceListener::onEvent(SystemEvent event)
{
    events.writeIfNotFull(std::visit([]<typename T>(T&& arg) { return CombinedEventType(std::forward<T>(arg)); }, std::move(event)));
}

GoogleEventTraceListener::GoogleEventTraceListener(const std::filesystem::path& path)
    : file(path, std::ios::out | std::ios::trunc)
    , traceThread([this](const std::stop_token& stopToken) { threadRoutine(stopToken); })
{
    NES_INFO("Writing Google Event Trace to: {}", path);
}

GoogleEventTraceListener::~GoogleEventTraceListener()
{
    flush();
}

void GoogleEventTraceListener::flush()
{
    if (traceThread.joinable()) {
        traceThread.request_stop();
        traceThread.join();
    }
    
    if (file.is_open()) {
        file.flush();
        file.close();
    }
}

} 