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

#include <filesystem>
#include <fstream>
#include <thread>
#include <unordered_map>
#include <variant>
#include <Listeners/SystemEventListener.hpp>
#include <folly/MPMCQueue.h>
#include <nlohmann/json.hpp>
#include <QueryEngineStatisticListener.hpp>
#include <StatisticPrinter.hpp>

namespace NES
{
/// This printer generates Chrome DevTools trace files that can be opened in Chrome's
/// chrome://tracing/ interface for performance analysis (or any other event trace visualizer)
struct GoogleEventTracePrinter final : QueryEngineStatisticListener, SystemEventListener
{
    using CombinedEventType = FlattenVariant<SystemEvent, Event>::type;
    void onEvent(Event event) override;
    void onEvent(SystemEvent event) override;

    /// Constructs a GoogleEventTracePrinter that writes to the specified file path
    /// @param path The file path where the trace will be written
    explicit GoogleEventTracePrinter(const std::filesystem::path& path);
    ~GoogleEventTracePrinter();

    /// Flushes the trace file and closes it, blocking until all pending events are written
    void flush();

private:
    enum class Category
    {
        Query,
        Pipeline,
        Task,
        System
    };

    enum class Phase
    {
        Begin,
        End,
        Instant
    };

    static uint64_t timestampToMicroseconds(const std::chrono::system_clock::time_point& timestamp);

    nlohmann::json createTraceEvent(
        const std::string& name,
        Category cat,
        Phase ph,
        uint64_t ts,
        uint64_t dur = 0,
        const nlohmann::json& args = {});

    /// Thread routine that processes events and writes to the trace file
    void threadRoutine(const std::stop_token& token);
    void writeTraceHeader();
    void writeTraceFooter();

    std::ofstream file;
    folly::MPMCQueue<CombinedEventType> events{1000};
    std::jthread traceThread;
    bool headerWritten = false;
    bool footerWritten = false;

    /// Track active tasks for duration calculation
    std::unordered_map<TaskId, std::chrono::system_clock::time_point> activeTasks;

    /// Track active queries and pipelines for cleanup
    std::unordered_map<QueryId, std::pair<std::chrono::system_clock::time_point, WorkerThreadId>> activeQueries;
    std::unordered_map<PipelineId, std::tuple<QueryId, std::chrono::system_clock::time_point, WorkerThreadId>> activePipelines;
};
} 