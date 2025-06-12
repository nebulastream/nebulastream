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
#include <mutex>
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
struct GoogleEventTraceListener final : QueryEngineStatisticListener, SystemEventListener
{
    using CombinedEventType = FlattenVariant<SystemEvent, Event>::type;
    void onEvent(Event event) override;
    void onEvent(SystemEvent event) override;

    explicit GoogleEventTraceListener(const std::filesystem::path& path);
    ~GoogleEventTraceListener();

    /// Flushes the trace file and closes it
    void flush();

private:
    static uint64_t timestampToMicroseconds(const std::chrono::system_clock::time_point& timestamp);
    
    nlohmann::json createTraceEvent(const std::string& name,
                                   const std::string& cat, 
                                   const std::string& ph, 
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
    std::mutex fileMutex;
    bool headerWritten = false;
    bool footerWritten = false;
    
    /// Track active tasks for duration calculation
    std::unordered_map<TaskId, std::chrono::system_clock::time_point> activeTasks;
    std::mutex activeTasksMutex;
};
} 