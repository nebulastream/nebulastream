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

/// This printer stores all events and their associated input data in a format that can be used
/// to replay the exact sequence of actions that occurred during execution
/// Note: For full replay capability including actual tuple data, the event system would need
/// to be modified to capture TupleBuffer data at the source
struct ReplayPrinter final : QueryEngineStatisticListener, SystemEventListener
{
    using CombinedEventType = FlattenVariant<SystemEvent, Event>::type;
    void onEvent(Event event) override;
    void onEvent(SystemEvent event) override;

    /// Constructs a ReplayPrinter that writes to files in the specified directory
    /// @param baseDir The directory where replay files will be created (one per query)
    explicit ReplayPrinter(const std::filesystem::path& baseDir);
    ~ReplayPrinter();

    /// Flushes all replay files and closes them, blocking until all pending events are written
    void flush();

    /// Store the path to a compiled query shared library for replay
    /// @param queryId The query ID that the library belongs to
    /// @param libraryPath The path to the compiled shared library
    void storeCompiledLibrary(QueryId queryId, const std::filesystem::path& libraryPath);

private:
    /// Thread routine that processes events and writes to replay files
    void threadRoutine(const std::stop_token& token);
    void writeReplayHeader(std::ofstream& file);
    void writeReplayFooter(std::ofstream& file);

    /// Helper function to serialize an event with all its data
    nlohmann::json serializeEvent(const CombinedEventType& event);

    /// Helper function to get or create a file for a specific query
    std::ofstream& getOrCreateQueryFile(QueryId queryId);

    /// Helper function to close a query file
    void closeQueryFile(QueryId queryId);

    std::filesystem::path baseDirectory;
    folly::MPMCQueue<CombinedEventType> events{1000};
    std::jthread replayThread;

    /// Track active files for each query
    std::unordered_map<QueryId, std::ofstream> queryFiles;
    std::unordered_map<QueryId, bool> queryHeadersWritten;
    std::unordered_map<QueryId, bool> queryFootersWritten;

    /// Track active queries and pipelines for relationship mapping
    std::unordered_map<QueryId, std::vector<PipelineId>> activeQueries;
    std::unordered_map<PipelineId, std::pair<QueryId, WorkerThreadId>> activePipelines;
    
    /// Track compiled libraries for each query
    std::unordered_map<QueryId, std::vector<std::filesystem::path>> compiledLibraries;
};

} 