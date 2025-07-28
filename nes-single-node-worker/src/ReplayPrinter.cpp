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

#include <ReplayPrinter.hpp>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <stop_token>
#include <variant>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/SystemEventListener.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <Util/ThreadNaming.hpp>
#include <fmt/format.h>
#include <folly/MPMCQueue.h>
#include <nlohmann/json.hpp>
#include <QueryEngineStatisticListener.hpp>

namespace NES
{

uint64_t timestampToMicroseconds(const std::chrono::system_clock::time_point& timestamp)
{
    return std::chrono::duration_cast<std::chrono::microseconds>(timestamp.time_since_epoch()).count();
}

nlohmann::json ReplayPrinter::serializeEvent(const CombinedEventType& event)
{
    nlohmann::json eventData;
    eventData["timestamp"] = timestampToMicroseconds(std::chrono::system_clock::now());

    std::visit(
        Overloaded{
            [&](const SubmitQuerySystemEvent& submitEvent)
            {
                eventData["type"] = "SubmitQuerySystemEvent";
                eventData["queryId"] = submitEvent.queryId.getRawValue();
                eventData["query"] = submitEvent.query;
                eventData["eventTimestamp"] = timestampToMicroseconds(submitEvent.timestamp);
            },
            [&](const StartQuerySystemEvent& startEvent)
            {
                eventData["type"] = "StartQuerySystemEvent";
                eventData["queryId"] = startEvent.queryId.getRawValue();
                eventData["eventTimestamp"] = timestampToMicroseconds(startEvent.timestamp);
            },
            [&](const StopQuerySystemEvent& stopEvent)
            {
                eventData["type"] = "StopQuerySystemEvent";
                eventData["queryId"] = stopEvent.queryId.getRawValue();
                eventData["eventTimestamp"] = timestampToMicroseconds(stopEvent.timestamp);
            },
            [&](const QueryStart& queryStart)
            {
                eventData["type"] = "QueryStart";
                eventData["queryId"] = queryStart.queryId.getRawValue();
                eventData["threadId"] = queryStart.threadId.getRawValue();
                eventData["eventTimestamp"] = timestampToMicroseconds(queryStart.timestamp);
            },
            [&](const QueryStop& queryStop)
            {
                eventData["type"] = "QueryStop";
                eventData["queryId"] = queryStop.queryId.getRawValue();
                eventData["threadId"] = queryStop.threadId.getRawValue();
                eventData["eventTimestamp"] = timestampToMicroseconds(queryStop.timestamp);
            },
            [&](const PipelineStart& pipelineStart)
            {
                eventData["type"] = "PipelineStart";
                eventData["pipelineId"] = pipelineStart.pipelineId.getRawValue();
                eventData["queryId"] = pipelineStart.queryId.getRawValue();
                eventData["threadId"] = pipelineStart.threadId.getRawValue();
                eventData["eventTimestamp"] = timestampToMicroseconds(pipelineStart.timestamp);
            },
            [&](const PipelineStop& pipelineStop)
            {
                eventData["type"] = "PipelineStop";
                eventData["pipelineId"] = pipelineStop.pipelineId.getRawValue();
                eventData["queryId"] = pipelineStop.queryId.getRawValue();
                eventData["threadId"] = pipelineStop.threadId.getRawValue();
                eventData["eventTimestamp"] = timestampToMicroseconds(pipelineStop.timestamp);
            },
            [&](const TaskExecutionStart& taskStart)
            {
                eventData["type"] = "TaskExecutionStart";
                eventData["taskId"] = taskStart.taskId.getRawValue();
                eventData["pipelineId"] = taskStart.pipelineId.getRawValue();
                eventData["queryId"] = taskStart.queryId.getRawValue();
                eventData["threadId"] = taskStart.threadId.getRawValue();
                eventData["numberOfTuples"] = taskStart.numberOfTuples;
                eventData["eventTimestamp"] = timestampToMicroseconds(taskStart.timestamp);
                
                /// Add metadata about the tuple processing
                auto metadata = nlohmann::json::object();
                metadata["tupleCount"] = taskStart.numberOfTuples;
                metadata["processingType"] = "batch";
                
                /// Add tuple data (always captured)
                const auto& tupleData = taskStart.tupleData;
                auto tupleDataJson = nlohmann::json::object();
                tupleDataJson["numberOfTuples"] = tupleData.numberOfTuples;
                tupleDataJson["bufferSize"] = tupleData.bufferSize;
                tupleDataJson["rawDataSize"] = tupleData.rawData.size();
                
                /// Convert raw data to base64 for JSON serialization
                std::string base64Data;
                base64Data.reserve((tupleData.rawData.size() + 2) / 3 * 4);
                
                for (size_t i = 0; i < tupleData.rawData.size(); i += 3)
                {
                    uint32_t chunk = 0;
                    int bytes = 0;
                    
                    for (int j = 0; j < 3 && i + j < tupleData.rawData.size(); ++j)
                    {
                        chunk = (chunk << 8) | tupleData.rawData[i + j];
                        bytes++;
                    }
                    
                    const char* base64Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
                    for (int j = 0; j < 4; ++j)
                    {
                        if (j * 6 < bytes * 8)
                        {
                            base64Data += base64Chars[(chunk >> (18 - j * 6)) & 0x3F];
                        }
                        else
                        {
                            base64Data += '=';
                        }
                    }
                }
                
                tupleDataJson["rawData"] = base64Data;
                metadata["tupleData"] = tupleDataJson;
                metadata["note"] = "Tuple data captured for full replay capability";
                
                eventData["metadata"] = metadata;
            },
            [&](const TaskExecutionComplete& taskComplete)
            {
                eventData["type"] = "TaskExecutionComplete";
                eventData["taskId"] = taskComplete.taskId.getRawValue();
                eventData["pipelineId"] = taskComplete.pipelineId.getRawValue();
                eventData["queryId"] = taskComplete.queryId.getRawValue();
                eventData["threadId"] = taskComplete.threadId.getRawValue();
                eventData["eventTimestamp"] = timestampToMicroseconds(taskComplete.timestamp);
            },
            [&](const TaskEmit& taskEmit)
            {
                eventData["type"] = "TaskEmit";
                eventData["taskId"] = taskEmit.taskId.getRawValue();
                eventData["fromPipeline"] = taskEmit.fromPipeline.getRawValue();
                eventData["toPipeline"] = taskEmit.toPipeline.getRawValue();
                eventData["queryId"] = taskEmit.queryId.getRawValue();
                eventData["threadId"] = taskEmit.threadId.getRawValue();
                eventData["numberOfTuples"] = taskEmit.numberOfTuples;
                eventData["eventTimestamp"] = timestampToMicroseconds(taskEmit.timestamp);
                
                /// Add metadata about the data flow
                auto metadata = nlohmann::json::object();
                metadata["tupleCount"] = taskEmit.numberOfTuples;
                metadata["flowType"] = "pipeline_transfer";
                metadata["sourcePipeline"] = taskEmit.fromPipeline.getRawValue();
                metadata["targetPipeline"] = taskEmit.toPipeline.getRawValue();
                
                /// Add tuple data (always captured)
                const auto& tupleData = taskEmit.tupleData;
                auto tupleDataJson = nlohmann::json::object();
                tupleDataJson["numberOfTuples"] = tupleData.numberOfTuples;
                tupleDataJson["bufferSize"] = tupleData.bufferSize;
                tupleDataJson["rawDataSize"] = tupleData.rawData.size();
                
                /// Convert raw data to base64 for JSON serialization
                std::string base64Data;
                base64Data.reserve((tupleData.rawData.size() + 2) / 3 * 4);
                
                for (size_t i = 0; i < tupleData.rawData.size(); i += 3)
                {
                    uint32_t chunk = 0;
                    int bytes = 0;
                    
                    for (int j = 0; j < 3 && i + j < tupleData.rawData.size(); ++j)
                    {
                        chunk = (chunk << 8) | tupleData.rawData[i + j];
                        bytes++;
                    }
                    
                    const char* base64Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
                    for (int j = 0; j < 4; ++j)
                    {
                        if (j * 6 < bytes * 8)
                        {
                            base64Data += base64Chars[(chunk >> (18 - j * 6)) & 0x3F];
                        }
                        else
                        {
                            base64Data += '=';
                        }
                    }
                }
                
                tupleDataJson["rawData"] = base64Data;
                metadata["tupleData"] = tupleDataJson;
                metadata["note"] = "Tuple data captured for full replay capability";
                
                eventData["metadata"] = metadata;
            },
            [&](const TaskExpired& taskExpired)
            {
                eventData["type"] = "TaskExpired";
                eventData["taskId"] = taskExpired.taskId.getRawValue();
                eventData["pipelineId"] = taskExpired.pipelineId.getRawValue();
                eventData["queryId"] = taskExpired.queryId.getRawValue();
                eventData["threadId"] = taskExpired.threadId.getRawValue();
                eventData["eventTimestamp"] = timestampToMicroseconds(taskExpired.timestamp);
            }},
        event);

    return eventData;
}

void ReplayPrinter::onEvent(Event event)
{
    events.writeIfNotFull(std::visit([]<typename T>(T&& arg) { return CombinedEventType(std::forward<T>(arg)); }, std::move(event)));
}

void ReplayPrinter::onEvent(SystemEvent event)
{
    events.writeIfNotFull(std::visit([]<typename T>(T&& arg) { return CombinedEventType(std::forward<T>(arg)); }, std::move(event)));
}

ReplayPrinter::ReplayPrinter(const std::filesystem::path& baseDir)
    : baseDirectory(baseDir), replayThread([this](const std::stop_token& stopToken) { threadRoutine(stopToken); })
{
    NES_INFO("Writing Replay Data to directory: {}", baseDir);
    NES_INFO("Tuple data capture is ENABLED - this will significantly increase memory usage and file size");
    NES_INFO("Compiled libraries will be stored in: {}/libraries", baseDir);
    
    /// Create the base directory if it doesn't exist
    if (!std::filesystem::exists(baseDirectory))
    {
        std::filesystem::create_directories(baseDirectory);
    }
}

ReplayPrinter::~ReplayPrinter()
{
    if (replayThread.joinable())
    {
        replayThread.request_stop();
        replayThread.join();
    }

    /// Close all open files
    for (auto& [queryId, file] : queryFiles)
    {
        if (file.is_open())
        {
            closeQueryFile(queryId);
        }
    }
}

void ReplayPrinter::flush()
{
    if (replayThread.joinable())
    {
        replayThread.request_stop();
        replayThread.join();
    }

    /// Close all open files
    for (auto& [queryId, file] : queryFiles)
    {
        if (file.is_open())
        {
            closeQueryFile(queryId);
        }
    }
}

void ReplayPrinter::storeCompiledLibrary(QueryId queryId, const std::filesystem::path& libraryPath)
{
    /// Create a libraries subfolder in the replay directory
    auto librariesDir = baseDirectory / "libraries";
    if (!std::filesystem::exists(librariesDir))
    {
        std::filesystem::create_directories(librariesDir);
    }
    
    /// Copy the library to the replay directory's libraries subfolder
    if (std::filesystem::exists(libraryPath))
    {
        auto filename = libraryPath.filename();
        auto replayLibraryPath = librariesDir / filename;
        
        try
        {
            std::filesystem::copy_file(libraryPath, replayLibraryPath, std::filesystem::copy_options::overwrite_existing);
            compiledLibraries[queryId].push_back(replayLibraryPath);
            NES_INFO("Copied compiled library for Query {}: {} -> {}", queryId.getRawValue(), libraryPath, replayLibraryPath);
        }
        catch (const std::filesystem::filesystem_error& e)
        {
            NES_WARNING("Failed to copy compiled library {} for Query {}: {}", libraryPath, queryId.getRawValue(), e.what());
            /// Still store the original path for reference
            compiledLibraries[queryId].push_back(libraryPath);
        }
    }
    else
    {
        NES_WARNING("Compiled library not found: {}", libraryPath);
        /// Store the path anyway for reference
        compiledLibraries[queryId].push_back(libraryPath);
    }
}

std::ofstream& ReplayPrinter::getOrCreateQueryFile(QueryId queryId)
{
    auto it = queryFiles.find(queryId);
    if (it == queryFiles.end())
    {
        /// Create a new file for this query
        auto filename = fmt::format("ReplayData_Query_{}_{:%Y-%m-%d_%H-%M-%S}_{}.json", 
                                   queryId.getRawValue(), 
                                   std::chrono::system_clock::now(), 
                                   ::getpid());
        auto filepath = baseDirectory / filename;
        
        auto& file = queryFiles[queryId];
        file.open(filepath, std::ios::out | std::ios::trunc);
        
        /// Initialize tracking for this query
        queryHeadersWritten[queryId] = false;
        queryFootersWritten[queryId] = false;
        
        NES_INFO("Created replay file for Query {}: {}", queryId.getRawValue(), filepath);
    }
    
    return queryFiles[queryId];
}

void ReplayPrinter::closeQueryFile(QueryId queryId)
{
    auto fileIt = queryFiles.find(queryId);
    if (fileIt != queryFiles.end())
    {
        auto& file = fileIt->second;
        if (file.is_open())
        {
            writeReplayFooter(file);
            file.flush();
            file.close();
        }
        queryFiles.erase(fileIt);
        queryHeadersWritten.erase(queryId);
        queryFootersWritten.erase(queryId);
        
        NES_INFO("Closed replay file for Query {}", queryId.getRawValue());
    }
}

void ReplayPrinter::writeReplayHeader(std::ofstream& file)
{
    file << "{\n";
    file << "  \"replayData\": {\n";
    file << "    \"version\": \"1.0\",\n";
    file << "    \"description\": \"NES Query Engine Replay Data\",\n";
    file << "    \"createdAt\": " << timestampToMicroseconds(std::chrono::system_clock::now()) << ",\n";
    file << "    \"events\": [\n";
}

void ReplayPrinter::writeReplayFooter(std::ofstream& file)
{
    file << "\n    ]\n";
    file << "  }\n";
    file << "}\n";
}

void ReplayPrinter::threadRoutine(const std::stop_token& token)
{
    setThreadName("ReplayPrinter");

    /// Track first event for each query file
    std::unordered_map<QueryId, bool> firstEventForQuery;

    while (!token.stop_requested())
    {
        CombinedEventType event = QueryStart{WorkerThreadId(0), QueryId(0)}; /// Will be overwritten

        if (!events.tryReadUntil(std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(100), event))
        {
            continue;
        }

        /// Determine which query this event belongs to
        QueryId targetQueryId = QueryId(0);
        std::visit(
            Overloaded{
                [&](const SubmitQuerySystemEvent& submitEvent) { targetQueryId = submitEvent.queryId; },
                [&](const StartQuerySystemEvent& startEvent) { targetQueryId = startEvent.queryId; },
                [&](const StopQuerySystemEvent& stopEvent) { targetQueryId = stopEvent.queryId; },
                [&](const QueryStart& queryStart) { targetQueryId = queryStart.queryId; },
                [&](const QueryStop& queryStop) { targetQueryId = queryStop.queryId; },
                [&](const PipelineStart& pipelineStart) { targetQueryId = pipelineStart.queryId; },
                [&](const PipelineStop& pipelineStop) { targetQueryId = pipelineStop.queryId; },
                [&](const TaskExecutionStart& taskStart) { targetQueryId = taskStart.queryId; },
                [&](const TaskExecutionComplete& taskComplete) { targetQueryId = taskComplete.queryId; },
                [&](const TaskEmit& taskEmit) { targetQueryId = taskEmit.queryId; },
                [&](const TaskExpired& taskExpired) { targetQueryId = taskExpired.queryId; },
                [&](const auto&) { 
                    /// For events without query ID, use a default or skip
                    NES_WARNING("Event without query ID encountered, skipping");
                    return;
                }},
            event);

        /// Get or create the file for this query
        auto& file = getOrCreateQueryFile(targetQueryId);
        
        /// Write header if this is the first event for this query
        if (!queryHeadersWritten[targetQueryId])
        {
            writeReplayHeader(file);
            queryHeadersWritten[targetQueryId] = true;
            firstEventForQuery[targetQueryId] = true;
        }

        /// Serialize the event with all its data
        auto eventData = serializeEvent(event);
        
        /// Add compiled library information for QueryStart events
        std::visit(Overloaded{
            [&](const QueryStart& queryStart) {
                auto libIt = compiledLibraries.find(queryStart.queryId);
                if (libIt != compiledLibraries.end() && !libIt->second.empty()) {
                    auto metadata = nlohmann::json::object();
                    metadata["compiledLibraries"] = nlohmann::json::array();
                    for (const auto& libPath : libIt->second) {
                        auto libInfo = nlohmann::json::object();
                        libInfo["path"] = libPath.string();
                        libInfo["filename"] = libPath.filename().string();
                        
                        /// Add file size if the file exists
                        if (std::filesystem::exists(libPath)) {
                            libInfo["size"] = std::filesystem::file_size(libPath);
                            libInfo["lastModified"] = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::filesystem::last_write_time(libPath).time_since_epoch()).count();
                        }
                        
                        metadata["compiledLibraries"].push_back(libInfo);
                    }
                    eventData["metadata"] = metadata;
                }
            },
            [&](const auto&) {
                /// No special handling for other events
            }
        }, event);
        
        /// Write the event with proper comma handling
        if (!firstEventForQuery[targetQueryId])
        {
            file << ",\n";
        }
        firstEventForQuery[targetQueryId] = false;
        file << "      " << eventData.dump(2);

        /// Track active entities for relationship mapping
        std::visit(
            Overloaded{
                [&](const QueryStart& queryStart)
                {
                    activeQueries[queryStart.queryId] = std::vector<PipelineId>{};
                },
                [&](const QueryStop& queryStop)
                {
                    activeQueries.erase(queryStop.queryId);
                    /// Close the file when the query stops
                    closeQueryFile(queryStop.queryId);
                },
                [&](const PipelineStart& pipelineStart)
                {
                    activePipelines.emplace(pipelineStart.pipelineId, std::make_pair(pipelineStart.queryId, pipelineStart.threadId));
                    /// Add pipeline to the query's pipeline list
                    auto it = activeQueries.find(pipelineStart.queryId);
                    if (it != activeQueries.end()) {
                        it->second.push_back(pipelineStart.pipelineId);
                    }
                },
                [&](const PipelineStop& pipelineStop)
                {
                    activePipelines.erase(pipelineStop.pipelineId);
                },
                [&](const auto&) {
                    /// No tracking needed for other event types
                }},
            event);
    }

    /// Write footers for all open files when the thread stops
    for (auto& [queryId, file] : queryFiles)
    {
        if (file.is_open() && !queryFootersWritten[queryId])
        {
            writeReplayFooter(file);
            queryFootersWritten[queryId] = true;
        }
    }
}

} 