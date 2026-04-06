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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <Identifiers/Identifiers.hpp>

namespace NES::Systest
{

/// A status change event received from the query_status unix socket.
struct QueryStatusEvent
{
    uint64_t timestampMs;
    std::string queryId; /// LocalQueryId UUID string (as emitted by ToUuid)
    std::string status; /// "started", "running", "stopped", "failed"
};

/// Aggregated compilation time for a query (sum of all pipeline compile times).
struct CompilationStats
{
    uint64_t totalCompileTimeNs = 0;
    uint64_t pipelineCount = 0;
};

/// Aggregated buffer ingestion stats for a query.
struct IngestionStats
{
    uint64_t totalTuples = 0;
    uint64_t totalBuffers = 0;
    uint64_t windowCount = 0;
};

/// Reads system query unix sockets in background threads:
/// - query_status: terminal events for query lifecycle
/// - pipeline_compilation_times: per-pipeline compile times, aggregated per query
class SystemSocketReader
{
public:
    /// Construct a reader for the given worker hosts.
    explicit SystemSocketReader(const std::vector<Host>& workerHosts);
    ~SystemSocketReader();

    SystemSocketReader(const SystemSocketReader&) = delete;
    SystemSocketReader& operator=(const SystemSocketReader&) = delete;

    /// Block until at least one event with a terminal status ("stopped" or "failed")
    /// is available, then return all accumulated terminal events and clear them.
    /// Returns empty if the reader is stopped.
    std::vector<QueryStatusEvent> waitForTerminalEvents();

    /// Get accumulated compilation stats for a query (by LocalQueryId UUID string).
    /// Returns zero stats if no compilation events were received for this query.
    CompilationStats getCompilationStats(const std::string& localQueryId) const;

    /// Get accumulated buffer ingestion stats for a query (by LocalQueryId UUID string).
    /// Returns zero stats if no ingestion events were received for this query.
    IngestionStats getIngestionStats(const std::string& localQueryId) const;

    /// Stop all background reader threads.
    void stop();

    /// Wait for all expected system query sockets to appear on the filesystem.
    /// Throws on timeout.
    static void waitForSockets(const std::vector<Host>& workerHosts, std::chrono::milliseconds timeout = std::chrono::milliseconds(30000));

    /// Convert a Host to its query_status socket path.
    static std::string statusSocketPathForHost(const Host& host);

    /// Convert a Host to its pipeline_compilation_times socket path.
    static std::string compilationSocketPathForHost(const Host& host);

    /// Convert a Host to its buffer_ingestion socket path.
    static std::string ingestionSocketPathForHost(const Host& host);

private:
    void statusReaderThread(std::string socketPath);
    void compilationReaderThread(std::string socketPath);
    void ingestionReaderThread(std::string socketPath);

    std::vector<std::thread> threads;
    std::atomic<bool> running{true};

    /// query_status events
    std::mutex statusMutex;
    std::condition_variable cv;
    std::queue<QueryStatusEvent> terminalEvents;

    /// compilation time tracking (guarded by compilationMutex)
    mutable std::mutex compilationMutex;
    std::unordered_map<std::string, CompilationStats> compilationStatsPerQuery;

    /// per-query buffer ingestion stats (guarded by ingestionMutex)
    mutable std::mutex ingestionMutex;
    std::unordered_map<std::string, IngestionStats> ingestionStatsPerQuery;
};

}
