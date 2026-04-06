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
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <SystemStatsBroadcaster.hpp>
#include <SingleNodeWorkerRPCService.pb.h>

namespace NES::Systest
{

struct QueryStatusEvent
{
    uint64_t timestampMs;
    std::string queryId;
    std::string status;
};

struct CompilationStats
{
    uint64_t totalCompileTimeNs = 0;
    uint64_t pipelineCount = 0;
};

struct IngestionStats
{
    uint64_t totalTuples = 0;
    uint64_t totalBuffers = 0;
    uint64_t windowCount = 0;
};

/// Reads system stat events via the SystemStatsBroadcaster (embedded mode)
/// or gRPC streaming (remote mode). Replaces SystemSocketReader.
class GrpcSystemStatsReader
{
public:
    /// Embedded mode: subscribe directly to the broadcaster (in-process, no gRPC).
    explicit GrpcSystemStatsReader(SystemStatsBroadcaster* broadcaster);

    /// Remote mode: connect to gRPC streaming endpoints on each worker.
    explicit GrpcSystemStatsReader(const std::vector<std::string>& grpcAddresses);

    ~GrpcSystemStatsReader();

    GrpcSystemStatsReader(const GrpcSystemStatsReader&) = delete;
    GrpcSystemStatsReader& operator=(const GrpcSystemStatsReader&) = delete;

    std::vector<QueryStatusEvent> waitForTerminalEvents();
    CompilationStats getCompilationStats(const std::string& localQueryId) const;
    IngestionStats getIngestionStats(const std::string& localQueryId) const;
    void stop();

private:
    void processEvent(const SystemStatEvent& event);
    void embeddedReaderThread(std::shared_ptr<SystemStatsSubscriber> subscriber);
    void grpcReaderThread(std::string grpcAddress);

    std::atomic<bool> running{true};
    std::vector<std::thread> threads;

    SystemStatsBroadcaster* broadcaster = nullptr;
    std::shared_ptr<SystemStatsSubscriber> embeddedSubscriber;

    std::mutex statusMutex;
    std::condition_variable cv;
    std::queue<QueryStatusEvent> terminalEvents;

    mutable std::mutex compilationMutex;
    std::unordered_map<std::string, CompilationStats> compilationStatsPerQuery;

    mutable std::mutex ingestionMutex;
    std::unordered_map<std::string, IngestionStats> ingestionStatsPerQuery;
};

}
