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

#ifndef NES_RUNTIME_INCLUDE_RUNTIME_QUERYSTATISTICS_HPP_
#define NES_RUNTIME_INCLUDE_RUNTIME_QUERYSTATISTICS_HPP_
#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <folly/Synchronized.h>

namespace NES::Runtime
{

class QueryStatistics;
using QueryStatisticsPtr = std::shared_ptr<QueryStatistics>;

class QueryStatistics
{
public:
    QueryStatistics(QueryId queryId) : queryId(queryId) {};
    QueryStatistics(const QueryStatistics& other);

    void setProcessedTasks(uint64_t processedTasks);
    void setProcessedTuple(uint64_t processedTuple);
    void setProcessedBuffers(uint64_t processedBuffers);

    /// @param noOverwrite If true, the value is only set, if the previous value was 0 (if it was never set).
    void setTimestampQueryStart(uint64_t timestampQueryStart, bool noOverwrite);

    /// @param noOverwrite If true, the value is only set, if the previous value was 0 (if it was never set).
    void setTimestampFirstProcessedTask(uint64_t timestampFirstProcessedTask, bool noOverwrite);

    void setTimestampLastProcessedTask(uint64_t timestampLastProcessedTask);


    void incLatencySum(uint64_t latency);
    void incAvailableGlobalBufferSum(uint64_t size);
    void incAvailableFixedBufferSum(uint64_t size);
    void incQueueSizeSum(uint64_t size);

    void incProcessedTasks();
    void incProcessedTuple(uint64_t tupleCnt);
    void incProcessedBuffers();

    void incProcessedWatermarks();
    void incTasksPerPipelineId(PipelineId pipelineId, WorkerThreadId workerId);


    [[nodiscard]] uint64_t getLatencySum() const;
    [[nodiscard]] uint64_t getAvailableGlobalBufferSum() const;
    [[nodiscard]] uint64_t getAvailableFixedBufferSum() const;
    [[nodiscard]] uint64_t getQueueSizeSum() const;

    [[nodiscard]] uint64_t getProcessedTasks() const;
    [[nodiscard]] uint64_t getProcessedTuple() const;
    [[nodiscard]] uint64_t getProcessedBuffers() const;

    /// In milliseconds
    [[nodiscard]] uint64_t getTimestampQueryStart() const;
    /// In milliseconds
    [[nodiscard]] uint64_t getTimestampFirstProcessedTask() const;
    /// In milliseconds
    [[nodiscard]] uint64_t getTimestampLastProcessedTask() const;

    [[nodiscard]] uint64_t getProcessedWatermarks() const;


    /// Add for the current time stamp (now) a new latency value
    void addTimestampToLatencyValue(uint64_t now, uint64_t latency);

    /// Get the ts to latency map which stores ts as key and latencies in vectors
    folly::Synchronized<std::map<uint64_t, std::vector<uint64_t>>>& getTsToLatencyMap();

    folly::Synchronized<std::map<PipelineId, std::map<WorkerThreadId, std::atomic<uint64_t>>>>& getPipelineIdToTaskMap();

    [[nodiscard]] std::string getQueryStatisticsAsString() const;

    [[nodiscard]] QueryId getQueryId() const;

    /// Clear the content of the statistics
    void clear();

private:
    std::atomic<uint64_t> processedTasks = 0;
    std::atomic<uint64_t> processedTuple = 0;
    std::atomic<uint64_t> processedBuffers = 0;
    std::atomic<uint64_t> processedWatermarks = 0;
    std::atomic<uint64_t> latencySum = 0;
    std::atomic<uint64_t> queueSizeSum = 0;
    std::atomic<uint64_t> availableGlobalBufferSum = 0;
    std::atomic<uint64_t> availableFixedBufferSum = 0;

    std::atomic<uint64_t> timestampQueryStart = 0;
    std::atomic<uint64_t> timestampFirstProcessedTask = 0;
    std::atomic<uint64_t> timestampLastProcessedTask = 0;

    std::atomic<QueryId> queryId = INVALID_QUERY_ID;
    folly::Synchronized<std::map<uint64_t, std::vector<uint64_t>>> tsToLatencyMap;
    folly::Synchronized<std::map<PipelineId, std::map<WorkerThreadId, std::atomic<uint64_t>>>> pipelineIdToTaskThroughputMap;
};

} /// namespace NES::Runtime

#endif /// NES_RUNTIME_INCLUDE_RUNTIME_QUERYSTATISTICS_HPP_
