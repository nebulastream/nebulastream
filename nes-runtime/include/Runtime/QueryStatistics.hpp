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

#ifndef NES_CORE_INCLUDE_RUNTIME_QUERYSTATISTICS_HPP_
#define NES_CORE_INCLUDE_RUNTIME_QUERYSTATISTICS_HPP_
#include <atomic>
#include <folly/Synchronized.h>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace NES::Runtime {

class QueryStatistics;
using QueryStatisticsPtr = std::shared_ptr<QueryStatistics>;

class QueryStatistics {
  public:
    QueryStatistics(uint64_t queryId, uint64_t subQueryId) : queryId(queryId), subQueryId(subQueryId){};

    QueryStatistics(const QueryStatistics& other);

    /**
     * @brief getter for processedTasks
     * @return processedTasks
     */
    [[nodiscard]] uint64_t getProcessedTasks() const;

    /**
    * @brief getter for processedTuple
    * @return processedTuple
    */
    [[nodiscard]] uint64_t getProcessedTuple() const;

    /**
    * @brief getter for processedBuffers
    * @return processedBuffers
    */
    [[nodiscard]] uint64_t getProcessedBuffers() const;

    /**
    * @brief getter for timestampQueryStart
    * @return timestampQueryStart  In MS.
    */
    [[nodiscard]] uint64_t getTimestampQueryStart() const;

    /**
    * @brief getter for timestampFirstProcessedTask
    * @return timestampFirstProcessedTask  In MS.
    */
    [[nodiscard]] uint64_t getTimestampFirstProcessedTask() const;

    /**
    * @brief getter for timestampLastProcessedTask
    * @return timestampLastProcessedTask  In MS.
    */
    [[nodiscard]] uint64_t getTimestampLastProcessedTask() const;

    /**
    * @brief getter for processedWatermarks
    * @return processedBuffers
    */
    [[nodiscard]] uint64_t getProcessedWatermarks() const;

    /**
    * @brief setter for processedTasks
    */
    void setProcessedTasks(uint64_t processedTasks);

    /**
    * @brief setter for processedTuple
    */
    void setProcessedTuple(uint64_t processedTuple);

    /**
    * @brief setter for timestampQueryStart
    * @param timestampQueryStart In MS.
    * @param noOverwrite If true, the value is only set, if the previous value was 0 (if it was never set).
    */
    void setTimestampQueryStart(uint64_t timestampQueryStart, bool noOverwrite);

    /**
    * @brief setter for timestampFirstProcessedTask
    * @param timestampFirstProcessedTask In MS.
    * @param noOverwrite If true, the value is only set, if the previous value was 0 (if it was never set).
    */
    void setTimestampFirstProcessedTask(uint64_t timestampFirstProcessedTask, bool noOverwrite);

    /**
    * @brief setter for timestampLastProcessedTask
    * @param timestampLastProcessedTask In MS.
    */
    void setTimestampLastProcessedTask(uint64_t timestampLastProcessedTask);

    /**
    * @brief increment processedBuffers
    */
    void incProcessedBuffers();

    /**
    * @brief increment processedTasks
    */
    void incProcessedTasks();

    /**
    * @brief increment processedTuple
    */
    void incProcessedTuple(uint64_t tupleCnt);

    /**
    * @brief increment latency sum
    */
    void incLatencySum(uint64_t latency);

    /**
    * @brief increment latency sum
     * @param pipelineId
     * @param workerId
    */
    void incTasksPerPipelineId(uint64_t pipelineId, uint64_t workerId);

    /**
    * @brief get pipeline id task map
    */
    folly::Synchronized<std::map<uint64_t, std::map<uint64_t, std::atomic<uint64_t>>>>& getPipelineIdToTaskMap();

    /**
     * @brief get sum of all latencies
     * @return value
     */
    [[nodiscard]] uint64_t getLatencySum() const;

    /**
    * @brief increment queue size sum
    */
    void incQueueSizeSum(uint64_t size);

    /**
     * @brief get sum of all available buffers
     * @return value
     */
    [[nodiscard]] uint64_t getAvailableGlobalBufferSum() const;

    /**
    * @brief increment available buffer sum
    */
    void incAvailableGlobalBufferSum(uint64_t size);

    /**
     * @brief get sum of all fixed buffer buffers
     * @return value
     */
    [[nodiscard]] uint64_t getAvailableFixedBufferSum() const;

    /**
    * @brief increment available fixed buffer sum
    */
    void incAvailableFixedBufferSum(uint64_t size);

    /**
     * @brief get sum of all queue sizes
     * @return value
     */
    [[nodiscard]] uint64_t getQueueSizeSum() const;

    /**
    * @brief increment processedWatermarks
    */
    void incProcessedWatermarks();

    /**
    * @brief setter for processedBuffers
    * @return processedBuffers
    */
    void setProcessedBuffers(uint64_t processedBuffers);

    /**
     * @brief return the current statistics as a string
     * @return statistics as a string
     */
    std::string getQueryStatisticsAsString();

    /**
    * @brief get the query id of this queriy
    * @return queryId
    */
    [[nodiscard]] uint64_t getQueryId() const;

    /**
     * @brief get the sub id of this qep (the pipeline stage)
     * @return subqueryID
     */
    [[nodiscard]] uint64_t getSubQueryId() const;

    /**
     * Add for the current time stamp (now) a new latency value
     * @param now
     * @param latency
     */
    void addTimestampToLatencyValue(uint64_t now, uint64_t latency);

    /**
     * get the ts to latency map which stores ts as key and latencies in vectors
     * @return
     */
    folly::Synchronized<std::map<uint64_t, std::vector<uint64_t>>>& getTsToLatencyMap();

    /**
     * clear the content of the statistics
     */
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

    std::atomic<uint64_t> queryId = 0;
    std::atomic<uint64_t> subQueryId = 0;
    folly::Synchronized<std::map<uint64_t, std::vector<uint64_t>>> tsToLatencyMap;
    folly::Synchronized<std::map<uint64_t, std::map<uint64_t, std::atomic<uint64_t>>>> pipelineIdToTaskThroughputMap;
};

}// namespace NES::Runtime

#endif// NES_CORE_INCLUDE_RUNTIME_QUERYSTATISTICS_HPP_
