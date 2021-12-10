/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_RUNTIME_QUERY_STATISTICS_HPP_
#define NES_INCLUDE_RUNTIME_QUERY_STATISTICS_HPP_
#include <atomic>
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
    * @brief getter for processedWatermarks
    * @return processedBuffers
    */
    [[nodiscard]] uint64_t getProcessedWatermarks() const;

    /**
    * @brief setter for processedTasks
    * @return processedTasks
    */
    void setProcessedTasks(uint64_t processedTasks);

    /**
   * @brief setter for processedTuple
   * @return processedTuple
   */
    void setProcessedTuple(uint64_t processedTuple);

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
    std::map<uint64_t, std::vector<uint64_t>> getTsToLatencyMap();


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

  private:
    std::atomic<uint64_t> queryId = 0;
    std::atomic<uint64_t> subQueryId = 0;
    std::map<uint64_t, std::vector<uint64_t>> tsToLatencyMap;
};

}// namespace NES::Runtime

#endif// NES_INCLUDE_RUNTIME_QUERY_STATISTICS_HPP_
