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

#ifndef NES_INCLUDE_NODEENGINE_QUERYSTATISTICS_HPP_
#define NES_INCLUDE_NODEENGINE_QUERYSTATISTICS_HPP_
#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <vector>
namespace NES::NodeEngine {

class QueryStatistics {
  public:
    QueryStatistics(uint64_t queryId, uint64_t subQueryId)
        : processedTasks(0), processedTuple(0), processedBuffers(0), processedWatermarks(0), queryId(queryId),
          subQueryId(subQueryId){};

    /**
     * @brief getter for processedTasks
     * @return processedTasks
     */
    const std::atomic<uint64_t> getProcessedTasks() const;

    /**
   * @brief getter for processedTuple
   * @return processedTuple
   */
    const std::atomic<uint64_t> getProcessedTuple() const;

    /**
   * @brief getter for processedBuffers
   * @return processedBuffers
   */
    const std::atomic<uint64_t> getProcessedBuffers() const;

    /**
    * @brief getter for processedWatermarks
    * @return processedBuffers
    */
    const std::atomic<uint64_t> getProcessedWatermarks() const;

    /**
    * @brief setter for processedTasks
    * @return processedTasks
    */
    void setProcessedTasks(const std::atomic<uint64_t>& processedTasks);

    /**
   * @brief setter for processedTuple
   * @return processedTuple
   */
    void setProcessedTuple(const std::atomic<uint64_t>& processedTuple);

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
    * @brief increment
    */
    void incLatencySum(uint64_t latency);

    /**
     * @brief get sum of all latencies
     * @return value
     */
    const std::atomic<uint64_t> getLatencySum() const;

    /**
    * @brief increment processedWatermarks
    */
    void incProcessedWatermarks();

    /**
  * @brief setter for processedBuffers
  * @return processedBuffers
  */
    void setProcessedBuffers(const std::atomic<uint64_t>& processedBuffers);

    /**
     * @brief return the current statistics as a string
     * @return statistics as a string
     */
    std::string getQueryStatisticsAsString();

    /**
    * @brief get the query id of this queriy
    * @return queryId
    */
    uint64_t getQueryId() const;

    /**
     * @brief get the sub id of this qep (the pipeline stage)
     * @return subqueryID
     */
    uint64_t getSubQueryId() const;

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

  private:
    std::atomic<uint64_t> processedTasks;
    std::atomic<uint64_t> processedTuple;
    std::atomic<uint64_t> processedBuffers;
    std::atomic<uint64_t> processedWatermarks;
    std::atomic<uint64_t> latencySum;
    std::atomic<uint64_t> queryId;
    std::atomic<uint64_t> subQueryId;
    std::map<uint64_t, std::vector<uint64_t>> tsToLatencyMap;
};

typedef std::shared_ptr<QueryStatistics> QueryStatisticsPtr;

}// namespace NES::NodeEngine

#endif//NES_INCLUDE_NODEENGINE_QUERYSTATISTICS_HPP_
