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

#ifndef NES_BENCHMARK_INCLUDE_MEASUREMENTS_HPP_
#define NES_BENCHMARK_INCLUDE_MEASUREMENTS_HPP_

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace NES::Benchmark::Measurements {
/**
 * @brief stores all measurements and provides some helper functions
 */
class Measurements {
  public:
    /**
     * @brief adds a new measurement
     * @param processedTasks
     * @param processedBuffers
     * @param processedTuples
     * @param latencySum
     * @param queueSizeSum
     * @param availGlobalBufferSum
     * @param availFixedBufferSum
     */
    void addNewMeasurement(size_t processedTasks,
                           size_t processedBuffers,
                           size_t processedTuples,
                           size_t latencySum,
                           size_t queueSizeSum,
                           size_t availGlobalBufferSum,
                           size_t availFixedBufferSum,
                           uint64_t timeStamp) {
        addNewProcessedTasks(timeStamp, processedTasks);
        addNewProcessedBuffers(timeStamp, processedBuffers);
        addNewProcessedTuples(timeStamp, processedTuples);
        addNewLatencySum(timeStamp, latencySum);
        addNewQueueSizeSum(timeStamp, queueSizeSum);
        addNewAvailGlobalBufferSum(timeStamp, availGlobalBufferSum);
        addNewAvailFixedBufferSum(timeStamp, availFixedBufferSum);
    }

    /**
     * @brief adds availFixedBufferSum
     * @param timestamp
     * @param availFixedBufferSum
     */
    void addNewTimestamp(size_t timestamp) { timestamps.push_back(timestamp); }

    /**
     * @brief returns the measurements and calculates the deltas of the measurements and then
     * returns them as comma separated values
     * @param schemaSizeInByte
     * @return comma separated values
     */
    std::vector<std::string> getMeasurementsAsCSV(size_t schemaSizeInByte, size_t numberOfQueries);

    /**
     * @brief get only the throughput number
     * @return string
     */
    std::string getThroughputAsString();

  private:
    /**
     * @brief adds processedTasks
     * @param timestamp
     * @param processedTasks
     */
    void addNewProcessedTasks(size_t timestamp, size_t processedTasks) { allProcessedTasks[timestamp] += processedTasks; }

    /**
     * @brief adds processedBuffers
     * @param timestamp
     * @param processedBuffers
     */
    void addNewProcessedBuffers(size_t timestamp, size_t processedBuffers) { allProcessedBuffers[timestamp] += processedBuffers; }

    /**
     * @brief adds processedTuples
     * @param timestamp
     * @param processedTuples
     */
    void addNewProcessedTuples(size_t timestamp, size_t processedTuples) { allProcessedTuples[timestamp] += processedTuples; }

    /**
     * @brief adds latencySum
     * @param timestamp
     * @param newLatencySum
     */
    void addNewLatencySum(size_t timestamp, size_t latencySum) { allLatencySum[timestamp] += latencySum; }

    /**
     * @brief adds queueSizeSum
     * @param timestamp
     * @param queueSizeSum
     */
    void addNewQueueSizeSum(size_t timestamp, size_t queueSizeSum) { allQueueSizeSums[timestamp] += queueSizeSum; }

    /**
     * @brief adds availGlobalBufferSum
     * @param timestamp
     * @param availGlobalBufferSum
     */
    void addNewAvailGlobalBufferSum(size_t timestamp, size_t availGlobalBufferSum) {
        allAvailGlobalBufferSum[timestamp] += availGlobalBufferSum;
    }

    /**
     * @brief adds availFixedBufferSum
     * @param timestamp
     * @param availFixedBufferSum
     */
    void addNewAvailFixedBufferSum(size_t timestamp, size_t availFixedBufferSum) {
        allAvailFixedBufferSum[timestamp] += availFixedBufferSum;
    }

  private:
    std::vector<size_t> timestamps;
    std::map<size_t, size_t> allProcessedTasks;
    std::map<size_t, size_t> allProcessedBuffers;
    std::map<size_t, size_t> allProcessedTuples;
    std::map<size_t, size_t> allLatencySum;
    std::map<size_t, size_t> allQueueSizeSums;
    std::map<size_t, size_t> allAvailGlobalBufferSum;
    std::map<size_t, size_t> allAvailFixedBufferSum;
};
}// namespace NES::Benchmark::Measurements
#endif // NES_BENCHMARK_INCLUDE_MEASUREMENTS_HPP_
