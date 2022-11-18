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

#ifndef NES_MEASUREMENTS_HPP
#define NES_MEASUREMENTS_HPP

#include <cstdint>
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
        addNewProcessedTasks(processedTasks);
        addNewProcessedBuffers(processedBuffers);
        addNewProcessedTuples(processedTuples);
        addNewLatencySum(latencySum);
        addNewQueueSizeSum(queueSizeSum);
        addNewAvailGlobalBufferSum(availGlobalBufferSum);
        addNewAvailFixedBufferSum(availFixedBufferSum);
        addNewTimeStamp(timeStamp);
    }

    /**
     * @brief returns the measurements and calculates the deltas of the measurements and then
     * returns them as comma separated values
     * @param schemaSizeInByte
     * @return comma separated values
     */
    std::vector<std::string> getMeasurementsAsCSV(size_t schemaSizeInByte);

  private:
    /**
     * @brief adds processedTasks
     * @param processedTasks
     */
    void addNewProcessedTasks(size_t processedTasks) { allProcessedTasks.push_back(processedTasks); }

    /**
     * @brief adds processedBuffers
     * @param processedBuffers
     */
    void addNewProcessedBuffers(size_t processedBuffers) { allProcessedBuffers.push_back(processedBuffers); }

    /**
     * @brief adds processedTuples
     * @param processedTuples
     */
    void addNewProcessedTuples(size_t processedTuples) { allProcessedTuples.push_back(processedTuples); }

    /**
     * @brief adds latencySum
     * @param newLatencySum
     */
    void addNewLatencySum(size_t latencySum) { allLatencySum.push_back(latencySum); }

    /**
     * @brief adds queueSizeSum
     * @param queueSizeSum
     */
    void addNewQueueSizeSum(size_t queueSizeSum) { allQueueSizeSums.push_back(queueSizeSum); }

    /**
     * @brief adds availGlobalBufferSum
     * @param availGlobalBufferSum
     */
    void addNewAvailGlobalBufferSum(size_t availGlobalBufferSum) { allAvailGlobalBufferSum.push_back(availGlobalBufferSum); }

    /**
     * @brief adds availFixedBufferSum
     * @param availFixedBufferSum
     */
    void addNewAvailFixedBufferSum(size_t availFixedBufferSum) { allAvailFixedBufferSum.push_back(availFixedBufferSum); }

    /**
     * @brief adds a timestamp
     * @param timeStamp
     */
    void addNewTimeStamp(uint64_t timeStamp) { allTimeStamps.push_back(timeStamp); }

  private:
    std::vector<uint64_t> allTimeStamps;
    std::vector<size_t> allProcessedTasks;
    std::vector<size_t> allProcessedBuffers;
    std::vector<size_t> allProcessedTuples;
    std::vector<size_t> allLatencySum;
    std::vector<size_t> allQueueSizeSums;
    std::vector<size_t> allAvailGlobalBufferSum;
    std::vector<size_t> allAvailFixedBufferSum;
};
}// namespace NES::Benchmark::Measurements
#endif//NES_MEASUREMENTS_HPP