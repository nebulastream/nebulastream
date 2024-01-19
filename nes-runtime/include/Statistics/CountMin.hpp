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

#ifndef NES_NES_RUNTIME_INCLUDE_STATISTICS_STATISTICS_COUNTMIN_HPP_
#define NES_NES_RUNTIME_INCLUDE_STATISTICS_STATISTICS_COUNTMIN_HPP_

#include <vector>

#include <Statistics/Statistic.hpp>

namespace NES::Experimental::Statistics {

/**
 * @brief this class stores the 2D array of a CountMin sketches,
 * its meta data and provides the functionalities associated with Count-Min Sketches
 */
class CountMin : public Statistic {
  public:
    CountMin(uint64_t width,
             const std::vector<std::vector<uint64_t>>& data,
             StatisticCollectorIdentifierPtr statisticCollectorIdentifier,
             const uint64_t observedTuples,
             const uint64_t depth,
             const uint64_t startTime,
             const uint64_t endTime)
        : Statistic(std::move(statisticCollectorIdentifier), observedTuples, depth, startTime, endTime), width(width),
          error(calcError(width)), probability(calcProbability(depth)), data(data) {}

    /**
     * @brief calculates the error that the Count-Min will most often guarantee
     * @param width the width of the Count-Min sketch
     * @return the error
     */
    double calcError(uint64_t width);

    /**
     * @brief calculates the probability, that the actual error exceeds the parametrized error, given the depth
     * @param depth the depth of the Count-Min sketch
     * @return the probability
     */
    double calcProbability(uint64_t depth);

    /**
     * @brief receives a depth and a width and increments the counter in that location
     * @param row the row where the counter is increased by 1
     * @param column the column where the counter is increased by 1
     */
    void increment(uint64_t row, uint64_t column);

    /**
     * @return returns the width of the Count-Min Sketch
     */
    [[nodiscard]] uint64_t getWidth() const;

    /**
     * @return returns the error of the Count-Min Sketch
     */
    [[nodiscard]] double getError() const;

    /**
     * @return returns the probability of the Count-Min Sketch
     */
    [[nodiscard]] double getProbability() const;

    /**
     * @return the sketch data as a vector of vectors
     */
    [[nodiscard]] const std::vector<std::vector<uint64_t>>& getData() const;

  private:
    uint64_t width;
    double error;
    double probability;
    std::vector<std::vector<uint64_t>> data;
};
}// namespace NES::Experimental::Statistics
#endif//NES_NES_RUNTIME_INCLUDE_STATISTICS_STATISTICS_COUNTMIN_HPP_
