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

#ifndef NES_NES_RUNTIME_INCLUDE_STATISTICS_STATISTICS_RESERVOIRSAMPLE_HPP_
#define NES_NES_RUNTIME_INCLUDE_STATISTICS_STATISTICS_RESERVOIRSAMPLE_HPP_

#include <Statistics/Statistic.hpp>

namespace NES::Experimental::Statistics {

/**
 * @brief
 */
class ReservoirSample : public Statistic {
  public:
    /**
     * @brief
     * @param data
     * @param statisticCollectorIdentifier
     * @param observedTuples
     * @param depth
     * @param startTime
     * @param endTime
     */
    ReservoirSample(const std::vector<uint64_t>& data,
                    StatisticCollectorIdentifierPtr statisticCollectorIdentifier,
                    const uint64_t observedTuples,
                    const uint64_t depth,
                    const uint64_t startTime,
                    const uint64_t endTime);

    /**
     * @return a vector containing the data of the sample
     */
    std::vector<uint64_t>& getData();

  private:
    std::vector<uint64_t> data;
};
}// namespace NES::Experimental::Statistics
#endif//NES_NES_RUNTIME_INCLUDE_STATISTICS_STATISTICS_RESERVOIRSAMPLE_HPP_
