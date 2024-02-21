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
 * @brief this class stores the 1D array of a Reservoir-Sample,
 * its meta data and provides the functionalities associated with a reservoir.
 */
class ReservoirSample : public Statistic {
  public:
    /**
     * @param data the data of the sketch
     * @param statisticCollectorIdentifier the key of the sketch
     * @param observedTuples the number of observedTuples of the sketch
     * @param depth the depth of the sketch
     */
    ReservoirSample(const std::vector<uint64_t>& data,
                    StatisticCollectorIdentifierPtr statisticCollectorIdentifier,
                    uint64_t observedTuples,
                    uint64_t depth);

    double probe(StatisticProbeParameterPtr& probeParameters) override;

    /**
     * @return a vector containing the data of the sample
     */
    std::vector<uint64_t>& getData();

  private:
    std::vector<uint64_t> data;
};
}// namespace NES::Experimental::Statistics
#endif//NES_NES_RUNTIME_INCLUDE_STATISTICS_STATISTICS_RESERVOIRSAMPLE_HPP_
