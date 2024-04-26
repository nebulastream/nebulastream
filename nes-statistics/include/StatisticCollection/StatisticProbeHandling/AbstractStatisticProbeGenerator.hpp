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

#ifndef NES_STATISTICS_INCLUDE_STATISTICCOLLECTION_STATISTICPROBEHANDLING_ABSTRACTSTATISTICPROBEGENERATOR_HPP_
#define NES_STATISTICS_INCLUDE_STATISTICCOLLECTION_STATISTICPROBEHANDLING_ABSTRACTSTATISTICPROBEGENERATOR_HPP_

#include <StatisticCollection/StatisticCache/AbstractStatisticCache.hpp>
#include <StatisticCollection/StatisticRegistry/StatisticRegistry.hpp>
#include <Statistics/StatisticRequests.hpp>

#include <memory>

namespace NES::Statistic {

class AbstractStatisticProbeGenerator;
using StatisticProbeGeneratorPtr = std::shared_ptr<AbstractStatisticProbeGenerator>;

/**
 * @brief Abstract class that defines an interface of creating StatisticProbeRequestGRPC for a given StatisticProbeRequest.
 * The StatisticProbeRequestGRPC is used to send a probe request to a worker node and then collect the statistics.
 */
class AbstractStatisticProbeGenerator {
  public:
    /**
     * @brief Creates a vector of StatisticProbeRequestGRPC for a given StatisticProbeRequest.
     * @param registry
     * @param probeRequest
     * @param allWorkerIds
     * @return Vector of StatisticProbeRequestGRPC
     */
    virtual std::vector<StatisticProbeRequestGRPC> generateProbeRequests(const StatisticRegistry& registry,
                                                                         const AbstractStatisticCache& cache,
                                                                         const StatisticProbeRequest& probeRequest,
                                                                         const std::vector<WorkerId>& allWorkerIds) = 0;

    virtual ~AbstractStatisticProbeGenerator();
};

}// namespace NES::Statistic

#endif // NES_STATISTICS_INCLUDE_STATISTICCOLLECTION_STATISTICPROBEHANDLING_ABSTRACTSTATISTICPROBEGENERATOR_HPP_
