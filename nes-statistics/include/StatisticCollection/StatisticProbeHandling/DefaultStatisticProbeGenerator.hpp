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

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICPROBEHANDLING_DEFAULTSTATISTICPROBEHANDLER_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICPROBEHANDLING_DEFAULTSTATISTICPROBEHANDLER_HPP_

#include <StatisticCollection/StatisticProbeHandling/AbstractStatisticProbeGenerator.hpp>

namespace NES::Statistic {

/**
 * @brief Default implementation of the AbstractStatisticProbeHandler. It simply returns a vector of StatisticProbeRequestGRPC
 * that will then ask EVERY worker node for the statistics.
 */
class DefaultStatisticProbeGenerator : public AbstractStatisticProbeGenerator {
  public:
    /**
     * @brief Creates a new instance of the DefaultStatisticProbeHandler
     * @return AbstractStatisticProbeHandlerPtr
     */
    static StatisticProbeGeneratorPtr create();

    /**
     * @brief Creates one probe request per worker node, regardless if the statistic cache contains the requested statistic already
     * @param registry
     * @param cache
     * @param probeRequest
     * @param allWorkerIds
     * @return Vector of StatisticProbeRequestGRPC
     */
    std::vector<StatisticProbeRequestGRPC> generateProbeRequests(const StatisticRegistry&,
                                                                 const AbstractStatisticCache&,
                                                                 const StatisticProbeRequest& probeRequest,
                                                                 const std::vector<WorkerId>& allWorkerIds) override;
};

}// namespace NES::Statistic

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICPROBEHANDLING_DEFAULTSTATISTICPROBEHANDLER_HPP_
