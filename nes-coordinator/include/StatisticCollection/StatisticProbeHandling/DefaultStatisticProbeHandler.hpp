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

#include <Catalogs/Topology/Topology.hpp>
#include <StatisticCollection/StatisticProbeHandling/AbstractStatisticProbeHandler.hpp>

namespace NES::Statistic {

/**
 * @brief Default implementation of the AbstractStatisticProbeHandler. It simply returns a vector of StatisticProbeRequestGRPC
 * that will then ask EVERY worker node for the statistics.
 */
class DefaultStatisticProbeHandler : public AbstractStatisticProbeHandler {
  public:
    /**
     * @brief Creates a new instance of the DefaultStatisticProbeHandler
     * @return AbstractStatisticProbeHandlerPtr
     */
    static AbstractStatisticProbeHandlerPtr create();

    /**
     * @brief Creates one probe request per worker node, regardless if the statistic cache contains the requested statistic already
     * @param registry
     * @param probeRequest
     * @param topology
     * @return Vector of StatisticProbeRequestGRPC
     */
    std::vector<StatisticProbeRequestGRPC> getProbeRequests(const StatisticRegistry& registry,
                                                            const StatisticProbeRequest& probeRequest,
                                                            const Topology& topology) override;
};

}// namespace NES::Statistic

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICPROBEHANDLING_DEFAULTSTATISTICPROBEHANDLER_HPP_
