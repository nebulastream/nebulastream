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

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICPROBEHANDLING_ABSTRACTSTATISTICPROBEHANDLER_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICPROBEHANDLING_ABSTRACTSTATISTICPROBEHANDLER_HPP_

#include <Catalogs/Topology/Topology.hpp>
#include <StatisticCollection/StatisticRegistry/StatisticRegistry.hpp>
#include <StatisticCollection/StatisticRequests.hpp>
namespace NES::Statistic {

class AbstractStatisticProbeHandler;
using AbstractStatisticProbeHandlerPtr = std::shared_ptr<AbstractStatisticProbeHandler>;

/**
 * @brief Abstract class that defines an interface of creating StatisticProbeRequestGRPC for a given StatisticProbeRequest.
 * The StatisticProbeRequestGRPC is used to send a probe request to a worker node and then collect the statistics.
 */
class AbstractStatisticProbeHandler {
  public:
    /**
     * @brief Creates a vector of StatisticProbeRequestGRPC for a given StatisticProbeRequest.
     * @param registry
     * @param probeRequest
     * @param topology
     * @return Vector of StatisticProbeRequestGRPC
     */
    virtual std::vector<StatisticProbeRequestGRPC>
    getProbeRequests(const StatisticRegistry& registry, const StatisticProbeRequest& probeRequest, const Topology& topology) = 0;

    virtual ~AbstractStatisticProbeHandler();
};

}// namespace NES::Statistic

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICPROBEHANDLING_ABSTRACTSTATISTICPROBEHANDLER_HPP_
