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

#ifndef NES_STATISTICS_INCLUDE_STATISTICCOLLECTION_STATISTICPROBEHANDLING_STATISTICPROBEHANDLER_HPP_
#define NES_STATISTICS_INCLUDE_STATISTICCOLLECTION_STATISTICPROBEHANDLING_STATISTICPROBEHANDLER_HPP_
#include <StatisticCollection/StatisticProbeHandling/AbstractStatisticProbeGenerator.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <StatisticCollection/StatisticCache/AbstractStatisticCache.hpp>
#include <StatisticCollection/StatisticProbeHandling/StatisticProbeInterface.hpp>
#include <StatisticCollection/StatisticRegistry/StatisticRegistry.hpp>
#include <StatisticCollection/StatisticProbeHandling/gRPC/WorkerStatisticRPCClient.hpp>

namespace NES::Statistic {

class StatisticProbeHandler;
using StatisticProbeHandlerPtr = std::shared_ptr<StatisticProbeHandler>;

class StatisticProbeHandler : public StatisticProbeInterface {
  public:

    static StatisticProbeHandlerPtr create(const StatisticRegistryPtr& statisticRegistry,
                                             const StatisticProbeGeneratorPtr& statisticProbeGenerator,
                                             const StatisticCachePtr& statisticCache,
                                             const TopologyPtr& topology);

    ProbeResult<> probeStatistic(const StatisticProbeRequest& probeRequest,
                                 const bool& estimationAllowed,
                                 std::function<ProbeResult<>(ProbeResult<>)>&& aggFunction) override;

    /**
     * @brief Calls the probeStatistic function with estimationAllowed set to false
     * @param probeRequest
     * @return ProbeResult<>
     */
    ProbeResult<> probeStatistic(const StatisticProbeRequest& probeRequest);

    /**
     * @brief Returns the queryId for a given StatisticKey. THIS SHOULD BE ONLY USED FOR TESTING!!!
     * @param statisticKey
     * @return QueryId
     */
    QueryId getStatisticQueryId(const StatisticKey& statisticKey) const;

  private:
    StatisticProbeHandler(const StatisticRegistryPtr statisticRegistry,
                          const StatisticProbeGeneratorPtr statisticProbeGenerator,
                          const StatisticCachePtr statisticCache,
                          const TopologyPtr topology);

    StatisticRegistryPtr statisticRegistry;
    StatisticProbeGeneratorPtr statisticProbeGenerator;
    StatisticCachePtr statisticCache;
    TopologyPtr topology;
    WorkerStatisticRPCClientPtr workerRpcClientPtr;
};

}// namespace NES::Statistic

#endif // NES_STATISTICS_INCLUDE_STATISTICCOLLECTION_STATISTICPROBEHANDLING_STATISTICPROBEHANDLER_HPP_
