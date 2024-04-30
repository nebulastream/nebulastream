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

#include <Catalogs/Topology/TopologyNode.hpp>
#include <StatisticCollection/StatisticProbeHandling/StatisticProbeHandler.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Statistic {

QueryId StatisticProbeHandler::getStatisticQueryId(const StatisticKey& statisticKey) const {
    return statisticRegistry->getQueryId(statisticKey.hash());
}

ProbeResult<> StatisticProbeHandler::probeStatistic(const StatisticProbeRequest& probeRequest) {
    return probeStatistic(probeRequest, false, [](const ProbeResult<>& probeResult) { return probeResult; });
}

ProbeResult<> StatisticProbeHandler::probeStatistic(const StatisticProbeRequest& statisticProbeRequest,
                                                    const bool&,// #4682 will implement this
                                                    std::function<ProbeResult<>(ProbeResult<>)>&& aggFunction) {
    // 1. Check if there exist a statistic for this key
    if (!statisticRegistry->contains(statisticProbeRequest.statisticHash)) {
        NES_INFO("Could not find a statistic collection query for StatisticHash={}", statisticProbeRequest.statisticHash);
        return {};
    }

    // 2. Check if the statistic is tracked with the same granularity as wished
    const auto statisticInfo = statisticRegistry->getStatisticInfoWithGranularity(statisticProbeRequest.statisticHash,
                                                                                  statisticProbeRequest.granularity);
    if (!statisticInfo.has_value()) {
        NES_INFO("Could not find a statistic collection query for StatisticHash={} with granularity={}",
                 statisticProbeRequest.statisticHash,
                 statisticProbeRequest.granularity.toString());
        return {};
    }


    // 3. Getting all probe requests of nodes that we have to query for the statistic
    const auto workerIds = topology->getAllRegisteredNodeIds();
    const auto allProbeRequests =
        statisticProbeGenerator->generateProbeRequests(*statisticRegistry, *statisticCache, statisticProbeRequest, workerIds);

    // 4. Receive all statistics by sending the probe requests to the nodes
    ProbeResult<> probeResult;
    for (const auto& probeRequest : allProbeRequests) {
        const auto topologyNode = topology->getCopyOfTopologyNodeWithId(probeRequest.workerId);
        const auto gRPCAddress = topologyNode->getIpAddress() + ":" + std::to_string(topologyNode->getGrpcPort());
        const auto statisticValues = workerRpcClientPtr->probeStatistics(probeRequest, gRPCAddress);
        for (const auto& stat : statisticValues) {
            probeResult.addStatisticValue(stat);

            // Feeding it to the statistic cache for future use
            statisticCache->insertStatistic(probeRequest.statisticHash, stat);
        }
    }

    // 5. Calling the aggregation function and then returning the result
    return aggFunction(probeResult);
}

StatisticProbeHandlerPtr StatisticProbeHandler::create(const StatisticRegistryPtr& statisticRegistry,
                                                         const StatisticProbeGeneratorPtr& statisticProbeGenerator,
                                                         const StatisticCachePtr& statisticCache,
                                                         const TopologyPtr& topology) {
    return std::make_shared<StatisticProbeHandler>(StatisticProbeHandler(statisticRegistry, statisticProbeGenerator, statisticCache, topology));
}

StatisticProbeHandler::StatisticProbeHandler(const StatisticRegistryPtr statisticRegistry,
                                             const StatisticProbeGeneratorPtr statisticProbeGenerator,
                                             const StatisticCachePtr statisticCache,
                                             const TopologyPtr topology)
    : statisticRegistry(std::move(statisticRegistry)),
      statisticProbeGenerator(std::move(statisticProbeGenerator)),
      statisticCache(std::move(statisticCache)),
      topology(std::move(topology)) {}

}// namespace NES::Statistic