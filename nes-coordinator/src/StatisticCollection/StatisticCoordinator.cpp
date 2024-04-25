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

#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicyLazy.hpp>
#include <Operators/LogicalOperators/StatisticCollection/TriggerCondition/NeverTrigger.hpp>
#include <StatisticCollection/StatisticCache/DefaultStatisticCache.hpp>
#include <StatisticCollection/StatisticCoordinator.hpp>
#include <StatisticCollection/StatisticProbeHandling/DefaultStatisticProbeGenerator.hpp>
#include <Types/WindowType.hpp>

namespace NES::Statistic {
std::vector<StatisticKey> StatisticCoordinator::trackStatistic(const CharacteristicPtr& characteristic,
                                                               const Windowing::WindowTypePtr& window,
                                                               const TriggerConditionPtr& triggerCondition,
                                                               const SendingPolicyPtr& sendingPolicy,
                                                               std::function<void(CharacteristicPtr)>&& callBack) {

    // 1. Creating a query that collects the required statistic
    const auto statisticQuery =
        statisticQueryGenerator->createStatisticQuery(*characteristic, window, sendingPolicy, triggerCondition, *queryCatalog);

    // 2. Submitting the query to the system
    const auto queryId = requestHandlerService->validateAndQueueAddQueryRequest(statisticQuery.getQueryPlan(),
                                                                                Optimizer::PlacementStrategy::BottomUp);

    // 3. Extracting all statistic ids from the submitted queries to create StatisticKeys out of the statistic ids
    const auto newStatisticIds = statisticIdsExtractor.extractStatisticIdsFromQueryId(queryCatalog, queryId);
    const auto metric = characteristic->getType();

    // 4. For each statistic id, we create a new StatisticKey and also insert it into the registry, if the StatisticKey
    // is not already contained in the registry
    std::vector<StatisticKey> statisticKeysForThisTrackRequest;
    for (const auto& statisticId : newStatisticIds) {
        StatisticKey newKey(metric, statisticId);
        statisticKeysForThisTrackRequest.emplace_back(newKey);
        if (!statisticRegistry.contains(newKey.hash())) {
            StatisticInfo statisticInfo(window, triggerCondition, callBack, queryId, metric);
            statisticRegistry.insert(newKey.hash(), statisticInfo);
        }
    }

    // 5. We return all statisticKeys that belong to this track request, so that they can be used for probing the statistics
    return statisticKeysForThisTrackRequest;
}

std::vector<StatisticKey> StatisticCoordinator::trackStatistic(const CharacteristicPtr& characteristic,
                                                               const Windowing::WindowTypePtr& window) {
    return trackStatistic(characteristic, window, SENDING_LAZY(StatisticDataCodec::DEFAULT));
}

std::vector<StatisticKey> StatisticCoordinator::trackStatistic(const CharacteristicPtr& characteristic,
                                                               const Windowing::WindowTypePtr& window,
                                                               const SendingPolicyPtr& sendingPolicy) {
    return trackStatistic(characteristic, window, NeverTrigger::create(), sendingPolicy, nullptr);
}

ProbeResult<> StatisticCoordinator::probeStatistic(const StatisticKey& statisticKey,
                                                   const Windowing::TimeMeasure& startTs,
                                                   const Windowing::TimeMeasure& endTs,
                                                   const Windowing::TimeMeasure& granularity,
                                                   const ProbeExpression& probeExpression,
                                                   const bool&,// #4682 will implement this
                                                   std::function<ProbeResult<>(ProbeResult<>)>&& aggFunction) {
    // 1. Check if there exist a statistic for this key
    if (!statisticRegistry.contains(statisticKey.hash())) {
        NES_INFO("Could not find a statistic collection query for StatisticKey={}", statisticKey.toString());
        return {};
    }

    // 2. Check if the statistic is tracked with the same granularity as wished
    const auto statisticInfo = statisticRegistry.getStatisticInfoWithGranularity(statisticKey.hash(), granularity);
    if (!statisticInfo.has_value()) {
        NES_INFO("Could not find a statistic collection query for StatisticKey={} with granularity={}",
                 statisticKey.toString(),
                 granularity.toString());
        return {};
    }

    // 3. Getting all probe requests of nodes that we have to query for the statistic
    StatisticProbeRequest statisticProbeRequest(statisticKey.hash(), startTs, endTs, granularity, probeExpression);
    const auto allProbeRequests = statisticProbeHandler->generateProbeRequests(statisticRegistry,
                                                                               *statisticCache,
                                                                               statisticProbeRequest,
                                                                               topology->getAllRegisteredNodeIds());

    // 4. Receive all statistics by sending the probe requests to the nodes
    ProbeResult<> probeResult;
    for (const auto& probeRequest : allProbeRequests) {
        const auto topologyNode = topology->getCopyOfTopologyNodeWithId(probeRequest.workerId);
        const auto gRPCAddress = topologyNode->getIpAddress() + ":" + std::to_string(topologyNode->getGrpcPort());
        const auto statisticValues = workerRpcClient->probeStatistics(gRPCAddress, probeRequest);
        for (const auto& stat : statisticValues) {
            probeResult.addStatisticValue(stat);

            // Feeding it to the statistic cache for future use
            statisticCache->insertStatistic(statisticKey.hash(), stat);
        }
    }

    // 5. Calling the aggregation function and then returning the result
    return aggFunction(probeResult);
}

ProbeResult<> StatisticCoordinator::probeStatistic(const StatisticKey& statisticKey,
                                                   const Windowing::TimeMeasure& startTs,
                                                   const Windowing::TimeMeasure& endTs,
                                                   const Windowing::TimeMeasure& granularity,
                                                   const ProbeExpression& probeExpression,
                                                   const bool& estimationAllowed) {
    return probeStatistic(statisticKey,
                          startTs,
                          endTs,
                          granularity,
                          probeExpression,
                          estimationAllowed,
                          [](const ProbeResult<>& probeResult) {
                              return probeResult;
                          });
}

QueryId StatisticCoordinator::getStatisticQueryId(const StatisticKey& statisticKey) const {
    return statisticRegistry.getQueryId(statisticKey.hash());
}

StatisticCoordinator::StatisticCoordinator(const RequestHandlerServicePtr& requestHandlerService,
                                           const StatisticQueryGeneratorPtr& statisticQueryGenerator,
                                           const Catalogs::Query::QueryCatalogPtr& queryCatalog,
                                           const TopologyPtr& topology)
    : requestHandlerService(requestHandlerService), statisticQueryGenerator(statisticQueryGenerator),
      statisticProbeHandler(DefaultStatisticProbeGenerator::create()), statisticCache(DefaultStatisticCache::create()),
      topology(topology), queryCatalog(queryCatalog), workerRpcClient(WorkerRPCClient::create()) {}

StatisticCoordinator::~StatisticCoordinator() = default;

}// namespace NES::Statistic