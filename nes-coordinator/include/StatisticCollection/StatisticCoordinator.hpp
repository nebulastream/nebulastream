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

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTIC_STATISTICCOORDINATOR_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTIC_STATISTICCOORDINATOR_HPP_
#include <GRPC/WorkerRPCClient.hpp>
#include <Services/RequestHandlerService.hpp>
#include <StatisticCollection/QueryGeneration/DefaultStatisticQueryGenerator.hpp>
#include <StatisticCollection/QueryGeneration/StatisticIdsExtractor.hpp>
#include <StatisticCollection/StatisticCache/AbstractStatisticCache.hpp>
#include <StatisticCollection/StatisticInterface.hpp>
#include <StatisticCollection/StatisticProbeHandling/AbstractStatisticProbeGenerator.hpp>
#include <StatisticCollection/StatisticProbeHandling/ProbeExpression.hpp>
#include <StatisticCollection/StatisticRegistry/StatisticRegistry.hpp>
#include <Statistics/StatisticKey.hpp>
#include <functional>

namespace NES::Statistic {

class StatisticCoordinator;
using StatisticCoordinatorPtr = std::shared_ptr<StatisticCoordinator>;

class StatisticCoordinator : public StatisticInterface {
  public:
    /**
     * @brief Constructs a StatisticCoordinator
     * @param requestHandlerService
     * @param statisticQueryGenerator
     * @param queryCatalog
     */
    StatisticCoordinator(const RequestHandlerServicePtr& requestHandlerService,
                         const StatisticQueryGeneratorPtr& statisticQueryGenerator,
                         const Catalogs::Query::QueryCatalogPtr& queryCatalog,
                         const TopologyPtr& topology);

    /**
     * @brief Implements trackStatistic from StatisticInterface
     */
    std::vector<StatisticKey> trackStatistic(const CharacteristicPtr& characteristic,
                                             const Windowing::WindowTypePtr& window,
                                             const TriggerConditionPtr& triggerCondition,
                                             const SendingPolicyPtr& sendingPolicy,
                                             std::function<void(CharacteristicPtr)>&& callBack) override;
    /**
     * @brief Calling trackStatistic(characteristic, window, SENDING_LAZY), meaning that no trigger condition and
     * we use the SENDING_LAZY policy
     */
    std::vector<StatisticKey> trackStatistic(const CharacteristicPtr& characteristic, const Windowing::WindowTypePtr& window);

    /**
     * @brief Calling trackStatistic(characteristic, window, NeverTrigger(), sendingPolicy, nullptr)
     */
    std::vector<StatisticKey> trackStatistic(const CharacteristicPtr& characteristic,
                                             const Windowing::WindowTypePtr& window,
                                             const SendingPolicyPtr& sendingPolicy);

    /**
     * @brief Implements probeStatistics from StatisticInterface
     */
    ProbeResult<> probeStatistic(const StatisticKey& statisticKey,
                                 const Windowing::TimeMeasure& startTs,
                                 const Windowing::TimeMeasure& endTs,
                                 const Windowing::TimeMeasure& granularity,
                                 const ProbeExpression& probeExpression,
                                 const bool& estimationAllowed,
                                 std::function<ProbeResult<>(ProbeResult<>)>&& aggFunction) override;

    /**
     * @brief Calling probeStatistics with an aggregation function that does not change the ProbeResult
     */
    ProbeResult<> probeStatistic(const StatisticKey& statisticKey,
                                 const Windowing::TimeMeasure& startTs,
                                 const Windowing::TimeMeasure& endTs,
                                 const Windowing::TimeMeasure& granularity,
                                 const ProbeExpression& probeExpression,
                                 const bool& estimationAllowed);

    /**
     * @brief Returns the queryId for a given StatisticKey. THIS SHOULD BE ONLY USED FOR TESTING
     * @param statisticKey
     * @return QueryId
     */
    QueryId getStatisticQueryId(const StatisticKey& statisticKey) const;

    /**
     * @brief Virtual destructor
     */
    virtual ~StatisticCoordinator();

  private:
    RequestHandlerServicePtr requestHandlerService;
    StatisticIdsExtractor statisticIdsExtractor;
    StatisticQueryGeneratorPtr statisticQueryGenerator;
    StatisticRegistry statisticRegistry;
    StatisticProbeGeneratorPtr statisticProbeHandler;
    StatisticCachePtr statisticCache;
    TopologyPtr topology;
    Catalogs::Query::QueryCatalogPtr queryCatalog;
    WorkerRPCClientPtr workerRpcClient;
};

}// namespace NES::Statistic

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTIC_STATISTICCOORDINATOR_HPP_
