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
#include <Operators/LogicalOperators/StatisticCollection/Statistics/ProbeExpression.hpp>
#include <Services/RequestHandlerService.hpp>
#include <StatisticCollection/QueryGeneration/DefaultStatisticQueryGenerator.hpp>
#include <StatisticCollection/StatisticInterface.hpp>
#include <StatisticCollection/StatisticRegistry/StatisticIdsExtractor.hpp>
#include <StatisticCollection/StatisticRegistry/StatisticKey.hpp>
#include <StatisticCollection/StatisticRegistry/StatisticRegistry.hpp>
#include <StatisticCollection/StatisticStorage/AbstractStatisticStore.hpp>
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
     * @param statisticStore
     * @param queryCatalog
     */
    StatisticCoordinator(const RequestHandlerServicePtr& requestHandlerService,
                         const AbstractStatisticQueryGeneratorPtr& statisticQueryGenerator,
                         const AbstractStatisticStorePtr& statisticStore,
                         const Catalogs::Query::QueryCatalogPtr& queryCatalog);

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
     * @brief Implements probeStatistic from StatisticInterface
     */
    ProbeResult<> probeStatistic(const StatisticKey& statisticKey,
                                 const Windowing::TimeMeasure& startTs,
                                 const Windowing::TimeMeasure& endTs,
                                 const Windowing::TimeMeasure& granularity,
                                 const ProbeExpression& probeExpression,
                                 const bool& estimationAllowed,
                                 std::function<ProbeResult<>(ProbeResult<>)>&& aggFunction) override;

    /**
     * @brief Calling probeStatistic with an aggregation function that does not change the ProbeResult
     */
    ProbeResult<> probeStatistic(const StatisticKey& statisticKey,
                                 const Windowing::TimeMeasure& startTs,
                                 const Windowing::TimeMeasure& endTs,
                                 const Windowing::TimeMeasure& granularity,
                                 const ProbeExpression& probeExpression,
                                 const bool& estimationAllowed);

    /**
     * @brief Virtual destructor
     */
    virtual ~StatisticCoordinator();

  private:
    RequestHandlerServicePtr requestHandlerService;
    StatisticIdsExtractor statisticIdsExtractor;
    AbstractStatisticQueryGeneratorPtr statisticQueryGenerator;
    AbstractStatisticStorePtr statisticStore;
    StatisticRegistry statisticRegistry;
    Catalogs::Query::QueryCatalogPtr queryCatalog;
};

}// namespace NES::Statistic

#endif//NES_NES_COORDINATOR_INCLUDE_STATISTIC_STATISTICCOORDINATOR_HPP_
