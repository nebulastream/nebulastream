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

#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/StatisticSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Descriptor/CountMinDescriptor.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Descriptor/HyperLogLogDescriptor.hpp>
#include <Operators/LogicalOperators/StatisticCollection/LogicalStatisticWindowOperator.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Metrics/BufferRate.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Metrics/Cardinality.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Metrics/IngestionRate.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Metrics/MinVal.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Metrics/Selectivity.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <StatisticCollection/Characteristic/DataCharacteristic.hpp>
#include <StatisticCollection/Characteristic/InfrastructureCharacteristic.hpp>
#include <StatisticCollection/Characteristic/WorkloadCharacteristic.hpp>
#include <StatisticCollection/QueryGeneration/DefaultStatisticQueryGenerator.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Statistic {

StatisticQueryGeneratorPtr DefaultStatisticQueryGenerator::create() { return std::make_shared<DefaultStatisticQueryGenerator>(); }

Query DefaultStatisticQueryGenerator::createStatisticQuery(const Characteristic& characteristic,
                                                           const Windowing::WindowTypePtr& window,
                                                           const SendingPolicyPtr& sendingPolicy,
                                                           const TriggerConditionPtr& triggerCondition,
                                                           const Catalogs::Query::QueryCatalog& queryCatalog) {

    // Creating the synopsisDescriptor depending on the metric type
    const auto metricType = characteristic.getType();
    WindowStatisticDescriptorPtr statisticDescriptor;
    StatisticSynopsisType synopsisType;
    if (metricType->instanceOf<Selectivity>()) {
        statisticDescriptor = CountMinDescriptor::create(metricType->getField());
        synopsisType = StatisticSynopsisType::COUNT_MIN;
    } else if (metricType->instanceOf<IngestionRate>()) {
        statisticDescriptor = CountMinDescriptor::create(metricType->getField());
        synopsisType = StatisticSynopsisType::COUNT_MIN;
    } else if (metricType->instanceOf<BufferRate>()) {
        statisticDescriptor = CountMinDescriptor::create(metricType->getField());
        synopsisType = StatisticSynopsisType::COUNT_MIN;
    } else if (metricType->instanceOf<Cardinality>()) {
        statisticDescriptor = HyperLogLogDescriptor::create(metricType->getField());
        synopsisType = StatisticSynopsisType::HLL;
    } else if (metricType->instanceOf<MinVal>()) {
        statisticDescriptor = CountMinDescriptor::create(metricType->getField());
        synopsisType = StatisticSynopsisType::COUNT_MIN;
    } else {
        NES_NOT_IMPLEMENTED();
    }

    /*
     * For a workload characteristic, we have to copy all operators before the operator we are interested in.
     * This way, we create an additional query but, as we are copying the operators beforehand, we can use query merging
     * to only have one running query
     */
    const auto statisticDataCodec = sendingPolicy->getSinkDataCodec();
    if (characteristic.instanceOf<WorkloadCharacteristic>()) {
        const auto workloadCharacteristic = characteristic.as<const WorkloadCharacteristic>();
        const auto queryId = workloadCharacteristic->getQueryId();
        const auto operatorId = workloadCharacteristic->getOperatorId();
        const auto queryPlan = queryCatalog.getCopyOfLogicalInputQueryPlan(queryId);

        // Get the operator that we want to collect statistics for
        auto operatorToTrack = queryPlan->getOperatorWithOperatorId(operatorId);

        // Creating statistic operator and statistic sink
        auto statisticBuildOperator = LogicalOperatorFactory::createStatisticBuildOperator(std::move(window),
                                                                                           std::move(statisticDescriptor),
                                                                                           metricType->hash(),
                                                                                           sendingPolicy,
                                                                                           triggerCondition);
        auto statisticSinkOperator =
            LogicalOperatorFactory::createSinkOperator(StatisticSinkDescriptor::create(synopsisType, statisticDataCodec),
                                                       INVALID_WORKER_NODE_ID);
        statisticBuildOperator->addParent(statisticSinkOperator);

        // As we are operating on a queryPlanCopy, we can replace the operatorUnderTest with a statistic build operator
        // and then cut all parents of it, so that we can insert our statistic sink
        operatorToTrack->replace(statisticBuildOperator);
        operatorToTrack->removeAllParent();

        // Setting the statistic sink as the only root operator
        queryPlan->clearRootOperators();
        queryPlan->addRootOperator(statisticSinkOperator);

        NES_DEBUG("Created query: {}", queryPlan->toString());
        return {queryPlan};

    } else {
        // Building the query depending on the characteristic
        std::string logicalSourceName;
        if (characteristic.instanceOf<DataCharacteristic>()) {
            auto dataCharacteristic = characteristic.as<const DataCharacteristic>();
            logicalSourceName = dataCharacteristic->getLogicalSourceName();
        } else if (characteristic.instanceOf<InfrastructureStatistic>()) {
            auto infrastructureCharacteristic = characteristic.as<const InfrastructureStatistic>();
            logicalSourceName = INFRASTRUCTURE_BASE_LOGICAL_SOURCE_NAME + infrastructureCharacteristic->getNodeId().toString();
        } else {
            NES_NOT_IMPLEMENTED();
        }

        const auto query = Query::from(logicalSourceName)
                               .buildStatistic(window, statisticDescriptor, metricType->hash(), sendingPolicy, triggerCondition)
                               .sink(StatisticSinkDescriptor::create(synopsisType, statisticDataCodec));
        NES_DEBUG("Created query: {}", query.getQueryPlan()->toString());
        return query;
    }
}

DefaultStatisticQueryGenerator::~DefaultStatisticQueryGenerator() = default;
}// namespace NES::Statistic
