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
#include <Operators/LogicalOperators/StatisticCollection/Characteristic/DataCharacteristic.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Characteristic/InfrastructureCharacteristic.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Characteristic/WorkloadCharacteristic.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Descriptor/CountMinDescriptor.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Descriptor/HyperLogLogDescriptor.hpp>
#include <Operators/LogicalOperators/StatisticCollection/LogicalStatisticWindowOperator.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Statistics/Metrics/BufferRate.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Statistics/Metrics/Cardinality.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Statistics/Metrics/IngestionRate.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Statistics/Metrics/MinVal.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Statistics/Metrics/Selectivity.hpp>
#include <StatisticCollection/QueryGeneration/DefaultStatisticQueryGenerator.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Statistic {

Query DefaultStatisticQueryGenerator::createStatisticQuery(const Characteristic& characteristic,
                                                           const Windowing::WindowTypePtr& window,
                                                           const SendingPolicyPtr& sendingPolicy,
                                                           const TriggerConditionPtr& triggerCondition) {
    if (characteristic.instanceOf<WorkloadCharacteristic>()) {
        // TODO will be fixed in issue #4630
        NES_NOT_IMPLEMENTED();
    }

    // Creating the synopsisDescriptor depending on the metric type
    const auto metricType = characteristic.getType();
    WindowStatisticDescriptorPtr synopsisDescriptor;
    if (metricType->instanceOf<Selectivity>()) {
        synopsisDescriptor = CountMinDescriptor::create(metricType->getField());
    } else if (metricType->instanceOf<IngestionRate>()) {
        synopsisDescriptor = CountMinDescriptor::create(metricType->getField());
    } else if (metricType->instanceOf<BufferRate>()) {
        synopsisDescriptor = CountMinDescriptor::create(metricType->getField());
    } else if (metricType->instanceOf<Cardinality>()) {
        synopsisDescriptor = HyperLogLogDescriptor::create(metricType->getField());
    } else if (metricType->instanceOf<MinVal>()) {
        synopsisDescriptor = CountMinDescriptor::create(metricType->getField());
    } else {
        NES_NOT_IMPLEMENTED();
    }

    synopsisDescriptor->setTriggerCondition(triggerCondition);
    synopsisDescriptor->setSendingPolicy(sendingPolicy);

    // Building the query depending on the characteristic
    std::string logicalSourceName;
    if (characteristic.instanceOf<DataCharacteristic>()) {
        auto dataCharacteristic = characteristic.as<const DataCharacteristic>();
        logicalSourceName = dataCharacteristic->getLogicalSourceName();
    } else if (characteristic.instanceOf<InfrastructureStatistic>()) {
        auto infrastructureCharacteristic = characteristic.as<const InfrastructureStatistic>();
        logicalSourceName = INFRASTRUCTURE_BASE_LOGICAL_SOURCE_NAME + std::to_string(infrastructureCharacteristic->getNodeId());
    } else {
        NES_NOT_IMPLEMENTED();
    }

    // We add a statistic sink with issue #4632 here
    return Query::from(logicalSourceName)
        .buildStatistic(window, synopsisDescriptor, metricType->hash())
        .sink(NullOutputSinkDescriptor::create());
}
}// namespace NES::Statistic