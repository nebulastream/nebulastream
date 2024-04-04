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

#include <QueryCompiler/Operators/PhysicalOperators/StatisticCollection/PhysicalHyperLogLogBuildOperator.hpp>
namespace NES::QueryCompilation::PhysicalOperators {

PhysicalOperatorPtr
PhysicalHyperLogLogBuildOperator::create(const OperatorId operatorId,
                                      const StatisticId statisticId,
                                      const SchemaPtr& inputSchema,
                                      const SchemaPtr& outputSchema,
                                      const std::string& nameOfFieldToTrack,
                                      const uint64_t width,
                                      const Statistic::MetricHash metricHash,
                                      const Windowing::WindowTypePtr windowType,
                                      const Statistic::SendingPolicyPtr sendingPolicy) {
    return std::make_shared<PhysicalHyperLogLogBuildOperator>(PhysicalHyperLogLogBuildOperator(operatorId, statisticId, inputSchema, outputSchema, nameOfFieldToTrack, width, metricHash, windowType, sendingPolicy));
}

PhysicalOperatorPtr
PhysicalHyperLogLogBuildOperator::create(const StatisticId statisticId,
                                      const SchemaPtr& inputSchema,
                                      const SchemaPtr& outputSchema,
                                      const std::string& nameOfFieldToTrack,
                                      const uint64_t width,
                                      const Statistic::MetricHash metricHash,
                                      const Windowing::WindowTypePtr windowType,
                                      const Statistic::SendingPolicyPtr sendingPolicy) {
    return create(getNextOperatorId(), statisticId, inputSchema, outputSchema, nameOfFieldToTrack,
                  width, metricHash, windowType, sendingPolicy);
}

PhysicalHyperLogLogBuildOperator::PhysicalHyperLogLogBuildOperator(
    const OperatorId id,
    const StatisticId statisticId,
    const SchemaPtr& inputSchema,
    const SchemaPtr& outputSchema,
    const std::string& nameOfFieldToTrack,
    const uint64_t width,
    const Statistic::MetricHash metricHash,
    const Windowing::WindowTypePtr windowType,
    const Statistic::SendingPolicyPtr sendingPolicy)
    : Operator(id, statisticId),
      PhysicalSynopsisBuildOperator(nameOfFieldToTrack, metricHash, windowType, sendingPolicy),
      PhysicalUnaryOperator(id, statisticId, inputSchema, outputSchema),
      width(width) {}

OperatorPtr PhysicalHyperLogLogBuildOperator::copy() {
    auto copy = create(id, statisticId, inputSchema, outputSchema, nameOfFieldToTrack, width, metricHash, windowType, sendingPolicy);
    copy->as<PhysicalHyperLogLogBuildOperator>()->setInputOriginIds(inputOriginIds);
    return copy;
}

uint64_t PhysicalHyperLogLogBuildOperator::getWidth() const { return width; }

} // namespace NES::QueryCompilation::PhysicalOperators