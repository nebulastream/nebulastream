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

#include <QueryCompiler/Operators/PhysicalOperators/StatisticCollection/PhysicalCountMinBuildOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalOperatorPtr
PhysicalCountMinBuildOperator::create(const OperatorId operatorId,
                                      const StatisticId statisticId,
                                      const SchemaPtr& inputSchema,
                                      const SchemaPtr& outputSchema,
                                      const std::string& nameOfFieldToTrack,
                                      const uint64_t width,
                                      const uint64_t depth,
                                      const Statistic::StatisticMetricHash metricHash,
                                      const Windowing::WindowTypePtr windowType,
                                      const Statistic::SendingPolicyPtr sendingPolicy) {
    return std::make_shared<PhysicalCountMinBuildOperator>(PhysicalCountMinBuildOperator(operatorId, statisticId, inputSchema, outputSchema, nameOfFieldToTrack, width, depth, metricHash, windowType, sendingPolicy));
}

PhysicalOperatorPtr
PhysicalCountMinBuildOperator::create(const StatisticId statisticId,
                                      const SchemaPtr& inputSchema,
                                      const SchemaPtr& outputSchema,
                                      const std::string& nameOfFieldToTrack,
                                      const uint64_t width,
                                      const uint64_t depth,
                                      const Statistic::StatisticMetricHash metricHash,
                                      const Windowing::WindowTypePtr windowType,
                                      const Statistic::SendingPolicyPtr sendingPolicy) {
    return create(getNextOperatorId(), statisticId, inputSchema, outputSchema, nameOfFieldToTrack,
                  width, depth, metricHash, windowType, sendingPolicy);
}

PhysicalCountMinBuildOperator::PhysicalCountMinBuildOperator(
    const OperatorId id,
    const StatisticId statisticId,
    const SchemaPtr& inputSchema,
    const SchemaPtr& outputSchema,
    const std::string& nameOfFieldToTrack,
    const uint64_t width,
    const uint64_t depth,
    const Statistic::StatisticMetricHash metricHash,
    const Windowing::WindowTypePtr windowType,
    const Statistic::SendingPolicyPtr sendingPolicy)
    : Operator(id, statisticId),
      PhysicalSynopsisBuildOperator(nameOfFieldToTrack, metricHash, windowType, sendingPolicy),
      PhysicalUnaryOperator(id, statisticId, inputSchema, outputSchema),
      width(width), depth(depth) {}

OperatorPtr PhysicalCountMinBuildOperator::copy() {
    auto copy = create(id, statisticId, inputSchema, outputSchema, nameOfFieldToTrack, width, depth, metricHash, windowType, sendingPolicy);
    copy->as<PhysicalCountMinBuildOperator>()->setInputOriginIds(inputOriginIds);
    return copy;
}


uint64_t PhysicalCountMinBuildOperator::getWidth() const { return width; }

uint64_t PhysicalCountMinBuildOperator::getDepth() const { return depth; }


}// namespace NES::QueryCompilation::PhysicalOperators