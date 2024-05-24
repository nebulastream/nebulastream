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

#include <QueryCompiler/Operators/PhysicalOperators/StatisticCollection/PhysicalEquiWidthBuildOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators {


PhysicalOperatorPtr
PhysicalEquiWidthBuildOperator::create(const OperatorId operatorId,
                                      const StatisticId statisticId,
                                      const SchemaPtr& inputSchema,
                                      const SchemaPtr& outputSchema,
                                      const std::string& nameOfFieldToTrack,
                                      const uint64_t binWidth,
                                      const Statistic::StatisticMetricHash metricHash,
                                      const Windowing::WindowTypePtr windowType,
                                      const Statistic::SendingPolicyPtr sendingPolicy) {
    return std::make_shared<PhysicalEquiWidthBuildOperator>(PhysicalEquiWidthBuildOperator(operatorId, statisticId, inputSchema, outputSchema, nameOfFieldToTrack, binWidth, metricHash, windowType, sendingPolicy));

}
PhysicalOperatorPtr
PhysicalEquiWidthBuildOperator::create(const StatisticId statisticId,
                                      const SchemaPtr& inputSchema,
                                      const SchemaPtr& outputSchema,
                                      const std::string& nameOfFieldToTrack,
                                      const uint64_t binWidth,
                                      const Statistic::StatisticMetricHash metricHash,
                                      const Windowing::WindowTypePtr windowType,
                                      const Statistic::SendingPolicyPtr sendingPolicy) {
    return create(getNextOperatorId(), statisticId, inputSchema, outputSchema, nameOfFieldToTrack, binWidth, metricHash, windowType, sendingPolicy);
}

PhysicalEquiWidthBuildOperator::PhysicalEquiWidthBuildOperator(
    const OperatorId id,
    const StatisticId statisticId,
    const SchemaPtr& inputSchema,
    const SchemaPtr& outputSchema,
    const std::string& nameOfFieldToTrack,
    const uint64_t binWidth,
    const Statistic::StatisticMetricHash metricHash,
    const Windowing::WindowTypePtr windowType,
    const Statistic::SendingPolicyPtr sendingPolicy)
    : Operator(id, statisticId),
      PhysicalSynopsisBuildOperator(nameOfFieldToTrack, metricHash, windowType, sendingPolicy),
      PhysicalUnaryOperator(id, statisticId, inputSchema, outputSchema),
      binWidth(binWidth){}

OperatorPtr PhysicalEquiWidthBuildOperator::copy() {
    auto copy= std::make_shared<PhysicalEquiWidthBuildOperator>(PhysicalEquiWidthBuildOperator(id, statisticId, inputSchema, outputSchema, nameOfFieldToTrack, binWidth, metricHash, windowType, sendingPolicy));
    copy->as<PhysicalEquiWidthBuildOperator>()->setInputOriginIds(inputOriginIds);
    return copy;
}
uint64_t PhysicalEquiWidthBuildOperator::getWidth() const { return binWidth; }
std::string PhysicalEquiWidthBuildOperator::toString() const { return "PhysicalEquiWidthBuildOperator"; }

}// namespace NES::QueryCompilation::PhysicalOperators