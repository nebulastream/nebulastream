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

#include <QueryCompiler/Operators/PhysicalOperators/StatisticCollection/PhysicalReservoirSampleBuildOperator.hpp>
namespace NES::QueryCompilation::PhysicalOperators {

PhysicalOperatorPtr PhysicalReservoirSampleBuildOperator::create(const OperatorId id,
                                                                 const StatisticId statisticId,
                                                                 const SchemaPtr& inputSchema,
                                                                 const SchemaPtr& outputSchema,
                                                                 const uint64_t sampleSize,
                                                                 const Statistic::StatisticMetricHash metricHash,
                                                                 const Windowing::WindowTypePtr windowType,
                                                                 const Statistic::SendingPolicyPtr sendingPolicy) {
    return std::make_shared<PhysicalReservoirSampleBuildOperator>(PhysicalReservoirSampleBuildOperator(id, statisticId, inputSchema, outputSchema, sampleSize, metricHash, windowType, sendingPolicy));
}

PhysicalOperatorPtr PhysicalReservoirSampleBuildOperator::create(const StatisticId statisticId,
                                                                 const SchemaPtr& inputSchema,
                                                                 const SchemaPtr& outputSchema,
                                                                 const uint64_t sampleSize,
                                                                 const Statistic::StatisticMetricHash metricHash,
                                                                 const Windowing::WindowTypePtr windowType,
                                                                 const Statistic::SendingPolicyPtr sendingPolicy) {
    return create(getNextOperatorId(), statisticId, inputSchema, outputSchema, sampleSize, metricHash, windowType, sendingPolicy);
}

OperatorPtr PhysicalReservoirSampleBuildOperator::copy() {
    auto copy = create(id, statisticId, inputSchema, outputSchema, sampleSize, metricHash, windowType, sendingPolicy);
    copy->as<PhysicalReservoirSampleBuildOperator>()->setInputOriginIds(inputOriginIds);
    return copy;
}

uint64_t PhysicalReservoirSampleBuildOperator::getSampleSize() const { return sampleSize; }

PhysicalReservoirSampleBuildOperator::PhysicalReservoirSampleBuildOperator(const OperatorId id,
                                                                           const StatisticId statisticId,
                                                                           const SchemaPtr& inputSchema,
                                                                           const SchemaPtr& outputSchema,
                                                                           const uint64_t sampleSize,
                                                                           const Statistic::StatisticMetricHash metricHash,
                                                                           const Windowing::WindowTypePtr windowType,
                                                                           const Statistic::SendingPolicyPtr sendingPolicy)
    : Operator(id, statisticId),
      PhysicalSynopsisBuildOperator("", metricHash, windowType, sendingPolicy),
      PhysicalUnaryOperator(id, statisticId, inputSchema, outputSchema), sampleSize(sampleSize) {}

} // namespace NES::QueryCompilation::PhysicalOperators