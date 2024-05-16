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

#include <QueryCompiler/Operators/PhysicalOperators/StatisticCollection/PhysicalDDSketchBuildOperator.hpp>
#include <fmt/format.h>
namespace NES::QueryCompilation::PhysicalOperators {

PhysicalOperatorPtr PhysicalDDSketchBuildOperator::create(const OperatorId id,
                                                          const StatisticId statisticId,
                                                          const SchemaPtr& inputSchema,
                                                          const SchemaPtr& outputSchema,
                                                          const std::string& nameOfFieldToTrack,
                                                          const uint64_t numberOfPreAllocatedBuckets,
                                                          const double gamma,
                                                          ExpressionNodePtr calcLogFloorIndexExpressions,
                                                          ExpressionNodePtr greaterThanZeroExpression,
                                                          ExpressionNodePtr lessThanZeroExpression,
                                                          const Statistic::StatisticMetricHash metricHash,
                                                          const Windowing::WindowTypePtr windowType,
                                                          const Statistic::SendingPolicyPtr sendingPolicy) {
    return std::make_shared<PhysicalDDSketchBuildOperator>(PhysicalDDSketchBuildOperator(id, statisticId, inputSchema, outputSchema, nameOfFieldToTrack, numberOfPreAllocatedBuckets, gamma, calcLogFloorIndexExpressions, greaterThanZeroExpression, lessThanZeroExpression, metricHash, windowType, sendingPolicy));
}

PhysicalOperatorPtr PhysicalDDSketchBuildOperator::create(const StatisticId statisticId,
                                                          const SchemaPtr& inputSchema,
                                                          const SchemaPtr& outputSchema,
                                                          const std::string& nameOfFieldToTrack,
                                                          const uint64_t numberOfPreAllocatedBuckets,
                                                          const double gamma,
                                                          ExpressionNodePtr calcLogFloorIndexExpressions,
                                                          ExpressionNodePtr greaterThanZeroExpression,
                                                          ExpressionNodePtr lessThanZeroExpression,
                                                          const Statistic::StatisticMetricHash metricHash,
                                                          const Windowing::WindowTypePtr windowType,
                                                          const Statistic::SendingPolicyPtr sendingPolicy) {
    return create(getNextOperatorId(), statisticId, inputSchema, outputSchema, nameOfFieldToTrack, numberOfPreAllocatedBuckets, gamma, calcLogFloorIndexExpressions, greaterThanZeroExpression, lessThanZeroExpression, metricHash, windowType, sendingPolicy);
}

OperatorPtr PhysicalDDSketchBuildOperator::copy() {
    auto copy = create(id, statisticId, inputSchema, outputSchema, nameOfFieldToTrack, numberOfPreAllocatedBuckets, gamma, calcLogFloorIndexExpressions, greaterThanZeroExpression, lessThanZeroExpression, metricHash, windowType, sendingPolicy);
    copy->as<PhysicalDDSketchBuildOperator>()->setInputOriginIds(inputOriginIds);
    return copy;
}

uint64_t PhysicalDDSketchBuildOperator::getNumberOfPreAllocatedBuckets() const { return numberOfPreAllocatedBuckets; }

double PhysicalDDSketchBuildOperator::getGamma() const { return gamma; }
ExpressionNodePtr PhysicalDDSketchBuildOperator::getCalcLogFloorIndexExpressions() const { return calcLogFloorIndexExpressions; }
ExpressionNodePtr PhysicalDDSketchBuildOperator::getGreaterThanZeroExpression() const { return greaterThanZeroExpression; }
ExpressionNodePtr PhysicalDDSketchBuildOperator::getLessThanZeroExpression() const { return lessThanZeroExpression; }

std::string PhysicalDDSketchBuildOperator::toString() const {
    return fmt::format("PhysicalDDSketchBuildOperator:\n"
                       "{}"
                       "gamma: {}, "
                       "numberOfPreAllocatedBuckets: {}, "
                       "sendingPolicy: {}, "
                       "metricHash: {}\n",
                       PhysicalUnaryOperator::toString(),
                       gamma, numberOfPreAllocatedBuckets, sendingPolicy->toString(), metricHash);
}

PhysicalDDSketchBuildOperator::PhysicalDDSketchBuildOperator(const OperatorId id,
                                                             const StatisticId statisticId,
                                                             const SchemaPtr& inputSchema,
                                                             const SchemaPtr& outputSchema,
                                                             const std::string& nameOfFieldToTrack,
                                                             const uint64_t numberOfPreAllocatedBuckets,
                                                             const double gamma,
                                                             ExpressionNodePtr calcLogFloorIndexExpressions,
                                                             ExpressionNodePtr greaterThanZeroExpression,
                                                             ExpressionNodePtr lessThanZeroExpression,
                                                             const Statistic::StatisticMetricHash metricHash,
                                                             const Windowing::WindowTypePtr windowType,
                                                             const Statistic::SendingPolicyPtr sendingPolicy)
    : Operator(id, statisticId),
      PhysicalSynopsisBuildOperator(nameOfFieldToTrack, metricHash, windowType, sendingPolicy),
      PhysicalUnaryOperator(id, statisticId, inputSchema, outputSchema),
      numberOfPreAllocatedBuckets(numberOfPreAllocatedBuckets), gamma(gamma), calcLogFloorIndexExpressions(calcLogFloorIndexExpressions),
      greaterThanZeroExpression(greaterThanZeroExpression), lessThanZeroExpression(lessThanZeroExpression) {}

}  // namespace NES::QueryCompilation::PhysicalOperators