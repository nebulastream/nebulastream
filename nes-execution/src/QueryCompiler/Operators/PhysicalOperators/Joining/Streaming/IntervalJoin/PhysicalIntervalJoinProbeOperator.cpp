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

#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/IntervalJoin/PhysicalIntervalJoinProbeOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalOperatorPtr
PhysicalIntervalJoinProbeOperator::create(OperatorId id,
                                          StatisticId statisticId,
                                          const SchemaPtr& leftSchema,
                                          const SchemaPtr& rightSchema,
                                          const SchemaPtr& outputSchema,
                                          ExpressionNodePtr joinExpression,
                                          const std::string& intervalStartField,
                                          const std::string& intervalEndField,
                                          TimestampField timestampField,
                                          const Runtime::Execution::Operators::IJOperatorHandlerPtr& operatorHandler) {
    return std::make_shared<PhysicalIntervalJoinProbeOperator>(id,
                                                               statisticId,
                                                               leftSchema,
                                                               rightSchema,
                                                               outputSchema,
                                                               joinExpression,
                                                               intervalStartField,
                                                               intervalEndField,
                                                               timestampField,
                                                               operatorHandler);
}

PhysicalOperatorPtr
PhysicalIntervalJoinProbeOperator::create(StatisticId statisticId,
                                          const SchemaPtr& leftSchema,
                                          const SchemaPtr& rightSchema,
                                          const SchemaPtr& outputSchema,
                                          ExpressionNodePtr joinExpression,
                                          const std::string& intervalStartField,
                                          const std::string& intervalEndField,
                                          TimestampField timestampField,
                                          const Runtime::Execution::Operators::IJOperatorHandlerPtr& operatorHandler) {
    return create(getNextOperatorId(),
                  statisticId,
                  leftSchema,
                  rightSchema,
                  outputSchema,
                  joinExpression,
                  intervalStartField,
                  intervalEndField,
                  timestampField,
                  operatorHandler);
}

PhysicalIntervalJoinProbeOperator::PhysicalIntervalJoinProbeOperator(
    OperatorId id,
    StatisticId statisticId,
    const SchemaPtr& leftSchema,
    const SchemaPtr& rightSchema,
    const SchemaPtr& outputSchema,
    ExpressionNodePtr joinExpression,
    const std::string& intervalStartField,
    const std::string& intervalEndField,
    TimestampField timestampField,
    const Runtime::Execution::Operators::IJOperatorHandlerPtr& operatorHandler)
    : Operator(id), PhysicalBinaryOperator(id, statisticId, leftSchema, rightSchema, outputSchema),
      joinExpression(joinExpression), intervalStartField(intervalStartField), intervalEndField(intervalEndField),
      timestampField(timestampField), operatorHandler(operatorHandler) {}

std::string PhysicalIntervalJoinProbeOperator::toString() const {
    std::stringstream out;
    out << std::endl;
    out << "PhysicalIntervalJoinProbeOperator:\n";
    out << PhysicalBinaryOperator::toString();
    out << std::endl;
    return out.str();
}

OperatorPtr PhysicalIntervalJoinProbeOperator::copy() {
    return create(id,
                  statisticId,
                  leftInputSchema,
                  rightInputSchema,
                  outputSchema,
                  joinExpression,
                  intervalStartField,
                  intervalEndField,
                  timestampField,
                  operatorHandler);
}

ExpressionNodePtr PhysicalIntervalJoinProbeOperator::getJoinExpression() const { return joinExpression; }
const std::string& PhysicalIntervalJoinProbeOperator::getIntervalStartField() const { return intervalStartField; }
const std::string& PhysicalIntervalJoinProbeOperator::getIntervalEndField() const { return intervalEndField; }
TimestampField PhysicalIntervalJoinProbeOperator::getTimeStampField() const { return timestampField; }
const Runtime::Execution::Operators::IJOperatorHandlerPtr& PhysicalIntervalJoinProbeOperator::getIJOperatorHandler() const {
    return operatorHandler;
}

}// namespace NES::QueryCompilation::PhysicalOperators
