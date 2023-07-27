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
#include <Execution/Operators/Streaming/Join/StreamHashJoin/StreamHashJoinOperatorHandler.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalHashJoinProbeOperator.hpp>
#include <utility>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalHashJoinProbeOperator::PhysicalHashJoinProbeOperator(
    OperatorId id,
    const SchemaPtr& leftSchema,
    const SchemaPtr& rightSchema,
    const SchemaPtr& outputSchema,
    const std::string& joinFieldNameLeft,
    const std::string& joinFieldNameRight,
    const Runtime::Execution::Operators::StreamHashJoinOperatorHandlerPtr& operatorHandler)
    : OperatorNode(id), PhysicalHashJoinOperator(operatorHandler),
      PhysicalBinaryOperator(id, leftSchema, rightSchema, outputSchema), joinFieldNameLeft(joinFieldNameLeft),
      joinFieldNameRight(joinFieldNameRight) {}

std::string PhysicalHashJoinProbeOperator::toString() const { return "PhysicalHashJoinProbeOperator"; }

OperatorNodePtr PhysicalHashJoinProbeOperator::copy() {
    return create(id, leftInputSchema, rightInputSchema, outputSchema, joinFieldNameLeft, joinFieldNameRight, operatorHandler);
}

PhysicalOperatorPtr
PhysicalHashJoinProbeOperator::create(const SchemaPtr& leftSchema,
                                      const SchemaPtr& rightSchema,
                                      const SchemaPtr& outputSchema,
                                      const std::string& joinFieldNameLeft,
                                      const std::string& joinFieldNameRight,
                                      const Runtime::Execution::Operators::StreamHashJoinOperatorHandlerPtr& operatorHandler) {
    return create(Util::getNextOperatorId(),
                  leftSchema,
                  rightSchema,
                  outputSchema,
                  joinFieldNameLeft,
                  joinFieldNameRight,
                  operatorHandler);
}

PhysicalOperatorPtr
PhysicalHashJoinProbeOperator::create(OperatorId id,
                                      const SchemaPtr& leftSchema,
                                      const SchemaPtr& rightSchema,
                                      const SchemaPtr& outputSchema,
                                      const std::string& joinFieldNameLeft,
                                      const std::string& joinFieldNameRight,
                                      const Runtime::Execution::Operators::StreamHashJoinOperatorHandlerPtr& operatorHandler) {
    return std::make_shared<PhysicalHashJoinProbeOperator>(id,
                                                           leftSchema,
                                                           rightSchema,
                                                           outputSchema,
                                                           joinFieldNameLeft,
                                                           joinFieldNameRight,
                                                           operatorHandler);
}

const std::string& PhysicalHashJoinProbeOperator::getJoinFieldNameLeft() const { return joinFieldNameLeft; }
const std::string& PhysicalHashJoinProbeOperator::getJoinFieldNameRight() const { return joinFieldNameRight; }

}// namespace NES::QueryCompilation::PhysicalOperators