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
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalNestedLoopJoinProbeOperator.hpp>
#include <utility>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalNestedLoopJoinProbeOperator::PhysicalNestedLoopJoinProbeOperator(
    OperatorId id,
    SchemaPtr leftSchema,
    SchemaPtr rightSchema,
    SchemaPtr outputSchema,
    const std::string& joinFieldNameLeft,
    const std::string& joinFieldNameRight,
    const std::string& windowStartFieldName,
    const std::string& windowEndFieldName,
    const std::string& windowKeyFieldName,
    Runtime::Execution::Operators::NLJOperatorHandlerPtr operatorHandler)
    : OperatorNode(id), PhysicalNestedLoopJoinOperator(std::move(operatorHandler), id),
      PhysicalBinaryOperator(id, std::move(leftSchema), std::move(rightSchema), std::move(outputSchema)),
      joinFieldNameLeft(joinFieldNameLeft), joinFieldNameRight(joinFieldNameRight),
      windowStartFieldName(windowStartFieldName), windowEndFieldName(windowEndFieldName), windowKeyFieldName(windowKeyFieldName) {}

std::string PhysicalNestedLoopJoinProbeOperator::toString() const { return "PhysicalNestedLoopJoinProbeOperator"; }

OperatorNodePtr PhysicalNestedLoopJoinProbeOperator::copy() {
    return create(id, leftInputSchema, rightInputSchema, outputSchema, joinFieldNameLeft, joinFieldNameRight, windowStartFieldName,
                  windowEndFieldName, windowKeyFieldName, operatorHandler);
}

PhysicalOperatorPtr
PhysicalNestedLoopJoinProbeOperator::create(const SchemaPtr& leftSchema,
                                            const SchemaPtr& rightSchema,
                                            const SchemaPtr& outputSchema,
                                            const std::string& joinFieldNameLeft,
                                            const std::string& joinFieldNameRight,
                                            const std::string& windowStartFieldName,
                                            const std::string& windowEndFieldName,
                                            const std::string& windowKeyFieldName,
                                            const Runtime::Execution::Operators::NLJOperatorHandlerPtr& operatorHandler) {
    return create(Util::getNextOperatorId(),
                  leftSchema,
                  rightSchema,
                  outputSchema,
                  joinFieldNameLeft,
                  joinFieldNameRight,
                  windowStartFieldName,
                  windowEndFieldName,
                  windowKeyFieldName,
                  operatorHandler);
}

PhysicalOperatorPtr
PhysicalNestedLoopJoinProbeOperator::create(const OperatorId id,
                                            const SchemaPtr& leftSchema,
                                            const SchemaPtr& rightSchema,
                                            const SchemaPtr& outputSchema,
                                            const std::string& joinFieldNameLeft,
                                            const std::string& joinFieldNameRight,
                                            const std::string& windowStartFieldName,
                                            const std::string& windowEndFieldName,
                                            const std::string& windowKeyFieldName,
                                            const Runtime::Execution::Operators::NLJOperatorHandlerPtr& operatorHandler) {
    return std::make_shared<PhysicalNestedLoopJoinProbeOperator>(id,
                                                                 leftSchema,
                                                                 rightSchema,
                                                                 outputSchema,
                                                                 joinFieldNameLeft,
                                                                 joinFieldNameRight,
                                                                 windowStartFieldName,
                                                                 windowEndFieldName,
                                                                 windowKeyFieldName,
                                                                 operatorHandler);
}

const std::string& PhysicalNestedLoopJoinProbeOperator::getJoinFieldNameLeft() const { return joinFieldNameLeft; }
const std::string& PhysicalNestedLoopJoinProbeOperator::getJoinFieldNameRight() const { return joinFieldNameRight; }
const std::string& PhysicalNestedLoopJoinProbeOperator::getWindowStartFieldName() const { return windowStartFieldName; }
const std::string& PhysicalNestedLoopJoinProbeOperator::getWindowEndFieldName() const { return windowEndFieldName; }
const std::string& PhysicalNestedLoopJoinProbeOperator::getWindowKeyFieldName() const { return windowKeyFieldName; }

}// namespace NES::QueryCompilation::PhysicalOperators