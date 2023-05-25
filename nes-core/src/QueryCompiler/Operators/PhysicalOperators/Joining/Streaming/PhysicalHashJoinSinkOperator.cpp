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
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalHashJoinSinkOperator.hpp>
#include <utility>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalHashJoinSinkOperator::PhysicalHashJoinSinkOperator(
    OperatorId id,
    SchemaPtr leftSchema,
    SchemaPtr rightSchema,
    SchemaPtr outputSchema,
    Runtime::Execution::Operators::StreamHashJoinOperatorHandlerPtr operatorHandler)
    : OperatorNode(id), PhysicalHashJoinOperator(std::move(operatorHandler), id),
      PhysicalBinaryOperator(id, std::move(leftSchema), std::move(rightSchema), std::move(outputSchema)) {}

std::string PhysicalHashJoinSinkOperator::toString() const { return "PhysicalHashJoinSinkOperator"; }

OperatorNodePtr PhysicalHashJoinSinkOperator::copy() {
    return create(id, leftInputSchema, rightInputSchema, outputSchema, operatorHandler);
}

PhysicalOperatorPtr
PhysicalHashJoinSinkOperator::create(const SchemaPtr& leftSchema,
                                     const SchemaPtr& rightSchema,
                                     const SchemaPtr& outputSchema,
                                     const Runtime::Execution::Operators::StreamHashJoinOperatorHandlerPtr& operatorHandler) {
    return create(Util::getNextOperatorId(), leftSchema, rightSchema, outputSchema, operatorHandler);
}

PhysicalOperatorPtr
PhysicalHashJoinSinkOperator::create(OperatorId id,
                                     const SchemaPtr& leftSchema,
                                     const SchemaPtr& rightSchema,
                                     const SchemaPtr& outputSchema,
                                     const Runtime::Execution::Operators::StreamHashJoinOperatorHandlerPtr& operatorHandler) {
    return std::make_shared<PhysicalHashJoinSinkOperator>(id, leftSchema, rightSchema, outputSchema, operatorHandler);
}
}// namespace NES::QueryCompilation::PhysicalOperators