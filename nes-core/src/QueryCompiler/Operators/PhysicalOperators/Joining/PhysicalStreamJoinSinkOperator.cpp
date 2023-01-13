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
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalStreamJoinSinkOperator.hpp>
#include <utility>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalStreamJoinSinkOperator::PhysicalStreamJoinSinkOperator(OperatorId id,
                                                               SchemaPtr leftSchema,
                                                               SchemaPtr rightSchema,
                                                               SchemaPtr outputSchema,
                                                               Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr operatorHandler)
    : OperatorNode(id), PhysicalStreamJoinOperator(std::move(operatorHandler)),
      PhysicalBinaryOperator(id, std::move(leftSchema), std::move(rightSchema), std::move(outputSchema)) {}


std::string PhysicalStreamJoinSinkOperator::toString() const { return "PhysicalStreamJoinSinkOperator"; }

OperatorNodePtr PhysicalStreamJoinSinkOperator::copy() {
    return create(id, leftInputSchema, rightInputSchema, outputSchema, operatorHandler);
}

PhysicalOperatorPtr PhysicalStreamJoinSinkOperator::create(const SchemaPtr& leftSchema,
                                                           const SchemaPtr& rightSchema,
                                                           const SchemaPtr& outputSchema,
                                                           const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& operatorHandler) {
    return create(Util::getNextOperatorId(), leftSchema, rightSchema, outputSchema, operatorHandler);
}

PhysicalOperatorPtr PhysicalStreamJoinSinkOperator::create(OperatorId id,
                                       const SchemaPtr& leftSchema,
                                       const SchemaPtr& rightSchema,
                                       const SchemaPtr& outputSchema,
                                       const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& operatorHandler) {
    return std::make_shared<PhysicalStreamJoinSinkOperator>(id, leftSchema, rightSchema, outputSchema, operatorHandler);
}
}// namespace NES::QueryCompilation::PhysicalOperators