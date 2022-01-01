/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinSinkOperator.hpp>
#include <utility>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalOperatorPtr PhysicalJoinSinkOperator::create(SchemaPtr leftInputSchema,
                                                     SchemaPtr rightInputSchema,
                                                     SchemaPtr outputSchema,
                                                     Join::JoinOperatorHandlerPtr joinOperatorHandler) {
    return create(Util::getNextOperatorId(),
                  std::move(leftInputSchema),
                  std::move(rightInputSchema),
                  std::move(outputSchema),
                  std::move(joinOperatorHandler));
}

PhysicalOperatorPtr PhysicalJoinSinkOperator::create(OperatorId id,
                                                     const SchemaPtr& leftInputSchema,
                                                     const SchemaPtr& rightInputSchema,
                                                     const SchemaPtr& outputSchema,
                                                     const Join::JoinOperatorHandlerPtr& joinOperatorHandler) {
    return std::make_shared<PhysicalJoinSinkOperator>(id, leftInputSchema, rightInputSchema, outputSchema, joinOperatorHandler);
}

PhysicalJoinSinkOperator::PhysicalJoinSinkOperator(OperatorId id,
                                                   SchemaPtr leftInputSchema,
                                                   SchemaPtr rightInputSchema,
                                                   SchemaPtr outputSchema,
                                                   Join::JoinOperatorHandlerPtr joinOperatorHandler)
    : OperatorNode(id), PhysicalJoinOperator(std::move(joinOperatorHandler)),
      PhysicalBinaryOperator(id, std::move(leftInputSchema), std::move(rightInputSchema), std::move(outputSchema)){};

std::string PhysicalJoinSinkOperator::toString() const { return "PhysicalJoinSinkOperator"; }

OperatorNodePtr PhysicalJoinSinkOperator::copy() {
    return create(id, leftInputSchema, rightInputSchema, outputSchema, operatorHandler);
}

}// namespace NES::QueryCompilation::PhysicalOperators