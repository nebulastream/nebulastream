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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>
#include <utility>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalMapOperator::PhysicalMapOperator(OperatorId id,
                                         SchemaPtr inputSchema,
                                         SchemaPtr outputSchema,
                                         FieldAssignmentExpressionNodePtr mapExpression)
    : OperatorNode(id), PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema)),
      mapExpression(std::move(mapExpression)) {}

FieldAssignmentExpressionNodePtr PhysicalMapOperator::getMapExpression() { return mapExpression; }

PhysicalOperatorPtr PhysicalMapOperator::create(OperatorId id,
                                                const SchemaPtr& inputSchema,
                                                const SchemaPtr& outputSchema,
                                                const FieldAssignmentExpressionNodePtr& mapExpression) {
    return std::make_shared<PhysicalMapOperator>(id, inputSchema, outputSchema, mapExpression);
}

PhysicalOperatorPtr
PhysicalMapOperator::create(SchemaPtr inputSchema, SchemaPtr outputSchema, FieldAssignmentExpressionNodePtr mapExpression) {
    return create(Util::getNextOperatorId(),
                  std::move(inputSchema),
                  std::move(outputSchema),
                  std::move(mapExpression));
}

std::string PhysicalMapOperator::toString() const { return "PhysicalMapOperator"; }

OperatorNodePtr PhysicalMapOperator::copy() { return create(id, inputSchema, outputSchema, getMapExpression()); }

}// namespace NES::QueryCompilation::PhysicalOperators