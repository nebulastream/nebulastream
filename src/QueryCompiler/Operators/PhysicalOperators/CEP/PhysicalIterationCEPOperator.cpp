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
#include <QueryCompiler/Operators/CEP/PhysicalIterationCEPOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalIterationCEPOperator::PhysicalIterationCEPOperator(OperatorId id,
                                               SchemaPtr inputSchema,
                                               SchemaPtr outputSchema)
    : OperatorNode(id), PhysicalUnaryOperator(id, inputSchema, outputSchema), predicate(predicate) {}

PhysicalOperatorPtr
PhysicalFilterOperator::create(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema, ExpressionNodePtr expression) {
    return std::make_shared<PhysicalFilterOperator>(id, inputSchema, outputSchema, expression);
}

ExpressionNodePtr PhysicalFilterOperator::getPredicate() { return predicate; }

PhysicalOperatorPtr PhysicalFilterOperator::create(SchemaPtr inputSchema, SchemaPtr outputSchema, ExpressionNodePtr expression) {
    return create(UtilityFunctions::getNextOperatorId(), inputSchema, outputSchema, expression);
}

const std::string PhysicalFilterOperator::toString() const { return "PhysicalFilterOperator"; }

OperatorNodePtr PhysicalFilterOperator::copy() { return create(id, inputSchema, outputSchema, getPredicate()); }

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES