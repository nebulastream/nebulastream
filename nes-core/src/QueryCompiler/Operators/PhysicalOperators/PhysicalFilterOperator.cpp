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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalFilterOperator.hpp>
#include <utility>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalFilterOperator::PhysicalFilterOperator(OperatorId id,
                                               SchemaPtr inputSchema,
                                               SchemaPtr outputSchema,
                                               ExpressionNodePtr predicate)
    : OperatorNode(id), PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema)),
      predicate(std::move(predicate)) {}

PhysicalOperatorPtr PhysicalFilterOperator::create(OperatorId id,
                                                   const SchemaPtr& inputSchema,
                                                   const SchemaPtr& outputSchema,
                                                   const ExpressionNodePtr& expression) {
    return std::make_shared<PhysicalFilterOperator>(id, inputSchema, outputSchema, expression);
}

ExpressionNodePtr PhysicalFilterOperator::getPredicate() { return predicate; }

PhysicalOperatorPtr PhysicalFilterOperator::create(SchemaPtr inputSchema, SchemaPtr outputSchema, ExpressionNodePtr expression) {
    return create(Util::getNextOperatorId(), std::move(inputSchema), std::move(outputSchema), std::move(expression));
}

std::string PhysicalFilterOperator::toString() const { return "PhysicalFilterOperator"; }

OperatorNodePtr PhysicalFilterOperator::copy() { return create(id, inputSchema, outputSchema, getPredicate()); }

}// namespace NES::QueryCompilation::PhysicalOperators