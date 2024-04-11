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
#include <Operators/Expressions/FieldAssignmentExpressionNode.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>
#include <sstream>
#include <utility>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalMapOperator::PhysicalMapOperator(OperatorId id,
                                         StatisticId statisticId,
                                         SchemaPtr inputSchema,
                                         SchemaPtr outputSchema,
                                         FieldAssignmentExpressionNodePtr mapExpression)
    : Operator(id, statisticId), PhysicalUnaryOperator(id, statisticId, std::move(inputSchema), std::move(outputSchema)),
      mapExpression(std::move(mapExpression)) {}

FieldAssignmentExpressionNodePtr PhysicalMapOperator::getMapExpression() { return mapExpression; }

PhysicalOperatorPtr PhysicalMapOperator::create(OperatorId id,
                                                StatisticId statisticId,
                                                const SchemaPtr& inputSchema,
                                                const SchemaPtr& outputSchema,
                                                const FieldAssignmentExpressionNodePtr& mapExpression) {
    return std::make_shared<PhysicalMapOperator>(id, statisticId, inputSchema, outputSchema, mapExpression);
}

PhysicalOperatorPtr
PhysicalMapOperator::create(StatisticId statisticId, SchemaPtr inputSchema, SchemaPtr outputSchema, FieldAssignmentExpressionNodePtr mapExpression) {
    return create(getNextOperatorId(), statisticId, std::move(inputSchema), std::move(outputSchema), std::move(mapExpression));
}

std::string PhysicalMapOperator::toString() const {
    std::stringstream out;
    out << std::endl;
    out << "PhysicalMapOperator:\n";
    out << PhysicalUnaryOperator::toString();
    if (mapExpression != nullptr) {
        out << "mapExpression: " << mapExpression->toString();
    }
    out << std::endl;
    return out.str();
}

OperatorPtr PhysicalMapOperator::copy() {
    auto result = create(id, statisticId, inputSchema, outputSchema, getMapExpression());
    result->addAllProperties(properties);
    return result;
}

}// namespace NES::QueryCompilation::PhysicalOperators