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
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinSinkOperator.hpp>
#include <sstream>
#include <utility>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalOperatorPtr PhysicalJoinSinkOperator::create(StatisticId statisticId,
                                                     const SchemaPtr& leftInputSchema,
                                                     const SchemaPtr& rightInputSchema,
                                                     const SchemaPtr& outputSchema,
                                                     const Join::JoinOperatorHandlerPtr& joinOperatorHandler) {
    return create(getNextOperatorId(), statisticId, leftInputSchema, rightInputSchema, outputSchema, joinOperatorHandler);
}

PhysicalOperatorPtr PhysicalJoinSinkOperator::create(OperatorId id,
                                                     StatisticId statisticId,
                                                     const SchemaPtr& leftInputSchema,
                                                     const SchemaPtr& rightInputSchema,
                                                     const SchemaPtr& outputSchema,
                                                     const Join::JoinOperatorHandlerPtr& joinOperatorHandler) {
    return std::make_shared<PhysicalJoinSinkOperator>(id, statisticId, leftInputSchema, rightInputSchema, outputSchema, joinOperatorHandler);
}

PhysicalJoinSinkOperator::PhysicalJoinSinkOperator(OperatorId id,
                                                   StatisticId statisticId,
                                                   SchemaPtr leftInputSchema,
                                                   SchemaPtr rightInputSchema,
                                                   SchemaPtr outputSchema,
                                                   Join::JoinOperatorHandlerPtr joinOperatorHandler)
    : Operator(id), PhysicalJoinOperator(std::move(joinOperatorHandler)),
      PhysicalBinaryOperator(id, statisticId, std::move(leftInputSchema), std::move(rightInputSchema), std::move(outputSchema)){};

std::string PhysicalJoinSinkOperator::toString() const {
    std::stringstream out;
    out << std::endl;
    out << "PhysicalJoinSinkOperator:\n";
    out << PhysicalBinaryOperator::toString();
    return out.str();
}

OperatorPtr PhysicalJoinSinkOperator::copy() {
    auto result = create(
        id,
        statisticId,
        leftInputSchema,
        rightInputSchema,
        outputSchema,
        operatorHandler);// todo is this a valid copy? looks like we could loose the schemas and handlers at the move operator
    result->addAllProperties(properties);
    return result;
}

}// namespace NES::QueryCompilation::PhysicalOperators