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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMeosOperator.hpp>
#include <sstream>
#include <utility>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalMeosOperator::PhysicalMeosOperator(OperatorId id,
                                           StatisticId statisticId,
                                           SchemaPtr leftSchema,
                                           SchemaPtr middleSchema,
                                           SchemaPtr rightSchema,
                                           SchemaPtr outputSchema)
    : Operator(id, statisticId), PhysicalOperator(id, statisticId), TernaryOperator(id) {
    TernaryOperator::setLeftInputSchema(std::move(leftSchema));
    TernaryOperator::setMiddleInputSchema(std::move(middleSchema));
    TernaryOperator::setRightInputSchema(std::move(rightSchema));
    TernaryOperator::setOutputSchema(std::move(outputSchema));
}

std::string PhysicalMeosOperator::toString() const {
    std::stringstream out;
    out << TernaryOperator::toString();
    return out.str();
}

const std::string& PhysicalMeosOperator::getFunction() const { return function; }
const std::vector<ExpressionNodePtr>& PhysicalMeosOperator::getInputFields() const { return inputFields; }
const std::vector<ExpressionNodePtr>& PhysicalMeosOperator::getOutputFields() const { return outputFields; }

}// namespace NES::QueryCompilation::PhysicalOperators
