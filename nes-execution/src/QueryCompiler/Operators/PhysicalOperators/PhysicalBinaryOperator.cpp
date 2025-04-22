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
#include <memory>
#include <ostream>
#include <sstream>
#include <utility>
#include <API/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalBinaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

PhysicalBinaryOperator::PhysicalBinaryOperator(
    OperatorId id, std::shared_ptr<Schema> leftSchema, std::shared_ptr<Schema> rightSchema, std::shared_ptr<Schema> outputSchema)
    : Operator(id), PhysicalOperator(id), BinaryOperator(id)
{
    BinaryOperator::setLeftInputSchema(std::move(leftSchema));
    BinaryOperator::setRightInputSchema(std::move(rightSchema));
    BinaryOperator::setOutputSchema(std::move(outputSchema));
}

std::ostream& PhysicalBinaryOperator::toDebugString(std::ostream& os) const
{
    return BinaryOperator::toDebugString(os);
}
}
