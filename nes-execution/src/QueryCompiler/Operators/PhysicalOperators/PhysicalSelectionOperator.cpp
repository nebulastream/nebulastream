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
#include <sstream>
#include <utility>
#include <API/Schema.hpp>
#include <Functions/NodeFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSelectionOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

PhysicalSelectionOperator::PhysicalSelectionOperator(
    OperatorId id, std::shared_ptr<Schema> inputSchema, std::shared_ptr<Schema> outputSchema, std::shared_ptr<NodeFunction> predicate)
    : Operator(id), PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema)), predicate(std::move(predicate))
{
}

std::shared_ptr<PhysicalOperator> PhysicalSelectionOperator::create(
    OperatorId id,
    const std::shared_ptr<Schema>& inputSchema,
    const std::shared_ptr<Schema>& outputSchema,
    const std::shared_ptr<NodeFunction>& function)
{
    return std::make_shared<PhysicalSelectionOperator>(id, inputSchema, outputSchema, function);
}

std::shared_ptr<NodeFunction> PhysicalSelectionOperator::getPredicate()
{
    return predicate;
}

std::shared_ptr<PhysicalOperator> PhysicalSelectionOperator::create(
    const std::shared_ptr<Schema>& inputSchema, const std::shared_ptr<Schema>& outputSchema, const std::shared_ptr<NodeFunction>& function)
{
    return create(getNextOperatorId(), std::move(inputSchema), std::move(outputSchema), std::move(function));
}

std::string PhysicalSelectionOperator::toString() const
{
    std::stringstream out;
    out << std::endl;
    out << "PhysicalSelectionOperator:\n";
    out << PhysicalUnaryOperator::toString();
    return out.str();
}

std::shared_ptr<Operator> PhysicalSelectionOperator::copy()
{
    auto result = create(id, inputSchema, outputSchema, getPredicate());
    result->addAllProperties(properties);
    return result;
}

}
