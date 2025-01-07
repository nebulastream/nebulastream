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
#include <sstream>
#include <utility>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSelectionOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

PhysicalSelectionOperator::PhysicalSelectionOperator(
    OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema, NodeFunctionPtr predicate)
    : Operator(id), PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema)), predicate(std::move(predicate))
{
}

PhysicalOperatorPtr PhysicalSelectionOperator::create(
    OperatorId id, const SchemaPtr& inputSchema, const SchemaPtr& outputSchema, const NodeFunctionPtr& function)
{
    return std::make_shared<PhysicalSelectionOperator>(id, inputSchema, outputSchema, function);
}

NodeFunctionPtr PhysicalSelectionOperator::getPredicate()
{
    return predicate;
}

PhysicalOperatorPtr PhysicalSelectionOperator::create(SchemaPtr inputSchema, SchemaPtr outputSchema, NodeFunctionPtr function)
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
