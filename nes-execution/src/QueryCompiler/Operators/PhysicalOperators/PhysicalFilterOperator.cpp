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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalFilterOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

PhysicalFilterOperator::PhysicalFilterOperator(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema, FunctionNodePtr predicate)
    : Operator(id), PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema)), predicate(std::move(predicate))
{
}

PhysicalOperatorPtr
PhysicalFilterOperator::create(OperatorId id, const SchemaPtr& inputSchema, const SchemaPtr& outputSchema, const FunctionNodePtr& function)
{
    return std::make_shared<PhysicalFilterOperator>(id, inputSchema, outputSchema, function);
}

FunctionNodePtr PhysicalFilterOperator::getPredicate()
{
    return predicate;
}

PhysicalOperatorPtr PhysicalFilterOperator::create(SchemaPtr inputSchema, SchemaPtr outputSchema, FunctionNodePtr function)
{
    return create(getNextOperatorId(), std::move(inputSchema), std::move(outputSchema), std::move(function));
}

std::string PhysicalFilterOperator::toString() const
{
    std::stringstream out;
    out << std::endl;
    out << "PhysicalFilterOperator:\n";
    out << PhysicalUnaryOperator::toString();
    return out.str();
}

OperatorPtr PhysicalFilterOperator::copy()
{
    auto result = create(id, inputSchema, outputSchema, getPredicate());
    result->addAllProperties(properties);
    return result;
}

} /// namespace NES::QueryCompilation::PhysicalOperators
