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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalProjectOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

PhysicalProjectOperator::PhysicalProjectOperator(
    OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema, std::vector<NodeFunctionPtr> functions)
    : Operator(id), PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema)), functions(std::move(functions))
{
}

PhysicalOperatorPtr PhysicalProjectOperator::create(
    OperatorId id, const SchemaPtr& inputSchema, const SchemaPtr& outputSchema, const std::vector<NodeFunctionPtr>& functions)
{
    return std::make_shared<PhysicalProjectOperator>(id, inputSchema, outputSchema, functions);
}

PhysicalOperatorPtr PhysicalProjectOperator::create(SchemaPtr inputSchema, SchemaPtr outputSchema, std::vector<NodeFunctionPtr> functions)
{
    return create(getNextOperatorId(), std::move(inputSchema), std::move(outputSchema), std::move(functions));
}

std::vector<NodeFunctionPtr> PhysicalProjectOperator::getFunctions()
{
    return functions;
}

std::string PhysicalProjectOperator::toString() const
{
    std::stringstream out;
    out << std::endl;
    out << "PhysicalProjectOperator:\n";
    out << PhysicalUnaryOperator::toString();
    return out.str();
}

std::shared_ptr<Operator> PhysicalProjectOperator::copy()
{
    auto result = create(id, inputSchema, outputSchema, getFunctions());
    result->addAllProperties(properties);
    return result;
}

}
