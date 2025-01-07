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
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

PhysicalMapOperator::PhysicalMapOperator(
    OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema, NodeFunctionFieldAssignmentPtr mapFunction)
    : Operator(id), PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema)), mapFunction(std::move(mapFunction))
{
}

NodeFunctionFieldAssignmentPtr PhysicalMapOperator::getMapFunction()
{
    return mapFunction;
}

PhysicalOperatorPtr PhysicalMapOperator::create(
    OperatorId id, const SchemaPtr& inputSchema, const SchemaPtr& outputSchema, const NodeFunctionFieldAssignmentPtr& mapFunction)
{
    return std::make_shared<PhysicalMapOperator>(id, inputSchema, outputSchema, mapFunction);
}

PhysicalOperatorPtr PhysicalMapOperator::create(SchemaPtr inputSchema, SchemaPtr outputSchema, NodeFunctionFieldAssignmentPtr mapFunction)
{
    return create(getNextOperatorId(), std::move(inputSchema), std::move(outputSchema), std::move(mapFunction));
}

std::string PhysicalMapOperator::toString() const
{
    std::stringstream out;
    out << std::endl;
    out << "PhysicalMapOperator:\n";
    out << PhysicalUnaryOperator::toString();
    if (mapFunction != nullptr)
    {
        out << "mapFunction: " << *mapFunction;
    }
    out << std::endl;
    return out.str();
}

std::shared_ptr<Operator> PhysicalMapOperator::copy()
{
    auto result = create(id, inputSchema, outputSchema, getMapFunction());
    result->addAllProperties(properties);
    return result;
}

}
