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
#include <DataTypes/Schema.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Identifiers/Identifiers.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

PhysicalMapOperator::PhysicalMapOperator(
    OperatorId id, Schema inputSchema, Schema outputSchema, std::shared_ptr<NodeFunctionFieldAssignment> mapFunction)
    : Operator(id), PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema)), mapFunction(std::move(mapFunction))
{
}

std::shared_ptr<NodeFunctionFieldAssignment> PhysicalMapOperator::getMapFunction()
{
    return mapFunction;
}

std::shared_ptr<PhysicalOperator> PhysicalMapOperator::create(
    OperatorId id, Schema inputSchema, Schema outputSchema, const std::shared_ptr<NodeFunctionFieldAssignment>& mapFunction)
{
    return std::make_shared<PhysicalMapOperator>(id, inputSchema, outputSchema, mapFunction);
}

std::shared_ptr<PhysicalOperator>
PhysicalMapOperator::create(Schema inputSchema, Schema outputSchema, const std::shared_ptr<NodeFunctionFieldAssignment>& mapFunction)
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
