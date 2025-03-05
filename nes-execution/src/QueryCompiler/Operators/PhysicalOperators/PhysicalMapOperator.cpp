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
#include <utility>
#include <API/Schema.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Identifiers/Identifiers.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <Util/Common.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

PhysicalMapOperator::PhysicalMapOperator(
    OperatorId id,
    std::shared_ptr<Schema> inputSchema,
    std::shared_ptr<Schema> outputSchema,
    std::shared_ptr<NodeFunctionFieldAssignment> mapFunction)
    : Operator(id), PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema)), mapFunction(std::move(mapFunction))
{
}

std::shared_ptr<NodeFunctionFieldAssignment> PhysicalMapOperator::getMapFunction()
{
    return mapFunction;
}

std::shared_ptr<PhysicalOperator> PhysicalMapOperator::create(
    OperatorId id,
    const std::shared_ptr<Schema>& inputSchema,
    const std::shared_ptr<Schema>& outputSchema,
    const std::shared_ptr<NodeFunctionFieldAssignment>& mapFunction)
{
    return std::make_shared<PhysicalMapOperator>(id, inputSchema, outputSchema, mapFunction);
}

std::shared_ptr<PhysicalOperator> PhysicalMapOperator::create(
    const std::shared_ptr<Schema>& inputSchema,
    const std::shared_ptr<Schema>& outputSchema,
    const std::shared_ptr<NodeFunctionFieldAssignment>& mapFunction)
{
    return create(getNextOperatorId(), std::move(inputSchema), std::move(outputSchema), std::move(mapFunction));
}

std::ostream& PhysicalMapOperator::toDebugString(std::ostream& os) const
{
    os << "\nPhysicalMapOperator:\n";
    os << LogVerbosity::VerbosityLevel::Debug;
    if (mapFunction != nullptr)
    {
        os << "mapFunction: " << *mapFunction;
    }
    os << '\n';
    return PhysicalUnaryOperator::toDebugString(os);
    ;
}

std::ostream& PhysicalMapOperator::toQueryPlanString(std::ostream& os) const
{
    os << "PhysicalMapOperator(";
    os << LogVerbosity::VerbosityLevel::QueryPlan;
    if (mapFunction != nullptr)
    {
        os << *mapFunction;
    }
    os << ')';
    return PhysicalUnaryOperator::toQueryPlanString(os);
    ;
}

std::shared_ptr<Operator> PhysicalMapOperator::copy()
{
    auto result = create(id, inputSchema, outputSchema, getMapFunction());
    result->addAllProperties(properties);
    return result;
}

}
