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
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/ContentBasedWindow/PhysicalThresholdWindowOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

PhysicalThresholdWindowOperator::PhysicalThresholdWindowOperator(
    OperatorId id, Schema inputSchema, Schema outputSchema, std::shared_ptr<Windowing::LogicalWindowDescriptor> windowDefinition)
    : Operator(id)
    , PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema))
    , windowDefinition(std::move(windowDefinition))
{
}

std::shared_ptr<PhysicalThresholdWindowOperator> PhysicalThresholdWindowOperator::create(
    Schema inputSchema, Schema outputSchema, const std::shared_ptr<Windowing::LogicalWindowDescriptor>& windowDefinition)
{
    return std::make_shared<PhysicalThresholdWindowOperator>(getNextOperatorId(), inputSchema, outputSchema, windowDefinition);
}

std::shared_ptr<Windowing::LogicalWindowDescriptor> PhysicalThresholdWindowOperator::getWindowDefinition()
{
    return windowDefinition;
}

std::ostream& PhysicalThresholdWindowOperator::toDebugString(std::ostream& os) const
{
    os << "\nPhysicalThresholdWindowOperator:\n";
    return PhysicalUnaryOperator::toDebugString(os);
}

std::ostream& PhysicalThresholdWindowOperator::toQueryPlanString(std::ostream& os) const
{
    os << "PhysicalThresholdWindowOperator:";
    return PhysicalUnaryOperator::toQueryPlanString(os);
}

std::shared_ptr<Operator> PhysicalThresholdWindowOperator::copy()
{
    return create(inputSchema, outputSchema, windowDefinition);
}

}
