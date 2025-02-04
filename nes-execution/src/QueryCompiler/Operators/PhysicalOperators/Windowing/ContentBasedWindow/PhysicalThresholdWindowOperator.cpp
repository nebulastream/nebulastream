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
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/ContentBasedWindow/PhysicalThresholdWindowOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

PhysicalThresholdWindowOperator::PhysicalThresholdWindowOperator(
    OperatorId id,
    std::shared_ptr<Schema> inputSchema,
    std::shared_ptr<Schema> outputSchema,
    std::shared_ptr<Windowing::LogicalWindowDescriptor> windowDefinition)
    : Operator(id)
    , PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema))
    , windowDefinition(std::move(windowDefinition))
{
}

std::shared_ptr<PhysicalThresholdWindowOperator> PhysicalThresholdWindowOperator::create(
    const std::shared_ptr<Schema>& inputSchema,
    const std::shared_ptr<Schema>& outputSchema,
    const std::shared_ptr<Windowing::LogicalWindowDescriptor>& windowDefinition)
{
    return std::make_shared<PhysicalThresholdWindowOperator>(getNextOperatorId(), inputSchema, outputSchema, windowDefinition);
}

std::shared_ptr<Windowing::LogicalWindowDescriptor> PhysicalThresholdWindowOperator::getWindowDefinition()
{
    return windowDefinition;
}

std::string PhysicalThresholdWindowOperator::toString() const
{
    std::stringstream out;
    out << std::endl;
    out << "PhysicalThresholdWindowOperator:\n";
    out << PhysicalUnaryOperator::toString();
    return out.str();
}

std::shared_ptr<Operator> PhysicalThresholdWindowOperator::copy()
{
    return create(inputSchema, outputSchema, windowDefinition);
}

}
