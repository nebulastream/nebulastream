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
#include <Operators/LogicalOperators/Watermarks/WatermarkStrategyDescriptor.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalWatermarkAssignmentOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

PhysicalWatermarkAssignmentOperator::PhysicalWatermarkAssignmentOperator(
    OperatorId id,
    Schema inputSchema,
    Schema outputSchema,
    std::shared_ptr<Windowing::WatermarkStrategyDescriptor> watermarkStrategyDescriptor)
    : Operator(id)
    , PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema))
    , watermarkStrategyDescriptor(std::move(watermarkStrategyDescriptor))
{
}

std::shared_ptr<PhysicalOperator> PhysicalWatermarkAssignmentOperator::create(
    OperatorId id,
    const Schema& inputSchema,
    const Schema& outputSchema,
    const std::shared_ptr<Windowing::WatermarkStrategyDescriptor>& watermarkStrategyDescriptor)
{
    return std::make_shared<PhysicalWatermarkAssignmentOperator>(id, inputSchema, outputSchema, watermarkStrategyDescriptor);
}

std::shared_ptr<Windowing::WatermarkStrategyDescriptor> PhysicalWatermarkAssignmentOperator::getWatermarkStrategyDescriptor() const
{
    return watermarkStrategyDescriptor;
}

std::shared_ptr<PhysicalOperator> PhysicalWatermarkAssignmentOperator::create(
    const Schema& inputSchema,
    const Schema& outputSchema,
    const std::shared_ptr<Windowing::WatermarkStrategyDescriptor>& watermarkStrategyDescriptor)
{
    return create(getNextOperatorId(), std::move(inputSchema), std::move(outputSchema), std::move(watermarkStrategyDescriptor));
}

std::string PhysicalWatermarkAssignmentOperator::toString() const
{
    std::stringstream out;
    out << std::endl;
    out << "PhysicalWatermarkAssignmentOperator:\n";
    out << PhysicalUnaryOperator::toString();
    if (watermarkStrategyDescriptor != nullptr)
    {
        out << "watermarkStrategyDescriptor: " << watermarkStrategyDescriptor->toString();
    }
    out << std::endl;
    return out.str();
}

std::shared_ptr<Operator> PhysicalWatermarkAssignmentOperator::copy()
{
    auto result = create(id, inputSchema, outputSchema, getWatermarkStrategyDescriptor());
    result->addAllProperties(properties);
    return result;
}

}
