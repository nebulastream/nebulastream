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
#include <Identifiers/Identifiers.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkStrategyDescriptor.hpp>
#include <Operators/Operator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>


namespace NES
{

WatermarkAssignerLogicalOperator::WatermarkAssignerLogicalOperator(
    const std::shared_ptr<Windowing::WatermarkStrategyDescriptor>& watermarkStrategyDescriptor, OperatorId id)
    : Operator(id), UnaryLogicalOperator(id), watermarkStrategyDescriptor(watermarkStrategyDescriptor)
{
}

std::shared_ptr<Windowing::WatermarkStrategyDescriptor> WatermarkAssignerLogicalOperator::getWatermarkStrategyDescriptor() const
{
    return watermarkStrategyDescriptor;
}

std::ostream& WatermarkAssignerLogicalOperator::toDebugString(std::ostream& os) const
{
    return os << fmt::format("WATERMARKASSIGNER({})", id);
}

std::ostream& WatermarkAssignerLogicalOperator::toQueryPlanString(std::ostream& os) const
{
    return os << "WATERMARKASSIGNER";
}

bool WatermarkAssignerLogicalOperator::isIdentical(const Operator& rhs) const
{
    return (*this == rhs) && dynamic_cast<const WatermarkAssignerLogicalOperator*>(&rhs)->getId() == id;
}

bool WatermarkAssignerLogicalOperator::operator==(const Operator& rhs) const
{
    if (const auto rhsOperator = dynamic_cast<const WatermarkAssignerLogicalOperator*>(&rhs))
    {
        return getWatermarkStrategyDescriptor() == rhsOperator->getWatermarkStrategyDescriptor();
    }
    return false;
}

std::shared_ptr<Operator> WatermarkAssignerLogicalOperator::clone() const
{
    auto copy = std::make_shared<WatermarkAssignerLogicalOperator>(watermarkStrategyDescriptor, id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    for (const auto& pair : properties)
    {
        copy->addProperty(pair.first, pair.second);
    }
    return copy;
}

bool WatermarkAssignerLogicalOperator::inferSchema()
{
    if (!UnaryLogicalOperator::inferSchema())
    {
        return false;
    }
    watermarkStrategyDescriptor->inferStamp(inputSchema);
    return true;
}

}
