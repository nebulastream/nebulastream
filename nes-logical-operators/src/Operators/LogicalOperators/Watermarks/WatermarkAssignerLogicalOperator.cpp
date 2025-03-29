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


namespace NES
{

WatermarkAssignerLogicalOperator::WatermarkAssignerLogicalOperator(
    std::shared_ptr<Windowing::WatermarkStrategyDescriptor> const& watermarkStrategyDescriptor, OperatorId id)
    : Operator(id), LogicalUnaryOperator(id), watermarkStrategyDescriptor(watermarkStrategyDescriptor)
{
}

std::shared_ptr<Windowing::WatermarkStrategyDescriptor> WatermarkAssignerLogicalOperator::getWatermarkStrategyDescriptor() const
{
    return watermarkStrategyDescriptor;
}

std::string WatermarkAssignerLogicalOperator::toString() const
{
    std::stringstream ss;
    ss << "WATERMARKASSIGNER(" << id << ")";
    return ss.str();
}

bool WatermarkAssignerLogicalOperator::isIdentical(std::shared_ptr<Operator> const& rhs) const
{
    return equal(rhs) && NES::Util::as<WatermarkAssignerLogicalOperator>(rhs)->getId() == id;
}

bool WatermarkAssignerLogicalOperator::equal(std::shared_ptr<Operator> const& rhs) const
{
    if (NES::Util::instanceOf<WatermarkAssignerLogicalOperator>(rhs))
    {
        const auto watermarkAssignerOperator = NES::Util::as<WatermarkAssignerLogicalOperator>(rhs);
        return watermarkStrategyDescriptor->equal(watermarkAssignerOperator->getWatermarkStrategyDescriptor());
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
    if (!LogicalUnaryOperator::inferSchema())
    {
        return false;
    }
    watermarkStrategyDescriptor->inferStamp(inputSchema);
    return true;
}

}
