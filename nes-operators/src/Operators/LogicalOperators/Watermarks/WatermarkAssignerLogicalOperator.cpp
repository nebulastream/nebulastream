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
    std::shared_ptr<Windowing::WatermarkStrategyDescriptor> watermarkStrategyDescriptor, OperatorId id)
    : Operator(id), LogicalUnaryOperator(id), watermarkStrategyDescriptor(std::move(watermarkStrategyDescriptor))
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

bool WatermarkAssignerLogicalOperator::isIdentical(const std::shared_ptr<Node>& rhs) const
{
    return equal(rhs) && NES::Util::as<WatermarkAssignerLogicalOperator>(rhs)->getId() == id;
}

bool WatermarkAssignerLogicalOperator::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<WatermarkAssignerLogicalOperator>(rhs))
    {
        const auto watermarkAssignerOperator = NES::Util::as<WatermarkAssignerLogicalOperator>(rhs);
        return watermarkStrategyDescriptor->equal(watermarkAssignerOperator->getWatermarkStrategyDescriptor());
    }
    return false;
}

std::shared_ptr<Operator> WatermarkAssignerLogicalOperator::copy()
{
    auto copy = std::make_shared<WatermarkAssignerLogicalOperator>(watermarkStrategyDescriptor, id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setOperatorState(operatorState);
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

void WatermarkAssignerLogicalOperator::inferStringSignature()
{
    const std::shared_ptr<Operator> operatorNode = NES::Util::as<Operator>(shared_from_this());
    NES_TRACE("Inferring String signature for {}", *operatorNode);

    ///Infer query signatures for child operators
    for (const auto& child : children)
    {
        const std::shared_ptr<LogicalOperator> childOperator = NES::Util::as<LogicalOperator>(child);
        childOperator->inferStringSignature();
    }

    std::stringstream signatureStream;
    signatureStream << "WATERMARKASSIGNER(" << watermarkStrategyDescriptor->toString() << ").";
    const auto childSignature = NES::Util::as<LogicalOperator>(children[0])->getHashBasedSignature();
    signatureStream << *childSignature.begin()->second.begin();

    ///Update the signature
    const auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}

}
