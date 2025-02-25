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
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>


namespace NES
{

WatermarkAssignerLogicalOperator::WatermarkAssignerLogicalOperator(
    Windowing::WatermarkStrategyDescriptorPtr const& watermarkStrategyDescriptor, OperatorId id)
    : Operator(id), LogicalUnaryOperator(id), watermarkStrategyDescriptor(watermarkStrategyDescriptor)
{
}

Windowing::WatermarkStrategyDescriptorPtr WatermarkAssignerLogicalOperator::getWatermarkStrategyDescriptor() const
{
    return watermarkStrategyDescriptor;
}

std::string WatermarkAssignerLogicalOperator::toString() const
{
    std::stringstream ss;
    ss << "WATERMARKASSIGNER(" << id << ")";
    return ss.str();
}

bool WatermarkAssignerLogicalOperator::isIdentical(NodePtr const& rhs) const
{
    return equal(rhs) && NES::Util::as<WatermarkAssignerLogicalOperator>(rhs)->getId() == id;
}

bool WatermarkAssignerLogicalOperator::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<WatermarkAssignerLogicalOperator>(rhs))
    {
        auto watermarkAssignerOperator = NES::Util::as<WatermarkAssignerLogicalOperator>(rhs);
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
    copy->setZ3Signature(z3Signature);
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
    std::shared_ptr<Operator> operatorNode = NES::Util::as<Operator>(shared_from_this());
    NES_TRACE("Inferring String signature for {}", *operatorNode);

    ///Infer query signatures for child operators
    for (const auto& child : children)
    {
        const LogicalOperatorPtr childOperator = NES::Util::as<LogicalOperator>(child);
        childOperator->inferStringSignature();
    }

    std::stringstream signatureStream;
    signatureStream << "WATERMARKASSIGNER(" << watermarkStrategyDescriptor->toString() << ").";
    auto childSignature = NES::Util::as<LogicalOperator>(children[0])->getHashBasedSignature();
    signatureStream << *childSignature.begin()->second.begin();

    ///Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}

}
