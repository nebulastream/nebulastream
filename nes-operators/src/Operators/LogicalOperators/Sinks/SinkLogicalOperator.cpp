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

#include <utility>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>


namespace NES
{

bool SinkLogicalOperator::isIdentical(NodePtr const& rhs) const
{
    return equal(rhs) && NES::Util::as<SinkLogicalOperator>(rhs)->getId() == id;
}

bool SinkLogicalOperator::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<SinkLogicalOperator>(rhs))
    {
        const auto sinkOperator = NES::Util::as<SinkLogicalOperator>(rhs);
        return this->sinkName == sinkOperator->sinkName
            and ((this->sinkDescriptor) ? this->sinkDescriptor == sinkOperator->sinkDescriptor : true);
    }
    return false;
};

std::string SinkLogicalOperator::toString() const
{
    std::stringstream ss;
    ss << fmt::format("SINK(opId: {}, sinkName: {}, sinkDescriptor: ", id, sinkName);
    ((sinkDescriptor) ? (ss << sinkDescriptor) : ss << "(null))");
    ss << ")";
    return ss.str();
}
const Sinks::SinkDescriptor& SinkLogicalOperator::getSinkDescriptorRef() const
{
    if (this->sinkDescriptor)
    {
        return *sinkDescriptor;
    }
    throw UnknownSinkType("Tried to access the SinkDescriptor of a SinkLogicalOperator that does not have a SinkDescriptor yet.");
}

OperatorPtr SinkLogicalOperator::copy()
{
    ///We pass invalid worker id here because the properties will be copied later automatically.
    auto sinkDescriptorPtrCopy = sinkDescriptor;
    auto copy = std::make_shared<SinkLogicalOperator>(sinkName, id);
    copy->sinkDescriptor = std::move(sinkDescriptorPtrCopy);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setZ3Signature(z3Signature);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setOperatorState(operatorState);
    for (const auto& [propertyName, propertyValue] : properties)
    {
        copy->addProperty(propertyName, propertyValue);
    }
    return copy;
}

void SinkLogicalOperator::inferStringSignature()
{
    const OperatorPtr operatorNode = NES::Util::as<Operator>(shared_from_this());
    NES_TRACE("Inferring String signature for {}", operatorNode->toString());

    ///Infer query signatures for child operators
    for (const auto& child : children)
    {
        const LogicalOperatorPtr childOperator = NES::Util::as<LogicalOperator>(child);
        childOperator->inferStringSignature();
    }
    std::stringstream signatureStream;
    auto childSignature = NES::Util::as<LogicalOperator>(children[0])->getHashBasedSignature();
    signatureStream << "SINK()." << *childSignature.begin()->second.begin();

    ///Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}
}
