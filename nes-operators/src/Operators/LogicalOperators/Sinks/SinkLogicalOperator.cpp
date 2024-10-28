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
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>


namespace NES
{
SinkLogicalOperator::SinkLogicalOperator(std::string sinkName, const OperatorId id)
    : Operator(id), LogicalUnaryOperator(id), sinkName(std::move(sinkName))
{
}
SinkLogicalOperator::SinkLogicalOperator(std::string sinkName, std::shared_ptr<Sinks::SinkDescriptor> sinkDescriptor, const OperatorId id)
    : Operator(id), LogicalUnaryOperator(id), sinkName(std::move(sinkName)), sinkDescriptor(std::move(sinkDescriptor))
{
}

bool SinkLogicalOperator::isIdentical(NodePtr const& rhs) const
{
    return equal(rhs) && NES::Util::as<SinkLogicalOperator>(rhs)->getId() == id;
}

bool SinkLogicalOperator::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<SinkLogicalOperator>(rhs))
    {
        const auto sinkOperator = NES::Util::as<SinkLogicalOperator>(rhs);
        return *this->sinkDescriptor == sinkOperator->getSinkDescriptorRef();
    }
    return false;
};

std::string SinkLogicalOperator::toString() const
{
    std::stringstream ss;
    ss << "SINK(opId: " << id << ": {" << sinkDescriptor << "})";
    return ss.str();
}
const Sinks::SinkDescriptor& SinkLogicalOperator::getSinkDescriptorRef() const
{
    return *sinkDescriptor;
}

OperatorPtr SinkLogicalOperator::copy()
{
    auto sinkDescriptorPtrCopy = sinkDescriptor;
    auto result = std::make_shared<SinkLogicalOperator>(sinkName, std::move(sinkDescriptorPtrCopy), id);
    result->inferSchema();
    result->addAllProperties(properties);
    return result;
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
