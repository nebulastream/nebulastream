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
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
SinkLogicalOperator::SinkLogicalOperator(std::unique_ptr<Sinks::SinkDescriptor> sinkDescriptor, const OperatorId id)
    : Operator(id), LogicalUnaryOperator(id), sinkDescriptor(std::move(sinkDescriptor))
{
}

bool SinkLogicalOperator::isIdentical(NodePtr const& rhs) const
{
    return equal(rhs) && rhs->as<SinkLogicalOperator>()->getId() == id;
}

bool SinkLogicalOperator::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<SinkLogicalOperator>())
    {
        const auto sinkOperator = rhs->as<SinkLogicalOperator>();
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
    ///We pass invalid worker id here because the properties will be copied later automatically.
    auto exception = InvalidUseOfOperatorFunction(
        "OperatorLogicalSinkDescriptor does not support copy, because holds a unique pointer to a SinkDescriptor.");
    throw exception;
}

void SinkLogicalOperator::inferStringSignature()
{
    const OperatorPtr operatorNode = shared_from_this()->as<Operator>();
    NES_TRACE("Inferring String signature for {}", operatorNode->toString());

    ///Infer query signatures for child operators
    for (const auto& child : children)
    {
        const LogicalOperatorPtr childOperator = child->as<LogicalOperator>();
        childOperator->inferStringSignature();
    }
    std::stringstream signatureStream;
    auto childSignature = children[0]->as<LogicalOperator>()->getHashBasedSignature();
    signatureStream << "SINK()." << *childSignature.begin()->second.begin();

    ///Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}
} /// namespace NES
