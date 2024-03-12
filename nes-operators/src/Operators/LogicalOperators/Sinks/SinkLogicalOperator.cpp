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

#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES {
SinkLogicalOperator::SinkLogicalOperator(const SinkDescriptorPtr& sinkDescriptor, OperatorId id)
    : Operator(id), LogicalUnaryOperator(id), sinkDescriptor(sinkDescriptor) {}

SinkDescriptorPtr SinkLogicalOperator::getSinkDescriptor() const { return sinkDescriptor; }

void SinkLogicalOperator::setSinkDescriptor(SinkDescriptorPtr sd) { this->sinkDescriptor = std::move(sd); }

bool SinkLogicalOperator::isIdentical(NodePtr const& rhs) const {
    return equal(rhs) && rhs->as<SinkLogicalOperator>()->getId() == id;
}

bool SinkLogicalOperator::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<SinkLogicalOperator>()) {
        auto sinkOperator = rhs->as<SinkLogicalOperator>();
        return sinkOperator->getSinkDescriptor()->equal(sinkDescriptor);
    }
    return false;
};

bool SinkLogicalOperator::inferSchema() { return LogicalUnaryOperator::inferSchema(); }

std::string SinkLogicalOperator::toString() const {
    std::stringstream ss;
    ss << "SINK(opId: " << id << ", statisticId: " << statisticId << ": {" << sinkDescriptor->toString() << "})";
    return ss.str();
}

OperatorPtr SinkLogicalOperator::copy() {
    //We pass invalid worker id here because the properties will be copied later automatically.
    auto copy = LogicalOperatorFactory::createSinkOperator(sinkDescriptor, INVALID_WORKER_NODE_ID, id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setZ3Signature(z3Signature);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setOperatorState(operatorState);
    copy->setStatisticId(statisticId);
    for (auto [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}

void SinkLogicalOperator::inferStringSignature() {
    OperatorPtr operatorNode = shared_from_this()->as<Operator>();
    NES_TRACE("Inferring String signature for {}", operatorNode->toString());

    //Infer query signatures for child operators
    for (auto& child : children) {
        const LogicalOperatorPtr childOperator = child->as<LogicalOperator>();
        childOperator->inferStringSignature();
    }
    std::stringstream signatureStream;
    auto childSignature = children[0]->as<LogicalOperator>()->getHashBasedSignature();
    signatureStream << "SINK()." << *childSignature.begin()->second.begin();

    //Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}
}// namespace NES
