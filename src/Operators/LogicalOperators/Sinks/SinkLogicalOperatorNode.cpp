/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Optimizer/Utils/QuerySignatureUtil.hpp>
#include <utility>
#include <z3++.h>

namespace NES {
SinkLogicalOperatorNode::SinkLogicalOperatorNode(const SinkDescriptorPtr sinkDescriptor, OperatorId id)
    : sinkDescriptor(sinkDescriptor), UnaryOperatorNode(id) {}

SinkDescriptorPtr SinkLogicalOperatorNode::getSinkDescriptor() { return sinkDescriptor; }

void SinkLogicalOperatorNode::setSinkDescriptor(SinkDescriptorPtr sinkDescriptor) {
    this->sinkDescriptor = std::move(sinkDescriptor);
}

bool SinkLogicalOperatorNode::isIdentical(NodePtr rhs) const {
    return equal(rhs) && rhs->as<SinkLogicalOperatorNode>()->getId() == id;
}

bool SinkLogicalOperatorNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<SinkLogicalOperatorNode>()) {
        auto sinkOperator = rhs->as<SinkLogicalOperatorNode>();
        return sinkOperator->getSinkDescriptor()->equal(sinkDescriptor);
    }
    return false;
};

bool SinkLogicalOperatorNode::inferSchema() {
    // We assume that all children operators have the same output schema otherwise this plan is not valid
    for (const auto& child : children) {
        if (!child->as<OperatorNode>()->inferSchema()) {
            return false;
        }
    }
    if (children.empty()) {
        NES_THROW_RUNTIME_ERROR("UnaryOperatorNode: this node should have at least one child operator");
    }
    auto childSchema = children[0]->as<OperatorNode>()->getOutputSchema();
    for (const auto& child : children) {
        if (!child->as<OperatorNode>()->getOutputSchema()->equals(childSchema)) {
            NES_ERROR("UnaryOperatorNode: infer schema failed. The schema has to be the same across all child operators.");
            return false;
        }
    }
    inputSchema = childSchema->copy();
    outputSchema = childSchema->copy();
    std::string& qualifyingName = outputSchema->qualifyingName;
    //Remove qualifier from field name
    for(auto& field : outputSchema->fields){
        std::string& fieldName = field->name;
        field->name = fieldName.substr(fieldName.find(qualifyingName) + qualifyingName.length(), fieldName.length());
    }
    return true;
}

const std::string SinkLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "SINK(" << id << ")";
    return ss.str();
}

std::string SinkLogicalOperatorNode::getStringBasedSignature() {
    return "SINK()." + children[0]->as<LogicalOperatorNode>()->getStringBasedSignature();
}

OperatorNodePtr SinkLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createSinkOperator(sinkDescriptor, id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setSignature(signature);
    return copy;
}
}// namespace NES
