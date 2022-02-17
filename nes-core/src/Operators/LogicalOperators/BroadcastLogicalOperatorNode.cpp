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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/LogicalOperators/BroadcastLogicalOperatorNode.hpp>
#include <Optimizer/QuerySignatures/QuerySignatureUtil.hpp>

namespace NES {

BroadcastLogicalOperatorNode::BroadcastLogicalOperatorNode(OperatorId id)
    : OperatorNode(id), ExchangeOperatorNode(id), LogicalOperatorNode(id) {}

bool BroadcastLogicalOperatorNode::isIdentical(NodePtr const& rhs) const {
    return rhs->as<BroadcastLogicalOperatorNode>()->getId() == id;
}

bool BroadcastLogicalOperatorNode::equal(NodePtr const& rhs) const { return rhs->instanceOf<BroadcastLogicalOperatorNode>(); };

std::string BroadcastLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "BROADCAST(" << outputSchema->toString() << ")";
    return ss.str();
}

OperatorNodePtr BroadcastLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createBroadcastOperator(id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    for (auto [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}

bool BroadcastLogicalOperatorNode::inferSchema() {
    // We assume that all children operators have the same output schema otherwise this plan is not valid
    if (children.empty()) {
        NES_THROW_RUNTIME_ERROR("ExchangeOperatorNode: this node should have at least one child operator");
    }

    for (const auto& child : children) {
        if (!child->as<LogicalOperatorNode>()->inferSchema()) {
            return false;
        }
    }

    auto childSchema = children[0]->as<OperatorNode>()->getOutputSchema();
    for (const auto& child : children) {
        if (!child->as<OperatorNode>()->getOutputSchema()->equals(childSchema)) {
            NES_ERROR("ExchangeOperatorNode: infer schema failed. The schema has to be the same across all child operators.");
            return false;
        }
    }

    inputSchema = childSchema->copy();
    outputSchema = childSchema->copy();
    return true;
}

void BroadcastLogicalOperatorNode::inferStringSignature() { NES_NOT_IMPLEMENTED(); }

}// namespace NES
