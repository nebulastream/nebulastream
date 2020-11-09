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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/LogicalOperators/BroadcastLogicalOperatorNode.hpp>
#include <Optimizer/Utils/OperatorToZ3ExprUtil.hpp>
#include <z3++.h>

namespace NES {

BroadcastLogicalOperatorNode::BroadcastLogicalOperatorNode(OperatorId id) : LogicalOperatorNode(id) {}

bool BroadcastLogicalOperatorNode::isIdentical(NodePtr rhs) const {
    return rhs->as<BroadcastLogicalOperatorNode>()->getId() == id;
}

bool BroadcastLogicalOperatorNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<BroadcastLogicalOperatorNode>()) {
        return true;
    }
    return false;
};

bool BroadcastLogicalOperatorNode::inferSchema() {
    // infer the default input and output schema
    OperatorNode::inferSchema();
    return true;
}

const std::string BroadcastLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "BROADCAST(" << outputSchema->toString() << ")";
    return ss.str();
}

OperatorNodePtr BroadcastLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createBroadcastOperator(id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}

void BroadcastLogicalOperatorNode::inferZ3Expression(z3::ContextPtr context) {
    OperatorNodePtr operatorNode = shared_from_this()->as<OperatorNode>();
    expr = std::make_shared<z3::expr>(OperatorToZ3ExprUtil::createForOperator(operatorNode, *context));
}
}// namespace NES
