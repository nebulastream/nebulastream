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

#include <Common/DataTypes/DataType.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
namespace NES {
OrExpressionNode::OrExpressionNode() : LogicalBinaryExpressionNode(){};

OrExpressionNode::OrExpressionNode(OrExpressionNode* other) : LogicalBinaryExpressionNode(other) {}

ExpressionNodePtr OrExpressionNode::create(const ExpressionNodePtr left, const ExpressionNodePtr right) {
    auto orNode = std::make_shared<OrExpressionNode>();
    orNode->setChildren(left, right);
    return orNode;
}

bool OrExpressionNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<OrExpressionNode>()) {
        auto otherAndNode = rhs->as<OrExpressionNode>();
        return getLeft()->equal(otherAndNode->getLeft()) && getRight()->equal(otherAndNode->getRight());
    }
    return false;
}
const std::string OrExpressionNode::toString() const { return "OrNode(" + stamp->toString() + ")"; }

void OrExpressionNode::inferStamp(SchemaPtr schema) {
    // delegate stamp inference of children
    ExpressionNode::inferStamp(schema);
    // check if children stamp is correct
    if (!getLeft()->isPredicate()) {
        NES_THROW_RUNTIME_ERROR("OR Expression Node: the stamp of left child must be boolean, but was: "
                                + getLeft()->getStamp()->toString());
    }
    if (!getRight()->isPredicate()) {
        NES_THROW_RUNTIME_ERROR("OR Expression Node: the stamp of left child must be boolean, but was: "
                                + getRight()->getStamp()->toString());
    }
}
ExpressionNodePtr OrExpressionNode::copy() { return std::make_shared<OrExpressionNode>(OrExpressionNode(this)); }

}// namespace NES