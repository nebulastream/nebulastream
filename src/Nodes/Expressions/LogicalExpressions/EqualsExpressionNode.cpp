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
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
namespace NES {

EqualsExpressionNode::EqualsExpressionNode(EqualsExpressionNode* other) : LogicalBinaryExpressionNode(other) {}

EqualsExpressionNode::EqualsExpressionNode() : LogicalBinaryExpressionNode() {}

ExpressionNodePtr EqualsExpressionNode::create(const ExpressionNodePtr left, const ExpressionNodePtr right) {
    auto equals = std::make_shared<EqualsExpressionNode>();
    equals->setChildren(left, right);
    return equals;
}

bool EqualsExpressionNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<EqualsExpressionNode>()) {
        auto other = rhs->as<EqualsExpressionNode>();
        return this->getLeft()->equal(other->getLeft()) && this->getRight()->equal(other->getRight());
    }
    return false;
}

const std::string EqualsExpressionNode::toString() const {
    return "EqualsNode(" + stamp->toString() + ")";
}

ExpressionNodePtr EqualsExpressionNode::copy() {
    return std::make_shared<EqualsExpressionNode>(EqualsExpressionNode(this));
}

}// namespace NES
