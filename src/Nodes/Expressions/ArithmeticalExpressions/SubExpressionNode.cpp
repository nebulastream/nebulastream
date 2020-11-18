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
#include <Nodes/Expressions/ArithmeticalExpressions/SubExpressionNode.hpp>
#include <utility>
namespace NES {

SubExpressionNode::SubExpressionNode(DataTypePtr stamp) : ArithmeticalExpressionNode(std::move(stamp)){};

SubExpressionNode::SubExpressionNode(SubExpressionNode* other) : ArithmeticalExpressionNode(other) {}

ExpressionNodePtr SubExpressionNode::create(const ExpressionNodePtr left, const ExpressionNodePtr right) {
    auto subNode = std::make_shared<SubExpressionNode>(left->getStamp());
    subNode->setChildren(left, right);
    return subNode;
}

bool SubExpressionNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<SubExpressionNode>()) {
        auto otherSubNode = rhs->as<SubExpressionNode>();
        return getLeft()->equal(otherSubNode->getLeft()) && getRight()->equal(otherSubNode->getRight());
    }
    return false;
}
const std::string SubExpressionNode::toString() const { return "SubNode(" + stamp->toString() + ")"; }
ExpressionNodePtr SubExpressionNode::copy() { return std::make_shared<SubExpressionNode>(SubExpressionNode(this)); }

}// namespace NES