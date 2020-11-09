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
#include <Nodes/Expressions/ArithmeticalExpressions/DivExpressionNode.hpp>
#include <utility>
namespace NES {

DivExpressionNode::DivExpressionNode(DataTypePtr stamp) : ArithmeticalExpressionNode(std::move(stamp)){};

DivExpressionNode::DivExpressionNode(DivExpressionNode* other) : ArithmeticalExpressionNode(other) {}

ExpressionNodePtr DivExpressionNode::create(const ExpressionNodePtr left, const ExpressionNodePtr right) {
    auto divNode = std::make_shared<DivExpressionNode>(left->getStamp());
    divNode->setChildren(left, right);
    return divNode;
}

bool DivExpressionNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<DivExpressionNode>()) {
        auto otherDivNode = rhs->as<DivExpressionNode>();
        return getLeft()->equal(otherDivNode->getLeft()) && getRight()->equal(otherDivNode->getRight());
    }
    return false;
}
const std::string DivExpressionNode::toString() const {
    return "DivNode(" + stamp->toString() + ")";
}
ExpressionNodePtr DivExpressionNode::copy() {
    return std::make_shared<DivExpressionNode>(DivExpressionNode(this));
}

}// namespace NES