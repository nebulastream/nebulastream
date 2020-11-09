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
#include <Nodes/Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <utility>
namespace NES {

AddExpressionNode::AddExpressionNode(DataTypePtr stamp) : ArithmeticalExpressionNode(std::move(stamp)){};

AddExpressionNode::AddExpressionNode(AddExpressionNode* other) : ArithmeticalExpressionNode(other) {}

ExpressionNodePtr AddExpressionNode::create(const ExpressionNodePtr left, const ExpressionNodePtr right) {
    auto addNode = std::make_shared<AddExpressionNode>(left->getStamp());
    addNode->setChildren(left, right);
    return addNode;
}

bool AddExpressionNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<AddExpressionNode>()) {
        auto otherAddNode = rhs->as<AddExpressionNode>();
        return getLeft()->equal(otherAddNode->getLeft()) && getRight()->equal(otherAddNode->getRight());
    }
    return false;
}

const std::string AddExpressionNode::toString() const {
    return "AddNode(" + stamp->toString() + ")";
}
ExpressionNodePtr AddExpressionNode::copy() {
    return std::make_shared<AddExpressionNode>(AddExpressionNode(this));
}

}// namespace NES