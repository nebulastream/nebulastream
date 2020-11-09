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

#include <Nodes/Expressions/BinaryExpressionNode.hpp>
#include <utility>

namespace NES {
BinaryExpressionNode::BinaryExpressionNode(DataTypePtr stamp) : ExpressionNode(std::move(stamp)) {}

BinaryExpressionNode::BinaryExpressionNode(BinaryExpressionNode* other) : ExpressionNode(other) {
    addChildWithEqual(getLeft()->copy());
    addChildWithEqual(getRight()->copy());
}

void BinaryExpressionNode::setChildren(const ExpressionNodePtr left, const ExpressionNodePtr right) {
    addChildWithEqual(left);
    addChildWithEqual(right);
}

ExpressionNodePtr BinaryExpressionNode::getLeft() const {
    if (children.size() != 2) {
        NES_FATAL_ERROR("A binary expression always should have two children, but it had: " << children.size());
    }
    return children[0]->as<ExpressionNode>();
}

ExpressionNodePtr BinaryExpressionNode::getRight() const {
    if (children.size() != 2) {
        NES_FATAL_ERROR("A binary expression always should have two children, but it had: " << children.size());
    }
    return children[1]->as<ExpressionNode>();
}

}// namespace NES