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

#include <Nodes/Expressions/MultiExpressionNode.hpp>

namespace NES{
MultiExpressionNode::MultiExpressionNode(DataTypePtr stamp) : ExpressionNode(std::move(stamp)) {}

MultiExpressionNode::MultiExpressionNode(MultiExpressionNode* other) : ExpressionNode(other) {
    for (auto child : children()) {
        addChildWithEqual(child->copy());
    }
}

void MultiExpressionNode::setChildren(const std::vector<ExpressionNodePtr>& children) {
    for (auto child : children) {
        addChildWithEqual(child);
    }
}

std::vector<ExpressionNodePtr> MultiExpressionNode::children() const {
    std::vector<ExpressionNodePtr> obtainedChildren;
    for (auto child : Node::children) {
        obtainedChildren.push_back(child->as<ExpressionNode>());
    }
    return obtainedChildren;
}
}