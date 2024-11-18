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

#include <Expressions/TernaryExpressionNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES {
TernaryExpressionNode::TernaryExpressionNode(DataTypePtr stamp) : ExpressionNode(std::move(stamp)) {}

TernaryExpressionNode::TernaryExpressionNode(TernaryExpressionNode* other) : ExpressionNode(other) {
    addChildWithEqual(getLeft()->copy());
    addChildWithEqual(getMiddle()->copy());
    addChildWithEqual(getRight()->copy());
}

void TernaryExpressionNode::setChildren(ExpressionNodePtr const& left,
                                        ExpressionNodePtr const& middle,
                                        ExpressionNodePtr const& right) {
    addChildWithEqual(left);
    addChildWithEqual(middle);
    addChildWithEqual(right);
}

ExpressionNodePtr TernaryExpressionNode::getLeft() const {
    if (children.size() != 3) {
        NES_FATAL_ERROR("A Ternary expression always should have three children, but it had: {}", children.size());
    }
    return children[0]->as<ExpressionNode>();
}

ExpressionNodePtr TernaryExpressionNode::getMiddle() const {
    if (children.size() != 3) {
        NES_FATAL_ERROR("A Ternary expression always should have three children, but it had: {}", children.size());
    }
    return children[1]->as<ExpressionNode>();
}

ExpressionNodePtr TernaryExpressionNode::getRight() const {
    if (children.size() != 3) {
        NES_FATAL_ERROR("A Ternary expression always should have two children, but it had: {}", children.size());
    }
    return children[2]->as<ExpressionNode>();
}

}// namespace NES
