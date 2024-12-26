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

#include <Expressions/SenaryExpressionNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES {
SenaryExpressionNode::SenaryExpressionNode(DataTypePtr stamp) : ExpressionNode(std::move(stamp)) {}

SenaryExpressionNode::SenaryExpressionNode(SenaryExpressionNode* other) : ExpressionNode(other) {
    addChildWithEqual(getone()->copy());
    addChildWithEqual(gettwo()->copy());
    addChildWithEqual(getthree()->copy());
    addChildWithEqual(getfour()->copy());
    addChildWithEqual(getfive()->copy());
    addChildWithEqual(getsix()->copy());
}

void SenaryExpressionNode::setChildren(ExpressionNodePtr const& one,
                                       ExpressionNodePtr const& two,
                                       ExpressionNodePtr const& three,
                                       ExpressionNodePtr const& four,
                                       ExpressionNodePtr const& five,
                                       ExpressionNodePtr const& six) {
    addChildWithEqual(one);
    addChildWithEqual(two);
    addChildWithEqual(three);
    addChildWithEqual(four);
    addChildWithEqual(five);
    addChildWithEqual(six);
}

ExpressionNodePtr SenaryExpressionNode::getone() const {
    if (children.size() != 6) {
        NES_FATAL_ERROR("A Senary expression always should have six children, but it had: {}", children.size());
    }
    return children[0]->as<ExpressionNode>();
}

ExpressionNodePtr SenaryExpressionNode::gettwo() const {
    if (children.size() != 6) {
        NES_FATAL_ERROR("A Senary expression always should have six children, but it had: {}", children.size());
    }
    return children[1]->as<ExpressionNode>();
}

ExpressionNodePtr SenaryExpressionNode::getthree() const {
    if (children.size() != 6) {
        NES_FATAL_ERROR("A Senary expression always should have six children, but it had: {}", children.size());
    }
    return children[2]->as<ExpressionNode>();
}

ExpressionNodePtr SenaryExpressionNode::getfour() const {
    if (children.size() != 6) {
        NES_FATAL_ERROR("A Senary expression always should have six children, but it had: {}", children.size());
    }
    return children[3]->as<ExpressionNode>();
}

ExpressionNodePtr SenaryExpressionNode::getfive() const {
    if (children.size() != 6) {
        NES_FATAL_ERROR("A Senary expression always should have six children, but it had: {}", children.size());
    }
    return children[4]->as<ExpressionNode>();
}

ExpressionNodePtr SenaryExpressionNode::getsix() const {
    if (children.size() != 6) {
        NES_FATAL_ERROR("A Senary expression always should have six children, but it had: {}", children.size());
    }
    return children[5]->as<ExpressionNode>();
}

}// namespace NES
