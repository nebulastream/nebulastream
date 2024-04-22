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

#include <Common/DataTypes/DataType.hpp>
#include <Operators/Expressions/WhenExpressionNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES {
WhenExpressionNode::WhenExpressionNode(DataTypePtr stamp) : BinaryExpressionNode(std::move(stamp)){};

WhenExpressionNode::WhenExpressionNode(WhenExpressionNode* other) : BinaryExpressionNode(other) {}

ExpressionNodePtr WhenExpressionNode::create(const ExpressionNodePtr& left, const ExpressionNodePtr& right) {
    auto whenNode = std::make_shared<WhenExpressionNode>(left->getStamp());
    whenNode->setChildren(left, right);
    return whenNode;
}

void WhenExpressionNode::inferStamp( SchemaPtr schema) {

    auto left = getLeft();
    auto right = getRight();
    left->inferStamp( schema);
    right->inferStamp( schema);

    //left expression has to be boolean
    if (!left->getStamp()->isBoolean()) {
        NES_THROW_RUNTIME_ERROR("Error during stamp inference. Left type needs to be Boolean, but Left was: {} Right was: {}",
                                left->getStamp()->toString(),
                                right->getStamp()->toString());
    }

    //set stamp to right stamp, as only the left expression will be returned
    stamp = right->getStamp();
    NES_TRACE("WhenExpressionNode: we assigned the following stamp: {}", stamp->toString())
}

bool WhenExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<WhenExpressionNode>()) {
        auto otherWhenNode = rhs->as<WhenExpressionNode>();
        return getLeft()->equal(otherWhenNode->getLeft()) && getRight()->equal(otherWhenNode->getRight());
    }
    return false;
}

std::string WhenExpressionNode::toString() const {
    std::stringstream ss;
    ss << "WHEN(" << children[0]->toString() << "," << children[1]->toString() << ")";
    return ss.str();
}

ExpressionNodePtr WhenExpressionNode::copy() {

    return WhenExpressionNode::create(children[0]->as<ExpressionNode>()->copy(), children[1]->as<ExpressionNode>()->copy());
}

}// namespace NES