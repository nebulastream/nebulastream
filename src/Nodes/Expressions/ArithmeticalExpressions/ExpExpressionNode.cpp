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
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/ExpExpressionNode.hpp>
#include <cmath>

namespace NES {

ExpExpressionNode::ExpExpressionNode(DataTypePtr stamp) : ArithmeticalUnaryExpressionNode(std::move(stamp)){};

ExpExpressionNode::ExpExpressionNode(ExpExpressionNode* other) : ArithmeticalUnaryExpressionNode(other) {}

ExpressionNodePtr ExpExpressionNode::create(ExpressionNodePtr const& child) {
    auto expNode = std::make_shared<ExpExpressionNode>(child->getStamp());
    expNode->setChild(child);
    return expNode;
}

void ExpExpressionNode::inferStamp(SchemaPtr schema) {
    // infer stamp of child, check if its numerical, assume same stamp
    ArithmeticalUnaryExpressionNode::inferStamp(schema);

    // if stamp is integer, convert stamp to float
    stamp = DataTypeFactory::createFloatFromInteger(stamp);

    // increase lower bound to 0
    stamp = DataTypeFactory::copyTypeAndIncreaseLowerBound(stamp, 0.0);
    NES_TRACE("ExpExpressionNode: converted stamp to float and increased the lower bound of stamp to 0: " << toString());
}

bool ExpExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<ExpExpressionNode>()) {
        auto otherExpNode = rhs->as<ExpExpressionNode>();
        return child()->equal(otherExpNode->child());
    }
    return false;
}

std::string ExpExpressionNode::toString() const {
    std::stringstream ss;
    ss << "EXP(" << children[0]->toString() << ")";
    return ss.str();
}

ExpressionNodePtr ExpExpressionNode::copy() { return std::make_shared<ExpExpressionNode>(ExpExpressionNode(this)); }

}// namespace NES