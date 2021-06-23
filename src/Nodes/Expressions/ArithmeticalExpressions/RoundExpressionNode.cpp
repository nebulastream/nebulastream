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
#include <Nodes/Expressions/ArithmeticalExpressions/RoundExpressionNode.hpp>
#include <cmath>

namespace NES {

RoundExpressionNode::RoundExpressionNode(DataTypePtr stamp) : ArithmeticalUnaryExpressionNode(std::move(stamp)){};

RoundExpressionNode::RoundExpressionNode(RoundExpressionNode* other) : ArithmeticalUnaryExpressionNode(other) {}

ExpressionNodePtr RoundExpressionNode::create(ExpressionNodePtr const& child) {
    auto roundNode = std::make_shared<RoundExpressionNode>(child->getStamp());
    roundNode->setChild(child);
    return roundNode;
}

void RoundExpressionNode::inferStamp(SchemaPtr schema) {
    // infer stamp of child, check if its numerical, assume same stamp
    ArithmeticalUnaryExpressionNode::inferStamp(schema);

    // if stamp is integer, convert stamp to float
    stamp = DataTypeFactory::createFloatFromInteger(stamp);
    NES_TRACE("RoundExpressionNode: converted stamp to float: " << toString());
}

bool RoundExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<RoundExpressionNode>()) {
        auto otherRoundNode = rhs->as<RoundExpressionNode>();
        return child()->equal(otherRoundNode->child());
    }
    return false;
}

std::string RoundExpressionNode::toString() const {
    std::stringstream ss;
    ss << "ROUND(" << children[0]->toString() << ")";
    return ss.str();
}

ExpressionNodePtr RoundExpressionNode::copy() { return std::make_shared<RoundExpressionNode>(RoundExpressionNode(this)); }

}// namespace NES