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
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/FloorExpressionNode.hpp>
#include <cmath>

namespace NES {

FloorExpressionNode::FloorExpressionNode(DataTypePtr stamp) : ArithmeticalUnaryExpressionNode(std::move(stamp)){};

FloorExpressionNode::FloorExpressionNode(FloorExpressionNode* other) : ArithmeticalUnaryExpressionNode(other) {}

ExpressionNodePtr FloorExpressionNode::create(ExpressionNodePtr const& child) {
    auto floorNode = std::make_shared<FloorExpressionNode>(child->getStamp());
    floorNode->setChild(child);
    return floorNode;
}

void FloorExpressionNode::inferStamp(SchemaPtr schema) {
    // infer stamp of child, check if its numerical, assume same stamp
    ArithmeticalUnaryExpressionNode::inferStamp(schema);

    // if stamp is integer, convert stamp to float
    stamp = DataTypeFactory::createFloatFromInteger(stamp);
    NES_TRACE("FloorExpressionNode: converted stamp to float: " << toString());
}

bool FloorExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<FloorExpressionNode>()) {
        auto otherFloorNode = rhs->as<FloorExpressionNode>();
        return child()->equal(otherFloorNode->child());
    }
    return false;
}

std::string FloorExpressionNode::toString() const {
    std::stringstream ss;
    ss << "FLOOR(" << children[0]->toString() << ")";
    return ss.str();
}

ExpressionNodePtr FloorExpressionNode::copy() { return std::make_shared<FloorExpressionNode>(FloorExpressionNode(this)); }

}// namespace NES