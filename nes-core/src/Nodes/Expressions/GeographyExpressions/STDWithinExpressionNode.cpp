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
#include <Nodes/Expressions/GeographyExpressions/GeographyFieldsAccessExpressionNode.hpp>
#include <Nodes/Expressions/GeographyExpressions/STDWithinExpressionNode.hpp>

namespace NES {
STDWithinExpressionNode::STDWithinExpressionNode() : ExpressionNode(DataTypeFactory::createBoolean()) {}

STDWithinExpressionNode::STDWithinExpressionNode(STDWithinExpressionNode* other) : ExpressionNode(other) {
    addChildWithEqual(getPoint()->copy());
    addChildWithEqual(getWKT()->copy());
    addChildWithEqual(getDistance()->copy());
}

ExpressionNodePtr STDWithinExpressionNode::create(const GeographyFieldsAccessExpressionNodePtr& point,
                                                  const ConstantValueExpressionNodePtr& wkt,
                                                  const ConstantValueExpressionNodePtr& distance) {
    auto stDWithinNode = std::make_shared<STDWithinExpressionNode>();
    stDWithinNode->setChildren(point, wkt, distance);
    return stDWithinNode;
}

bool STDWithinExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<STDWithinExpressionNode>()) {
        auto otherAndNode = rhs->as<STDWithinExpressionNode>();
        return getPoint()->equal(otherAndNode->getPoint()) && getWKT()->equal(otherAndNode->getWKT())
            && getDistance()->equal(otherAndNode->getDistance());
    }
    return false;
}

std::string STDWithinExpressionNode::toString() const {
    std::stringstream ss;
    ss << "ST_DWITHIN(" << children[0]->toString() << ", " << children[1]->toString() << ", " << children[2]->toString() << ")";
    return ss.str();
}

void STDWithinExpressionNode::setChildren(ExpressionNodePtr const& point,
                                          ExpressionNodePtr const& wkt,
                                          ExpressionNodePtr const& distance) {
    addChildWithEqual(point);
    addChildWithEqual(wkt);
    addChildWithEqual(distance);
}

ExpressionNodePtr STDWithinExpressionNode::getPoint() const {
    if (children.size() != 3) {
        NES_FATAL_ERROR("An ST_DWithin Expression should always have three children, but it has: " << children.size());
    }
    return children[0]->as<GeographyFieldsAccessExpressionNode>();
}

ExpressionNodePtr STDWithinExpressionNode::getWKT() const {
    if (children.size() != 3) {
        NES_FATAL_ERROR("An ST_DWithin Expression should always have three children, but it has: " << children.size());
    }
    return children[1]->as<ConstantValueExpressionNode>();
}

ExpressionNodePtr STDWithinExpressionNode::getDistance() const {
    if (children.size() != 3) {
        NES_FATAL_ERROR("An ST_DWithin Expression should always have three children, but it has: " << children.size());
    }
    return children[2]->as<ConstantValueExpressionNode>();
}

void STDWithinExpressionNode::inferStamp(SchemaPtr schema) {
    // infer the stamps of the left and right child
    auto point = getPoint();
    auto wkt = getWKT();
    auto distance = getDistance();
    point->inferStamp(schema);
    wkt->inferStamp(schema);
    distance->inferStamp(schema);

    // check sub expressions if they are the correct type
    if (!point->getStamp()->isFloat() || !wkt->getStamp()->isCharArray() || !distance->getStamp()->isNumeric()) {
        throw std::logic_error(
            "ST_DWithinExpressionNode: Error during stamp inference. Types need to be Float and Text but Point was:"
            + point->getStamp()->toString() + ", WKT was: " + wkt->getStamp()->toString()
            + ", and the Distance was: " + distance->getStamp()->toString());
    }

    stamp = DataTypeFactory::createBoolean();
    NES_TRACE("ST_DWithinExpressionNode: The following stamp was assigned: " << toString());
}

ExpressionNodePtr STDWithinExpressionNode::copy() {
    return std::make_shared<STDWithinExpressionNode>(STDWithinExpressionNode(this));
}

}// namespace NES