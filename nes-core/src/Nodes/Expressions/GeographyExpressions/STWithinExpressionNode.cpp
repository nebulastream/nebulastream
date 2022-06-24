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
#include <Nodes/Expressions/GeographyExpressions/STWithinExpressionNode.hpp>

namespace NES {

STWithinExpressionNode::STWithinExpressionNode() : ExpressionNode(DataTypeFactory::createBoolean()) {}

STWithinExpressionNode::STWithinExpressionNode(STWithinExpressionNode* other) : ExpressionNode(other) {
    addChildWithEqual(getPoint()->copy());
    addChildWithEqual(getShape()->copy());
}

ExpressionNodePtr STWithinExpressionNode::create(const GeographyFieldsAccessExpressionNodePtr& point,
                                                 const ShapeExpressionNodePtr& shapeExpression) {
    auto stWithinNode = std::make_shared<STWithinExpressionNode>();
    stWithinNode->setChildren(point, shapeExpression);
    return stWithinNode;
}

bool STWithinExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<STWithinExpressionNode>()) {
        auto otherNode = rhs->as<STWithinExpressionNode>();
        return getPoint()->equal(otherNode->getPoint())
            && getShape()->equal(otherNode->getShape());
    }
    return false;
}

std::string STWithinExpressionNode::toString() const {
    std::stringstream ss;
    ss << "ST_WITHIN(" << children[0]->toString() << ", " << children[1]->toString() << ")";
    return ss.str();
}

void STWithinExpressionNode::setChildren(ExpressionNodePtr const& point,
                                         ShapeExpressionNodePtr const& shapeExpression) {
    addChildWithEqual(point);
    addChildWithEqual(shapeExpression);
}

ExpressionNodePtr STWithinExpressionNode::getPoint() const {
    if (children.size() != 2) {
        NES_FATAL_ERROR("An STWithin Expression should always have two children, but it has: " << children.size());
    }
    return children[0]->as<GeographyFieldsAccessExpressionNode>();
}

ShapeExpressionNodePtr STWithinExpressionNode::getShape() const {
    if (children.size() != 2) {
        NES_FATAL_ERROR("An STWithin Expression should always have two children, but it has: " << children.size());
    }
    return children[1]->as<ShapeExpressionNode>();
}

void STWithinExpressionNode::inferStamp(SchemaPtr schema) {
    // infer the stamps of the left and right child
    auto point = getPoint();
    auto shape = getShape();
    point->inferStamp(schema);
    auto shapeType = shape->getShapeType();
    auto validShape = (shapeType == ShapeType::Rectangle) || (shapeType == ShapeType::Polygon);

    // both sub expressions have to be numerical
    if (!point->getStamp()->isFloat() || !validShape) {
        throw std::logic_error(
            "STWithinExpressionNode: Error during stamp inference. AccessType need to be Float and ShapeType needs to be"
            " Polygon or Rectangle but the Point access type was: " + point->getStamp()->toString() + ", and the shape was: "
            + shape->toString());
    }

    stamp = DataTypeFactory::createBoolean();
    NES_TRACE("STWithinExpressionNode: The following stamp was assigned: " << toString());
}

ExpressionNodePtr STWithinExpressionNode::copy() {
    return std::make_shared<STWithinExpressionNode>(STWithinExpressionNode(this));
}

}// namespace NES