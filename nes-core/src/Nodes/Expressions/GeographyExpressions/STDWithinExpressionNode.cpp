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
    addChildWithEqual(getCircle()->copy());
}

ExpressionNodePtr STDWithinExpressionNode::create(GeographyFieldsAccessExpressionNodePtr const& point,
                                                  ShapeExpressionNodePtr const& shapeExpression) {
    auto stDWithinNode = std::make_shared<STDWithinExpressionNode>();
    stDWithinNode->setChildren(point, shapeExpression);
    return stDWithinNode;
}

bool STDWithinExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<STDWithinExpressionNode>()) {
        auto otherAndNode = rhs->as<STDWithinExpressionNode>();
        return getPoint()->equal(otherAndNode->getPoint()) && getCircle()->equal(otherAndNode->getCircle());
    }
    return false;
}

std::string STDWithinExpressionNode::toString() const {
    std::stringstream ss;
    ss << "ST_DWITHIN(" << children[0]->toString() << ", " << children[1]->toString() << ")";
    return ss.str();
}

void STDWithinExpressionNode::setChildren(ExpressionNodePtr const& point, ShapeExpressionNodePtr const& circle) {
    addChildWithEqual(point);
    addChildWithEqual(circle);
}

ExpressionNodePtr STDWithinExpressionNode::getPoint() const {
    if (children.size() != 2) {
        NES_FATAL_ERROR("An ST_DWithin Expression should always have two children, but it has: " << children.size());
    }
    return children[0]->as<GeographyFieldsAccessExpressionNode>();
}

ShapeExpressionNodePtr STDWithinExpressionNode::getCircle() const {
    if (children.size() != 2) {
        NES_FATAL_ERROR("An ST_DWithin Expression should always have two children, but it has: " << children.size());
    }
    return children[1]->as<CircleExpressionNode>();
}

void STDWithinExpressionNode::inferStamp(const Optimizer::TypeInferencePhaseContext& typeInferencePhaseContext,
                                         SchemaPtr schema) {
    // infer the stamps of the left and right child
    auto point = getPoint();
    auto circle = getCircle();
    point->inferStamp(typeInferencePhaseContext, schema);
    auto shapeType = circle->getShapeType();

    if (!point->getStamp()->isFloat() || shapeType != Circle) {
        throw std::logic_error(
            "ST_DWithinExpressionNode: Error during type inference. AccessTypes need to be Float and shape type needs"
            "to be Circle but Point was:"
            + point->getStamp()->toString() + ", shape was: " + circle->toString());
    }

    stamp = DataTypeFactory::createBoolean();
    NES_TRACE("ST_DWithinExpressionNode: The following stamp was assigned: " << toString());
}

ExpressionNodePtr STDWithinExpressionNode::copy() {
    return std::make_shared<STDWithinExpressionNode>(STDWithinExpressionNode(this));
}

}// namespace NES