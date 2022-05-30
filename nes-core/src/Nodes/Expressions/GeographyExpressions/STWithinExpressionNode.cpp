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
STWithinExpressionNode::STWithinExpressionNode()
    : ExpressionNode(DataTypeFactory::createBoolean()) {}

STWithinExpressionNode::STWithinExpressionNode(STWithinExpressionNode* other) : ExpressionNode(other) {
    addChildWithEqual(getPoint()->copy());
    addChildWithEqual(getWKT()->copy());
}

ExpressionNodePtr STWithinExpressionNode::create(const GeographyFieldsAccessExpressionNodePtr& point,
                                                 const ConstantValueExpressionNodePtr& wkt) {
    auto stWithinNode = std::make_shared<STWithinExpressionNode>();
    stWithinNode->setChildren(point, wkt);
    return stWithinNode;
}

bool STWithinExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<STWithinExpressionNode>()) {
        auto otherAndNode = rhs->as<STWithinExpressionNode>();
        return getPoint()->equal(otherAndNode->getPoint()) && getWKT()->equal(otherAndNode->getWKT());
    }
    return false;
}

std::string STWithinExpressionNode::toString() const {
    std::stringstream ss;
    ss << "ST_WITHIN(" << children[0]->toString() << ", " << children[1]->toString() << ")";
    return ss.str();
}

void STWithinExpressionNode::setChildren(ExpressionNodePtr const& point, ExpressionNodePtr const& wkt) {
    addChildWithEqual(point);
    addChildWithEqual(wkt);
}

ExpressionNodePtr STWithinExpressionNode::getPoint() const {
    if (children.size() != 2) {
        NES_FATAL_ERROR("An STWithin Expression should always have two children, but it has: " << children.size());
    }
    return children[0]->as<GeographyFieldsAccessExpressionNode>();
}

ExpressionNodePtr STWithinExpressionNode::getWKT() const {
    if (children.size() != 2) {
        NES_FATAL_ERROR("An STWithin Expression should always have two children, but it has: " << children.size());
    }
    return children[1]->as<ConstantValueExpressionNode>();
}

void STWithinExpressionNode::inferStamp(SchemaPtr schema) {
    // infer the stamps of the left and right child
    auto left = getPoint();
    auto right = getWKT();
    left->inferStamp(schema);
    right->inferStamp(schema);

    // both sub expressions have to be numerical
    if (!left->getStamp()->isFloat() || !right->getStamp()->isCharArray()) {
        throw std::logic_error(
            "STWithinExpressionNode: Error during stamp inference. Types need to be Float and Text but Left was:"
            + left->getStamp()->toString() + " Right was: " + right->getStamp()->toString());
    }

    stamp = DataTypeFactory::createBoolean();
    NES_TRACE("STWithinExpressionNode: The following stamp was assigned: " << toString());
}

ExpressionNodePtr STWithinExpressionNode::copy() {
    return std::make_shared<STWithinExpressionNode>(STWithinExpressionNode(this));
}

}// namespace NES