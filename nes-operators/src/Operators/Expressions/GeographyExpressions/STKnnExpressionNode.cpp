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
#include <Exceptions/InvalidArgumentException.hpp>
#include <Operators/Expressions/GeographyExpressions/GeographyFieldsAccessExpressionNode.hpp>
#include <Operators/Expressions/GeographyExpressions/STKnnExpressionNode.hpp>
#include <Operators/Expressions/GeographyExpressions/ShapeExpressions/PointExpressionNode.hpp>
#include <Util/Logger/Logger.hpp>
namespace NES {
STKnnExpressionNode::STKnnExpressionNode() : ExpressionNode(DataTypeFactory::createBoolean()), GeographyExpressionNode() {}

STKnnExpressionNode::STKnnExpressionNode(STKnnExpressionNode* other) : ExpressionNode(other), GeographyExpressionNode() {
    addChildWithEqual(getPoint()->copy());
    addChildWithEqual(getQueryPoint()->copy());
    addChildWithEqual(getK()->copy());
}

ExpressionNodePtr STKnnExpressionNode::create(const GeographyFieldsAccessExpressionNodePtr& point,
                                              const ShapeExpressionNodePtr& queryPoint,
                                              const ConstantValueExpressionNodePtr& k) {
    auto stKnnNode = std::make_shared<STKnnExpressionNode>();
    stKnnNode->setChildren(point, queryPoint, k);
    return stKnnNode;
}

bool STKnnExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<STKnnExpressionNode>()) {
        auto otherNode = rhs->as<STKnnExpressionNode>();
        return getPoint()->equal(otherNode->getPoint()) && getQueryPoint()->equal(otherNode->getQueryPoint())
            && getK()->equal(otherNode->getK());
    }
    return false;
}

std::string STKnnExpressionNode::toString() const {
    if (children.size() != 3) {
        NES_FATAL_ERROR("A STKnnExpressionNode should always access two children, but it had: {}", children.size());
        throw InvalidArgumentException("Invalid number of children in STKnnExpressionNode::toString(): children.size() = ",
                                       std::to_string(children.size()));
    }
    std::stringstream ss;
    ss << "ST_KNN(" << children[0]->toString() << ", " << children[1]->toString() << ", " << children[2]->toString() << ")";
    return ss.str();
}

void STKnnExpressionNode::setChildren(ExpressionNodePtr const& point,
                                      ShapeExpressionNodePtr const& queryPoint,
                                      ExpressionNodePtr const& k) {
    if (!point->instanceOf<GeographyFieldsAccessExpressionNode>() || !queryPoint->instanceOf<PointExpressionNode>()
        || !k->instanceOf<ConstantValueExpressionNode>()) {
        throw InvalidArgumentException("Invalid arguments in STKnnExpressionNode::setChildren(): ",
                                       "Point is : " + point->toString() + ", shape expression is : " + queryPoint->toString()
                                           + ", and k is : " + k->toString());
    }
    addChildWithEqual(point);
    addChildWithEqual(queryPoint);
    addChildWithEqual(k);
}

ExpressionNodePtr STKnnExpressionNode::getPoint() const {
    if (children.size() != 3) {
        NES_FATAL_ERROR("An ST_Knn Expression should always have three children, but it has: {}", children.size());
        throw InvalidArgumentException("Invalid number of children in STKnnExpressionNode::getPoint(): children.size() = ",
                                       std::to_string(children.size()));
    }
    return children[0]->as<GeographyFieldsAccessExpressionNode>();
}

ShapeExpressionNodePtr STKnnExpressionNode::getQueryPoint() const {
    if (children.size() != 3) {
        NES_FATAL_ERROR("An ST_Knn Expression should always have three children, but it has: {}", children.size());
        throw InvalidArgumentException("Invalid number of children in STKnnExpressionNode::getQueryPoint(): children.size() = ",
                                       std::to_string(children.size()));
    }
    return children[1]->as<ShapeExpressionNode>();
}

ExpressionNodePtr STKnnExpressionNode::getK() const {
    if (children.size() != 3) {
        NES_FATAL_ERROR("An ST_Knn Expression should always have three children, but it has: {}", children.size());
        throw InvalidArgumentException("Invalid number of children in STKnnExpressionNode::getK(): children.size() = ",
                                       std::to_string(children.size()));
    }
    return children[2]->as<ConstantValueExpressionNode>();
}

void STKnnExpressionNode::inferStamp( SchemaPtr schema) {
    // infer the stamps of the left and right child
    auto point = getPoint();
    auto queryPoint = getQueryPoint();
    auto k = getK();
    point->inferStamp( schema);
    auto shapeType = queryPoint->getShapeType();
    k->inferStamp( schema);

    if (!point->getStamp()->isFloat() || shapeType != ShapeType::Point || !k->getStamp()->isNumeric()) {
        throw std::logic_error("ST_KnnExpressionNode: Error during stamp inference. Types need to be Float for AttributeAccess,"
                               "ShapeType should be Point, and K should numeric but Point was: "
                               + point->getStamp()->toString() + ", QueryPoint was: " + queryPoint->toString()
                               + ", and the k was: " + k->getStamp()->toString());
    }

    stamp = DataTypeFactory::createBoolean();
    NES_TRACE("ST_KnnExpressionNode: The following stamp was assigned: {}", toString());
}

ExpressionNodePtr STKnnExpressionNode::copy() {
    auto point = getPoint()->copy();
    auto geographyFieldsAccessExpressionNode = point->as<GeographyFieldsAccessExpressionNode>();
    auto queryPoint = getQueryPoint()->copy();
    auto queryPointExpressionNode = queryPoint->as<PointExpressionNode>();
    auto constant = getK()->copy();
    auto k = constant->as<ConstantValueExpressionNode>();
    return create(geographyFieldsAccessExpressionNode, queryPointExpressionNode, k);
}

}// namespace NES