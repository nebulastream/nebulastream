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
#include <Nodes/Expressions/GeographyExpressions/STKnnExpressionNode.hpp>

namespace NES {
STKnnExpressionNode::STKnnExpressionNode() : ExpressionNode(DataTypeFactory::createBoolean()) {}

STKnnExpressionNode::STKnnExpressionNode(STKnnExpressionNode* other) : ExpressionNode(other) {
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
        return getPoint()->equal(otherNode->getPoint())
            && getQueryPoint()->equal(otherNode->getQueryPoint())
            && getK()->equal(otherNode->getK());
    }
    return false;
}

std::string STKnnExpressionNode::toString() const {
    std::stringstream ss;
    ss << "ST_KNN(" << children[0]->toString() << ", " << children[1]->toString() << ", " << children[2]->toString() << ")";
    return ss.str();
}

void STKnnExpressionNode::setChildren(ExpressionNodePtr const& point,
                                      ShapeExpressionNodePtr const& queryPoint,
                                      ExpressionNodePtr const& k) {
    addChildWithEqual(point);
    addChildWithEqual(queryPoint);
    addChildWithEqual(k);
}

ExpressionNodePtr STKnnExpressionNode::getPoint() const {
    if (children.size() != 3) {
        NES_FATAL_ERROR("An ST_Knn Expression should always have three children, but it has: " << children.size());
    }
    return children[0]->as<GeographyFieldsAccessExpressionNode>();
}

ShapeExpressionNodePtr STKnnExpressionNode::getQueryPoint() const {
    if (children.size() != 3) {
        NES_FATAL_ERROR("An ST_Knn Expression should always have three children, but it has: " << children.size());
    }
    return children[1]->as<ShapeExpressionNode>();
}

ExpressionNodePtr STKnnExpressionNode::getK() const {
    if (children.size() != 3) {
        NES_FATAL_ERROR("An ST_Knn Expression should always have three children, but it has: " << children.size());
    }
    return children[2]->as<ConstantValueExpressionNode>();
}

void STKnnExpressionNode::inferStamp(SchemaPtr schema) {
    // infer the stamps of the left and right child
    auto point = getPoint();
    auto queryPoint = getQueryPoint();
    auto k = getK();
    point->inferStamp(schema);
    auto shapeType = queryPoint->getShapeType();
    k->inferStamp(schema);

    // check sub expressions if they are the correct type
    if (!point->getStamp()->isFloat() || shapeType != Point || !k->getStamp()->isNumeric()) {
        throw std::logic_error(
            "ST_KnnExpressionNode: Error during stamp inference. Types need to be Float, ShapeType should be Point,"
            "and K should numeric but Point was: " + point->getStamp()->toString() + ", QueryPoint was: "
            + queryPoint->toString() + ", and the k was: " + k->getStamp()->toString());
    }

    stamp = DataTypeFactory::createBoolean();
    NES_TRACE("ST_KnnExpressionNode: The following stamp was assigned: " << toString());
}

ExpressionNodePtr STKnnExpressionNode::copy() {
    return std::make_shared<STKnnExpressionNode>(STKnnExpressionNode(this));
}

}// namespace NES