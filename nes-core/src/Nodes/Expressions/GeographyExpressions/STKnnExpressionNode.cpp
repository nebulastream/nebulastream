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
STKnnExpressionNode::STKnnExpressionNode()
    : ExpressionNode(DataTypeFactory::createBoolean()) {}

STKnnExpressionNode::STKnnExpressionNode(STKnnExpressionNode* other) : ExpressionNode(other) {
    addChildWithEqual(getPoint()->copy());
    addChildWithEqual(getWKT()->copy());
    addChildWithEqual(getK()->copy());
}

ExpressionNodePtr STKnnExpressionNode::create(const GeographyFieldsAccessExpressionNodePtr& point,
                                                  const ConstantValueExpressionNodePtr& wkt,
                                                  const ConstantValueExpressionNodePtr& k) {
    auto stKnnNode = std::make_shared<STKnnExpressionNode>();
    stKnnNode->setChildren(point, wkt, k);
    return stKnnNode;
}

bool STKnnExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<STKnnExpressionNode>()) {
        auto otherAndNode = rhs->as<STKnnExpressionNode>();
        return getPoint()->equal(otherAndNode->getPoint())
            && getWKT()->equal(otherAndNode->getWKT())
            && getK()->equal(otherAndNode->getK());
    }
    return false;
}

std::string STKnnExpressionNode::toString() const {
    std::stringstream ss;
    ss << "ST_Knn(" << children[0]->toString() << ", " << children[1]->toString() << ", " << children[2]->toString() << ")";
    return ss.str();
}

void STKnnExpressionNode::setChildren(ExpressionNodePtr const& point,
                                          ExpressionNodePtr const& wkt,
                                          ExpressionNodePtr const& k) {
    addChildWithEqual(point);
    addChildWithEqual(wkt);
    addChildWithEqual(k);
}

ExpressionNodePtr STKnnExpressionNode::getPoint() const {
    if (children.size() != 3) {
        NES_FATAL_ERROR("An ST_Knn Expression should always have three children, but it has: " << children.size());
    }
    return children[0]->as<GeographyFieldsAccessExpressionNode>();
}

ExpressionNodePtr STKnnExpressionNode::getWKT() const {
    if (children.size() != 3) {
        NES_FATAL_ERROR("An ST_Knn Expression should always have three children, but it has: " << children.size());
    }
    return children[1]->as<ConstantValueExpressionNode>();
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
    auto wkt = getWKT();
    auto k = getK();
    point->inferStamp(schema);
    wkt->inferStamp(schema);
    k->inferStamp(schema);

    // check sub expressions if they are the correct type
    if (!point->getStamp()->isFloat() || !wkt->getStamp()->isCharArray() || !k->getStamp()->isNumeric()) {
        throw std::logic_error(
            "ST_KnnExpressionNode: Error during stamp inference. Types need to be Float and Text but Point was:"
            + point->getStamp()->toString() + ", WKT was: " + wkt->getStamp()->toString() + ", and the k was: "
            + k->getStamp()->toString());
    }

    stamp = DataTypeFactory::createBoolean();
    NES_TRACE("ST_KnnExpressionNode: The following stamp was assigned: " << toString());
}

ExpressionNodePtr STKnnExpressionNode::copy() {
    return std::make_shared<STKnnExpressionNode>(STKnnExpressionNode(this));
}

}// namespace NES