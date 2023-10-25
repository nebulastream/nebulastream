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
#include <Util/Logger/Logger.hpp>
#include <memory>

namespace NES {
GeographyFieldsAccessExpressionNode::GeographyFieldsAccessExpressionNode(DataTypePtr stamp)
    : ExpressionNode(std::move(stamp)), GeographyExpressionNode(){};

GeographyFieldsAccessExpressionNode::GeographyFieldsAccessExpressionNode(const FieldAccessExpressionNodePtr& latitude,
                                                                         const FieldAccessExpressionNodePtr& longitude)
    : ExpressionNode(DataTypeFactory::createDouble()), GeographyExpressionNode() {
    addChildWithEqual(latitude->copy());
    addChildWithEqual(longitude->copy());
}

GeographyFieldsAccessExpressionNode::GeographyFieldsAccessExpressionNode(GeographyFieldsAccessExpressionNode* other)
    : ExpressionNode(other), GeographyExpressionNode() {
    auto lat = other->getLatitude()->copy();
    auto lng = other->getLongitude()->copy();
    setChildren(lat, lng);
}

ExpressionNodePtr GeographyFieldsAccessExpressionNode::create(const FieldAccessExpressionNodePtr& latitude,
                                                              const FieldAccessExpressionNodePtr& longitude) {
    auto geographyFieldAccessExpressionNode = std::make_shared<GeographyFieldsAccessExpressionNode>(latitude->getStamp());
    geographyFieldAccessExpressionNode->setChildren(latitude, longitude);
    return geographyFieldAccessExpressionNode;
}

bool GeographyFieldsAccessExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<GeographyFieldsAccessExpressionNode>()) {
        auto otherGeographyFieldsAccess = rhs->as<GeographyFieldsAccessExpressionNode>();
        return getLatitude()->equal(otherGeographyFieldsAccess->getLatitude())
            && getLongitude()->equal(otherGeographyFieldsAccess->getLongitude());
    }
    return false;
}

std::string GeographyFieldsAccessExpressionNode::toString() const {
    if (children.size() != 2) {
        NES_FATAL_ERROR("A geography access expression should always access two fields, but it had: {}", children.size());
        throw InvalidArgumentException(
            "Invalid field access in GeographyFieldsAccessExpressionNode::toString(): children.size() = ",
            std::to_string(children.size()));
    }
    std::stringstream ss;
    ss << "Geography(" << children[0]->toString() << ", " << children[1]->toString() << ")";
    return ss.str();
}

void GeographyFieldsAccessExpressionNode::setChildren(ExpressionNodePtr const& latitude, ExpressionNodePtr const& longitude) {
    addChildWithEqual(latitude);
    addChildWithEqual(longitude);
}

ExpressionNodePtr GeographyFieldsAccessExpressionNode::getLatitude() const {
    if (children.size() != 2) {
        NES_FATAL_ERROR("A geography access expression should always access two fields, but it had: {}", children.size());
        throw InvalidArgumentException(
            "Invalid field access in GeographyFieldsAccessExpressionNode::getLatitude(): children.size() = ",
            std::to_string(children.size()));
    }
    return children[0]->as<FieldAccessExpressionNode>();
}

ExpressionNodePtr GeographyFieldsAccessExpressionNode::getLongitude() const {
    if (children.size() != 2) {
        NES_FATAL_ERROR("A geography access expression should always access two fields, but it had: {}", children.size());
        throw InvalidArgumentException(
            "Invalid field access in GeographyFieldsAccessExpressionNode::getLongitude(): children.size() = ",
            std::to_string(children.size()));
    }
    return children[1]->as<FieldAccessExpressionNode>();
}

void GeographyFieldsAccessExpressionNode::inferStamp(SchemaPtr schema) {
    // infer the stamps of the left and right child
    auto left = getLatitude();
    auto right = getLongitude();
    left->inferStamp(schema);
    right->inferStamp(schema);

    // both sub expressions have to be of type float
    if (!left->getStamp()->isFloat() || !right->getStamp()->isFloat()) {
        throw std::logic_error(
            "GeographyFieldsAccessExpressionNode: Error during stamp inference. Types need to be Float but Left was:"
            + left->getStamp()->toString() + " Right was: " + right->getStamp()->toString());
    }

    stamp = DataTypeFactory::createFloat();
    NES_TRACE("GeographyFieldsAccessExpressionNode: The following stamp was assigned: {}", toString());
}

ExpressionNodePtr GeographyFieldsAccessExpressionNode::copy() {
    // copy the field access expression nodes
    auto latitudeExpressionNode = getLatitude()->copy();
    auto longitudeExpressionNode = getLongitude()->copy();
    auto latitudeFieldAccessExpressionNode = latitudeExpressionNode->as<FieldAccessExpressionNode>();
    auto longitudeFieldAccessExpressionNode = longitudeExpressionNode->as<FieldAccessExpressionNode>();
    return create(latitudeFieldAccessExpressionNode, longitudeFieldAccessExpressionNode);
}

}// namespace NES