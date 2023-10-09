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

#include <Operators/Expressions/GeographyExpressions/ShapeExpressions/CircleExpressionNode.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {
CircleExpressionNode::CircleExpressionNode(CircleExpressionNode* other) : ShapeExpressionNode(other->type) {
    latitude = other->getLatitude();
    longitude = other->getLongitude();
    radius = other->getRadius();
}

CircleExpressionNode::CircleExpressionNode(double latitude, double longitude, double radius)
    : ShapeExpressionNode(ShapeType::Circle), latitude(latitude), longitude(longitude), radius(radius) {}

ShapeExpressionNodePtr CircleExpressionNode::create(double latitude, double longitude, double radius) {
    auto circleNode = std::make_shared<CircleExpressionNode>(latitude, longitude, radius);
    return circleNode;
}

bool CircleExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<CircleExpressionNode>()) {
        auto otherNode = rhs->as<CircleExpressionNode>();
        return getLatitude() == otherNode->getLatitude() && getLongitude() == otherNode->getLongitude()
            && getRadius() == otherNode->getRadius();
    }
    return false;
}

std::string CircleExpressionNode::toString() const {
    std::stringstream ss;
    ss << "CIRCLE(lat: " << latitude << ", lon: " << longitude << ", radius: " << radius << ")";
    return ss.str();
}

double CircleExpressionNode::getLatitude() const { return latitude; }

double CircleExpressionNode::getLongitude() const { return longitude; }

double CircleExpressionNode::getRadius() const { return radius; }

ShapeExpressionNodePtr CircleExpressionNode::copy() { return std::make_shared<CircleExpressionNode>(CircleExpressionNode(this)); }

}// namespace NES