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

#include <Operators/Expressions/GeographyExpressions/ShapeExpressions/PointExpressionNode.hpp>
#include <Operators/Expressions/GeographyExpressions/ShapeExpressions/ShapeExpressionNode.hpp>
#include <Nodes/Node.hpp>
#include <sstream>

namespace NES {
PointExpressionNode::PointExpressionNode(PointExpressionNode* other) : ShapeExpressionNode(other->type) {
    latitude = other->getLatitude();
    longitude = other->getLongitude();
}

PointExpressionNode::PointExpressionNode(double latitude, double longitude)
    : ShapeExpressionNode(ShapeType::Point), latitude(latitude), longitude(longitude) {}

ShapeExpressionNodePtr PointExpressionNode::create(double latitude, double longitude) {
    auto pointNode = std::make_shared<PointExpressionNode>(latitude, longitude);
    return pointNode;
}

bool PointExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<PointExpressionNode>()) {
        auto otherNode = rhs->as<PointExpressionNode>();
        return getLatitude() == otherNode->getLatitude() && getLongitude() == otherNode->getLongitude();
    }
    return false;
}

std::string PointExpressionNode::toString() const {
    std::stringstream ss;
    ss << "POINT(lat: " << latitude << ", lon: " << longitude << ")";
    return ss.str();
}

double PointExpressionNode::getLatitude() const { return latitude; }

double PointExpressionNode::getLongitude() const { return longitude; }

ShapeExpressionNodePtr PointExpressionNode::copy() { return std::make_shared<PointExpressionNode>(PointExpressionNode(this)); }

}// namespace NES