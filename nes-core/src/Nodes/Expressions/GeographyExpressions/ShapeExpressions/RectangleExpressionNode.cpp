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

#include <Nodes/Node.hpp>
#include <Nodes/Expressions/GeographyExpressions/ShapeExpressions/ShapeExpressionNode.hpp>
#include <Nodes/Expressions/GeographyExpressions/ShapeExpressions/RectangleExpressionNode.hpp>

namespace NES {
RectangleExpressionNode::RectangleExpressionNode(RectangleExpressionNode* other)
    : ShapeExpressionNode(other->type) {
    latitude_low = other->getLongitudeLow();
    longitude_low = other->getLongitudeLow();
    latitude_high = other->getLongitudeHigh();
    longitude_high = other->getLongitudeHigh();
}

RectangleExpressionNode::RectangleExpressionNode(double latitude_low,
                                                 double longitude_low,
                                                 double latitude_high,
                                                 double longitude_high)
    : ShapeExpressionNode(ShapeType::Rectangle),
      latitude_low(latitude_low),
      longitude_low(longitude_low),
      latitude_high(latitude_high),
      longitude_high(longitude_high) {}

ShapeExpressionNodePtr RectangleExpressionNode::create(double latitude_low,
                                                       double longitude_low,
                                                       double latitude_high,
                                                       double longitude_high) {
    auto rectangleNode = std::make_shared<RectangleExpressionNode>(latitude_low,
                                                                   longitude_low,
                                                                   latitude_high,
                                                                   longitude_high);
    return rectangleNode;
}

bool RectangleExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<RectangleExpressionNode>()) {
        auto otherNode = rhs->as<RectangleExpressionNode>();
        return getLatitudeLow() == otherNode->getLatitudeLow()
            && getLongitudeLow() == otherNode->getLongitudeLow()
            && getLatitudeHigh() == otherNode->getLatitudeHigh()
            && getLongitudeHigh() == otherNode->getLongitudeHigh();
    }
    return false;
}

std::string RectangleExpressionNode::toString() const {
    std::stringstream ss;
    ss << "RECTANGLE(lat_low: " << latitude_low << ", lon_low: " << longitude_low
       << ", lat_high: " << latitude_high << ", lon_high: " << longitude_high << ")";
    return ss.str();
}

double RectangleExpressionNode::getLatitudeLow() const { return latitude_low; }

double RectangleExpressionNode::getLongitudeLow() const { return longitude_low; }

double RectangleExpressionNode::getLatitudeHigh() const { return latitude_high; }

double RectangleExpressionNode::getLongitudeHigh() const { return longitude_high; }

ShapeExpressionNodePtr RectangleExpressionNode::copy() {
    return std::make_shared<RectangleExpressionNode>(RectangleExpressionNode(this));
}

}// namespace NES