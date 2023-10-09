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

#include <Operators/Expressions/GeographyExpressions/ShapeExpressions/RectangleExpressionNode.hpp>
#include <Operators/Expressions/GeographyExpressions/ShapeExpressions/ShapeExpressionNode.hpp>
#include <Nodes/Node.hpp>
#include <sstream>

namespace NES {
RectangleExpressionNode::RectangleExpressionNode(RectangleExpressionNode* other) : ShapeExpressionNode(other->type) {
    latitudeLow = other->getLatitudeLow();
    longitudeLow = other->getLongitudeLow();
    latitudeHigh = other->getLatitudeHigh();
    longitudeHigh = other->getLongitudeHigh();
}

RectangleExpressionNode::RectangleExpressionNode(double latitudeLow,
                                                 double longitudeLow,
                                                 double latitudeHigh,
                                                 double longitudeHigh)
    : ShapeExpressionNode(ShapeType::Rectangle), latitudeLow(latitudeLow), longitudeLow(longitudeLow), latitudeHigh(latitudeHigh),
      longitudeHigh(longitudeHigh) {}

ShapeExpressionNodePtr
RectangleExpressionNode::create(double latitudeLow, double longitudeLow, double latitudeHigh, double longitudeHigh) {
    auto rectangleNode = std::make_shared<RectangleExpressionNode>(latitudeLow, longitudeLow, latitudeHigh, longitudeHigh);
    return rectangleNode;
}

bool RectangleExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<RectangleExpressionNode>()) {
        auto otherNode = rhs->as<RectangleExpressionNode>();
        return getLatitudeLow() == otherNode->getLatitudeLow() && getLongitudeLow() == otherNode->getLongitudeLow()
            && getLatitudeHigh() == otherNode->getLatitudeHigh() && getLongitudeHigh() == otherNode->getLongitudeHigh();
    }
    return false;
}

std::string RectangleExpressionNode::toString() const {
    std::stringstream ss;
    ss << "RECTANGLE(lat_low: " << latitudeLow << ", lon_low: " << longitudeLow << ", lat_high: " << latitudeHigh
       << ", lon_high: " << longitudeHigh << ")";
    return ss.str();
}

double RectangleExpressionNode::getLatitudeLow() const { return latitudeLow; }

double RectangleExpressionNode::getLongitudeLow() const { return longitudeLow; }

double RectangleExpressionNode::getLatitudeHigh() const { return latitudeHigh; }

double RectangleExpressionNode::getLongitudeHigh() const { return longitudeHigh; }

ShapeExpressionNodePtr RectangleExpressionNode::copy() {
    return std::make_shared<RectangleExpressionNode>(RectangleExpressionNode(this));
}

}// namespace NES