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

#include <Operators/Expressions/GeographyExpressions/ShapeExpressions/PolygonExpressionNode.hpp>
#include <Operators/Expressions/GeographyExpressions/ShapeExpressions/ShapeExpressionNode.hpp>
#include <Nodes/Node.hpp>
#include <iterator>
#include <sstream>

namespace NES {
PolygonExpressionNode::PolygonExpressionNode(PolygonExpressionNode* other) : ShapeExpressionNode(other->type) {
    coords = other->coords;
}

PolygonExpressionNode::PolygonExpressionNode(std::initializer_list<double> coords)
    : ShapeExpressionNode(ShapeType::Polygon), coords(coords) {}

PolygonExpressionNode::PolygonExpressionNode(std::vector<double> coords)
    : ShapeExpressionNode(ShapeType::Polygon), coords(coords) {}

ShapeExpressionNodePtr PolygonExpressionNode::create(std::initializer_list<double> coords) {
    return std::make_shared<PolygonExpressionNode>(coords);
}

ShapeExpressionNodePtr PolygonExpressionNode::create(std::vector<double> coords) {
    return std::make_shared<PolygonExpressionNode>(coords);
}

bool PolygonExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<PolygonExpressionNode>()) {
        auto otherNode = rhs->as<PolygonExpressionNode>();
        return getCoordinates() == otherNode->getCoordinates();
    }
    return false;
}

std::string PolygonExpressionNode::toString() const {
    std::stringstream ss;

    ss << "POLYGON(";
    if (!coords.empty()) {
        std::copy(coords.begin(), coords.end() - 1, std::ostream_iterator<double>(ss, ", "));
        ss << coords.back();
    }
    ss << ")";

    return ss.str();
}

std::vector<double> PolygonExpressionNode::getCoordinates() const { return coords; }

ShapeExpressionNodePtr PolygonExpressionNode::copy() {
    return std::make_shared<PolygonExpressionNode>(PolygonExpressionNode(this));
}

}// namespace NES