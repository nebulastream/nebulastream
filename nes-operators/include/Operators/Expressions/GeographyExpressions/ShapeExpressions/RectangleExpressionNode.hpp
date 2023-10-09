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

#ifndef NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_SHAPEEXPRESSIONS_RECTANGLEEXPRESSIONNODE_HPP_
#define NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_SHAPEEXPRESSIONS_RECTANGLEEXPRESSIONNODE_HPP_

#include <Operators/Expressions/GeographyExpressions/ShapeExpressions/ShapeExpressionNode.hpp>
#include <Nodes/Node.hpp>

namespace NES {

class RectangleExpressionNode;
using RectangleExpressionNodePtr = std::shared_ptr<RectangleExpressionNode>;

/**
 * @brief A rectangle expression represents a rectangle node, boundaries of which
 * is defined by two points, a south-west (lower-left) point and a north-east
 * (upper-right) point.
 *
 *                           o-----------------o north-east (latitudeHigh,
 *                           |                 |             longitudeHigh)
 *                           |                 |
 * south-west (latitudeLow,  o-----------------o
 *             longitudeLow)
 */
class RectangleExpressionNode : public ShapeExpressionNode {
  public:
    explicit RectangleExpressionNode(RectangleExpressionNode* other);
    explicit RectangleExpressionNode(double latitudeLow, double longitudeLow, double latitudeHigh, double longitudeHigh);
    ~RectangleExpressionNode() = default;

    /**
     * @brief Creates a new Rectangle expression node.
     * @param latitudeLow is the latitude value of south-west point of the rectangle.
     * @param longitudeLow is the longitude value of south-west point of the rectangle.
     * @param latitudeHigh is the latitude value of north-east point of the rectangle.
     * @param longitudeHigh is the longitude value of north-east point of the rectangle.
     */
    static ShapeExpressionNodePtr create(double latitudeLow, double longitudeLow, double latitudeHigh, double longitudeHigh);

    [[nodiscard]] bool equal(NodePtr const& rhs) const final;
    [[nodiscard]] std::string toString() const override;

    /**
     * @brief gets the value of latitudeLow.
     */
    double getLatitudeLow() const;

    /**
     * @brief gets the value of longitudeLow.
     */
    double getLongitudeLow() const;

    /**
     * @brief gets the value of latitudeHigh.
     */
    double getLatitudeHigh() const;

    /**
     * @brief gets the value of longitudeHigh.
     */
    double getLongitudeHigh() const;

    /**
    * @brief Creates a deep copy of this circle node.
    * @return ShapeExpressionNodePtr
    */
    ShapeExpressionNodePtr copy() override;

  private:
    double latitudeLow;
    double longitudeLow;
    double latitudeHigh;
    double longitudeHigh;
};
}// namespace NES

#endif// NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_SHAPEEXPRESSIONS_RECTANGLEEXPRESSIONNODE_HPP_
