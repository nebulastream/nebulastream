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

#ifndef NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_SHAPEEXPRESSIONS_CIRCLEEXPRESSIONNODE_HPP_
#define NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_SHAPEEXPRESSIONS_CIRCLEEXPRESSIONNODE_HPP_

#include <Operators/Expressions/GeographyExpressions/ShapeExpressions/ShapeExpressionNode.hpp>
#include <Nodes/Node.hpp>

namespace NES {

class CircleExpressionNode;
using CircleExpressionNodePtr = std::shared_ptr<CircleExpressionNode>;

/**
 * @brief A circle expression represents a circle node which consists of a point
 * (consisting of a latitude/longitude) which is the center of the circle and a
 * radius of the circle defined in meters.
 *
 *               *  *
 *           *          *
 *         *      radius  *
 *         *       o------*
 *         *    (lat,lon) *
 *           *          *
 *               *  *
 *
 */
class CircleExpressionNode : public ShapeExpressionNode {
  public:
    explicit CircleExpressionNode(CircleExpressionNode* other);
    explicit CircleExpressionNode(double latitude, double longitude, double radius);
    ~CircleExpressionNode() = default;

    /**
     * @brief Creates a new Circle expression node.
     * @param latitude is the latitude of the center of the circle.
     * @param longitude is the longitude of the center of the circle.
     * @param radius represents the radius of the circle in meters.
     */
    static ShapeExpressionNodePtr create(double latitude, double longitude, double radius);

    [[nodiscard]] bool equal(NodePtr const& rhs) const final;
    [[nodiscard]] std::string toString() const override;

    /**
     * @brief gets the latitude.
     */
    double getLatitude() const;

    /**
     * @brief gets the longitude.
     */
    double getLongitude() const;

    /**
     * @brief gets the radius.
     */
    double getRadius() const;

    /**
    * @brief Creates a deep copy of this circle node.
    * @return ShapeExpressionNodePtr
    */
    ShapeExpressionNodePtr copy() override;

  private:
    double latitude;
    double longitude;
    double radius;
};
}// namespace NES

#endif// NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_SHAPEEXPRESSIONS_CIRCLEEXPRESSIONNODE_HPP_
