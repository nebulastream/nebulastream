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

#ifndef NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_SHAPEEXPRESSIONS_POLYGONEXPRESSIONNODE_HPP_
#define NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_SHAPEEXPRESSIONS_POLYGONEXPRESSIONNODE_HPP_

#include <Operators/Expressions/GeographyExpressions/ShapeExpressions/ShapeExpressionNode.hpp>
#include <Nodes/Node.hpp>

namespace NES {

class PolygonExpressionNode;
using PolygonExpressionNodePtr = std::shared_ptr<PolygonExpressionNode>;

/**
 * @brief A polygon expression represents a polygon node which consists of n number
 * of latitude, longitude coordinate pair of points.
 */
class PolygonExpressionNode : public ShapeExpressionNode {
  public:
    explicit PolygonExpressionNode(PolygonExpressionNode* other);
    explicit PolygonExpressionNode(std::initializer_list<double> coords);
    explicit PolygonExpressionNode(std::vector<double> coords);
    ~PolygonExpressionNode() = default;

    /**
     * @brief Creates a new polygon expression node.
     * @param coords are the coordinates of the polygon.
     */
    static ShapeExpressionNodePtr create(std::initializer_list<double> coords);

    /**
     * @brief Creates a new polygon expression node.
     * @param coords are the coordinates of the polygon.
     */
    static ShapeExpressionNodePtr create(std::vector<double> coords);

    [[nodiscard]] bool equal(NodePtr const& rhs) const final;
    [[nodiscard]] std::string toString() const override;

    /**
     * @brief gets the coordinates.
     */
    std::vector<double> getCoordinates() const;

    /**
    * @brief Creates a deep copy of this circle node.
    * @return ShapeExpressionNodePtr
    */
    ShapeExpressionNodePtr copy() override;

  private:
    std::vector<double> coords;
};
}// namespace NES

#endif// NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_SHAPEEXPRESSIONS_POLYGONEXPRESSIONNODE_HPP_
