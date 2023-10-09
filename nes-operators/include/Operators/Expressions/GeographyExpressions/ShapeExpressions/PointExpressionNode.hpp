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

#ifndef NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_SHAPEEXPRESSIONS_POINTEXPRESSIONNODE_HPP_
#define NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_SHAPEEXPRESSIONS_POINTEXPRESSIONNODE_HPP_

#include <Operators/Expressions/GeographyExpressions/ShapeExpressions/ShapeExpressionNode.hpp>
#include <Nodes/Node.hpp>

namespace NES {

class PointExpressionNode;
using PointExpressionNodePtr = std::shared_ptr<PointExpressionNode>;

/**
 * @brief A point expression represents a point node which consists of a
 * latitude, longitude coordinate pair.
 */
class PointExpressionNode : public ShapeExpressionNode {
  public:
    explicit PointExpressionNode(PointExpressionNode* other);
    explicit PointExpressionNode(double latitude, double longitude);
    ~PointExpressionNode() = default;

    /**
     * @brief Creates a new Point expression node.
     * @param latitude is the latitude.
     * @param longitude is the longitude.
     */
    static ShapeExpressionNodePtr create(double latitude, double longitude);

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
    * @brief Creates a deep copy of this circle node.
    * @return ShapeExpressionNodePtr
    */
    ShapeExpressionNodePtr copy() override;

  private:
    double latitude;
    double longitude;
};
}// namespace NES

#endif// NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_SHAPEEXPRESSIONS_POINTEXPRESSIONNODE_HPP_
