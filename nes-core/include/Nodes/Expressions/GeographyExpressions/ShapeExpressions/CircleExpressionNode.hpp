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

#ifndef NES_NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_SHAPEEXPRESSIONS_CIRCLEEXPRESSIONNODE_HPP_
#define NES_NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_SHAPEEXPRESSIONS_CIRCLEEXPRESSIONNODE_HPP_

#include <Nodes/Node.hpp>
#include <Nodes/Expressions/GeographyExpressions/ShapeExpressions/ShapeExpressionNode.hpp>

namespace NES {

class CircleExpressionNode;
using CircleExpressionNodePtr = std::shared_ptr<CircleExpressionNode>;

class CircleExpressionNode : public ShapeExpressionNode {
  public:
    explicit CircleExpressionNode(CircleExpressionNode* other);
    explicit CircleExpressionNode(double latitude,
                                  double longitude,
                                  double distance);
    ~CircleExpressionNode() = default;

    /**
     * @brief Creates a new Circle expression node.
     * @param latitude is the latitude
     * @param longitude is the longitude
     * @param distance represents the distance.
     */
    static ShapeExpressionNodePtr create(double latitude,
                                         double longitude,
                                         double distance);

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
     * @brief gets the distance.
     */
    double getDistance() const;

    /**
    * @brief Creates a deep copy of this circle node.
    * @return ShapeExpressionNodePtr
    */
    ShapeExpressionNodePtr copy() override;

  private:

    double latitude;
    double longitude;
    double distance;
};
}// namespace NES

#endif//NES_NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_SHAPEEXPRESSIONS_CIRCLEEXPRESSIONNODE_HPP_