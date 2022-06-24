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

#ifndef NES_NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_SHAPEEXPRESSIONS_RECTANGLEEXPRESSIONNODE_HPP_
#define NES_NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_SHAPEEXPRESSIONS_RECTANGLEEXPRESSIONNODE_HPP_

#include <Nodes/Node.hpp>
#include <Nodes/Expressions/GeographyExpressions/ShapeExpressions/ShapeExpressionNode.hpp>

namespace NES {

class RectangleExpressionNode;
using RectangleExpressionNodePtr = std::shared_ptr<RectangleExpressionNode>;

class RectangleExpressionNode : public ShapeExpressionNode {
  public:
    explicit RectangleExpressionNode(RectangleExpressionNode* other);
    explicit RectangleExpressionNode(double latitude_low,
                                     double longitude_low,
                                     double latitude_high,
                                     double longitude_high);
    ~RectangleExpressionNode() = default;

    /**
     * @brief Creates a new Rectangle expression node.
     * @param latitude_low is the latitude value of south-west point of the rectangle.
     * @param longitude_low is the longitude value of south-west point of the rectangle.
     * @param latitude_high is the latitude value of north-east point of the rectangle.
     * @param longitude_high is the longitude value of north-east point of the rectangle.
     */
    static ShapeExpressionNodePtr create(double latitude_low,
                                         double longitude_low,
                                         double latitude_high,
                                         double longitude_high);

    [[nodiscard]] bool equal(NodePtr const& rhs) const final;
    [[nodiscard]] std::string toString() const override;

    /**
     * @brief gets the value of latitude_low.
     */
    double getLatitudeLow() const;

    /**
     * @brief gets the value of longitude_low.
     */
    double getLongitudeLow() const;

    /**
     * @brief gets the value of latitude_high.
     */
    double getLatitudeHigh() const;

    /**
     * @brief gets the value of longitude_high.
     */
    double getLongitudeHigh() const;

    /**
    * @brief Creates a deep copy of this circle node.
    * @return ShapeExpressionNodePtr
    */
    ShapeExpressionNodePtr copy() override;

  private:
    double latitude_low;
    double longitude_low;
    double latitude_high;
    double longitude_high;
};
}// namespace NES

#endif//NES_NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_SHAPEEXPRESSIONS_RECTANGLEEXPRESSIONNODE_HPP_