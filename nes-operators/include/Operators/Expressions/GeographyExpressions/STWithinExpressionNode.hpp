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

#ifndef NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_STWITHINEXPRESSIONNODE_HPP_
#define NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_STWITHINEXPRESSIONNODE_HPP_

#include <Operators/Expressions/GeographyExpressions/GeographyExpressionNode.hpp>
#include <Operators/Expressions/GeographyExpressions/GeographyFieldsAccessExpressionNode.hpp>
#include <Operators/Expressions/GeographyExpressions/ShapeExpressions/ShapeExpressionNode.hpp>

namespace NES {

class ShapeExpressionNode;
using ShapeExpressionNodePtr = std::shared_ptr<ShapeExpressionNode>;

/**
 * @brief This node represents ST_WITHIN predicate, where ST stands for Spatial Type.
 * ST_WITHIN predicate in general defines the relationship between two objects (i.e.,
 * whether a geometric object is within another geometric object or not). In NES, we
 * only expect the stream to report the GPS coordinates from a source (i.e., a Point).
 * Thus in NES, by using ST_WITHIN expression a user can determine whether a point is
 * within a geometric object or not. For now, the geometric object will be limited to
 * a rectangle. ST_Within is supposed to be used with the filter operator as follows:
 *
 * stream.filter(ST_WITHIN(Attribute("latitude"), Attribute("longitude"), SHAPE))
 *
 * where latitude, and longitude represent the attributes lat/long in the schema, and
 * SHAPE is one of Circle, Rectangle, or Polygon.
 */
class STWithinExpressionNode : public ExpressionNode, public GeographyExpressionNode {
  public:
    STWithinExpressionNode();
    ~STWithinExpressionNode() = default;
    /**
     * @brief Create a new STWithin expression.
     * @param point is the GeographyFieldsAccessExpression which accesses two fields
     * in the schema, the first of which should be the latitude and the second should
     * be the longitude.
     * @param shape represents either a polygon or a rectangle.
     */
    static ExpressionNodePtr create(GeographyFieldsAccessExpressionNodePtr const& point, ShapeExpressionNodePtr const& shape);

    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;

    /**
     * @brief set the children node of this expression.
     */
    void setChildren(ExpressionNodePtr const& point, ShapeExpressionNodePtr const& shape);

    /**
     * @brief gets the point (or the left child).
     */
    ExpressionNodePtr getPoint() const;

    /**
     * @brief gets the shape.
     */
    ShapeExpressionNodePtr getShape() const;

    /**
     * @brief Infers the stamp of the STWITHIN expression node.
     * @param typeInferencePhaseContext
     * @param schema the current schema.
     */
    void inferStamp(SchemaPtr schema) override;

    /**
    * @brief Create a deep copy of this expression node.
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr copy() override;

  protected:
    explicit STWithinExpressionNode(STWithinExpressionNode* other);
};
}// namespace NES

#endif// NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_STWITHINEXPRESSIONNODE_HPP_
