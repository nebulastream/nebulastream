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

#ifndef NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_STDWITHINEXPRESSIONNODE_HPP_
#define NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_STDWITHINEXPRESSIONNODE_HPP_

#include <Operators/Expressions/GeographyExpressions/GeographyExpressionNode.hpp>
#include <Operators/Expressions/GeographyExpressions/GeographyFieldsAccessExpressionNode.hpp>
#include <Operators/Expressions/GeographyExpressions/ShapeExpressions/CircleExpressionNode.hpp>
#include <Operators/Expressions/GeographyExpressions/ShapeExpressions/ShapeExpressionNode.hpp>

namespace NES {

class ShapeExpressionNode;
using ShapeExpressionNodePtr = std::shared_ptr<ShapeExpressionNode>;

/**
 * @brief This node represents ST_DWithin predicate, where ST stands for Spatial type,
 * and DWithin stands for distance within. In general, the ST_DWITHIN predicate
 * defines whether a geometric object is within distance "d" of another geometric object.
 * In NES, we only expect the stream to report the GPS coordinates from a source (i.e.,
 * a Point). Thus in NES, by using ST_DWITHIN predicate a user can determine whether a
 * point is within distance "d" of another geometric object or not. For now, the geometric
 * object will be limited to another point, and the distance is assumed to be in meters.
 * ST_DWithin is supposed to be used with the filter operator as follows:
 *
 * stream.filter(ST_DWITHIN(Attribute("latitude"), Attribute("longitude"), SHAPE)
*
* where latitude, and longitude represent the attributes lat/long in the stream, and
* SHAPE is a circle. A circle can be defined as follows: CIRCLE(lat, lon, radius)
 * where lat/lon are latitude and longitude of the query point and the radius defines
* the distance (in meters) for the ST_DWITHIN predicate .
 */
class STDWithinExpressionNode : public ExpressionNode, public GeographyExpressionNode {
  public:
    STDWithinExpressionNode();
    ~STDWithinExpressionNode() = default;
    /**
     * @brief Create a new ST_DWithin expression.
     * @param point is the GeographyFieldsAccessExpression which accesses two fields
     * in the schema, the first of which should be the latitude and the second should
     * be the longitude.
     * @param shapeExpression a circle expression node.
     */
    static ExpressionNodePtr create(GeographyFieldsAccessExpressionNodePtr const& point,
                                    ShapeExpressionNodePtr const& shapeExpression);

    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;

    /**
     * @brief set the children node of this expression.
     */
    void setChildren(ExpressionNodePtr const& point, ShapeExpressionNodePtr const& circle);

    /**
     * @brief gets the point.
     */
    ExpressionNodePtr getPoint() const;

    /**
     * @brief gets the circle.
     */
    ShapeExpressionNodePtr getCircle() const;

    /**
     * @brief Infers the stamp of the ST_DWITHIN expression node.
     * @param typeInferencePhaseContext
     * @param schema the current schema.
     */
    void inferStamp( SchemaPtr schema) override;

    /**
    * @brief Create a deep copy of this expression node.
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr copy() override;

  protected:
    explicit STDWithinExpressionNode(STDWithinExpressionNode* other);
};
}// namespace NES

#endif// NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_STDWITHINEXPRESSIONNODE_HPP_
