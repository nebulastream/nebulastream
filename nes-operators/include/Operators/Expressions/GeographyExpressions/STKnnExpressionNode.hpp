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

#ifndef NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_STKNNEXPRESSIONNODE_HPP_
#define NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_STKNNEXPRESSIONNODE_HPP_

#include <Operators/Expressions/ConstantValueExpressionNode.hpp>
#include <Operators/Expressions/GeographyExpressions/GeographyExpressionNode.hpp>
#include <Operators/Expressions/GeographyExpressions/GeographyFieldsAccessExpressionNode.hpp>
#include <Operators/Expressions/GeographyExpressions/ShapeExpressions/ShapeExpressionNode.hpp>

namespace NES {

class ConstantValueExpressionNode;
using ConstantValueExpressionNodePtr = std::shared_ptr<ConstantValueExpressionNode>;

class ShapeExpressionNode;
using ShapeExpressionNodePtr = std::shared_ptr<ShapeExpressionNode>;

/**
 * @brief This node represents ST_KNN predicate, where ST stands for Spatial Type and
 * KNN stands for "K" nearest neighbor. In general, ST_KNN predicate retrieves the "k"
 * nearest neighbors of a query point from a group of geometric objects. In NES, we will
 * combine ST_KNN expression with a window which would allow us to define the batch of
 * objects from which to select the "k" nearest neighbors of the query point. For now,
 * this query remains unimplemented.
 */
class STKnnExpressionNode : public ExpressionNode, public GeographyExpressionNode {
  public:
    STKnnExpressionNode();
    ~STKnnExpressionNode() = default;
    /**
     * @brief Create a new ST_KNN expression.
     * @param point is the GeographyFieldsAccessExpression which accesses two fields
     * in the schema, the first of which should be the latitude and the second should
     * be the longitude.
     * @param queryPoint represents the query point (shape type should be point).
     * @param k represents the value for parameter k in the query.
     */
    static ExpressionNodePtr create(GeographyFieldsAccessExpressionNodePtr const& point,
                                    ShapeExpressionNodePtr const& queryPoint,
                                    ConstantValueExpressionNodePtr const& k);

    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;

    /**
     * @brief set the children node of this expression.
     * @param point is the GeographyFieldsAccessExpression which accesses two fields
     * in the schema, the first of which should be the latitude and the second should
     * be the longitude.
     * @param queryPoint represents the query point (shape type should be point).
     * @param k represents the value for parameter k in the query.
     */
    void setChildren(ExpressionNodePtr const& point, ShapeExpressionNodePtr const& queryPoint, ExpressionNodePtr const& k);

    /**
     * @brief gets the point.
     */
    ExpressionNodePtr getPoint() const;

    /**
     * @brief gets the wkt.
     */
    ShapeExpressionNodePtr getQueryPoint() const;

    /**
     * @brief gets the parameter k.
     */
    ExpressionNodePtr getK() const;

    /**
     * @brief Infers the stamp of the ST_KNN expression node.
     * @param typeInferencePhaseContext
     * @param schema the current schema.
     */
    void inferStamp(const Optimizer::TypeInferencePhaseContext& typeInferencePhaseContext, SchemaPtr schema) override;

    /**
    * @brief Create a deep copy of this expression node.
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr copy() override;

  protected:
    explicit STKnnExpressionNode(STKnnExpressionNode* other);
};
}// namespace NES

#endif// NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_STKNNEXPRESSIONNODE_HPP_
