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

#ifndef NES_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_STWITHINEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_STWITHINEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Expressions/GeographyExpressions/GeographyFieldsAccessExpressionNode.hpp>
namespace NES {

class ConstantValueExpressionNode;
using ConstantValueExpressionNodePtr = std::shared_ptr<ConstantValueExpressionNode>;

/**
 * @brief This node represents STWithin predicate.
 */
class STWithinExpressionNode : public ExpressionNode {
  public:
    STWithinExpressionNode();
    ~STWithinExpressionNode() = default;
    /**
     * @brief Create a new STWithin expression.
     * @param point is the GeographyFieldsAccessExpression which accesses two fields
     * in the schema, the first of which should be the latitude and the second should
     * be the longitude.
     * @param wkt represents the well-known text (WKT) of a polygon.
     */
    static ExpressionNodePtr create(GeographyFieldsAccessExpressionNodePtr const& point,
                                    ConstantValueExpressionNodePtr const& wkt);
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;

    /**
     * @brief set the children node of this expression.
     */
    void setChildren(ExpressionNodePtr const& point, ExpressionNodePtr const& wkt);

    /**
     * @brief gets the point (or the left child).
     */
    ExpressionNodePtr getPoint() const;

    /**
     * @brief gets the wkt (the right child).
     */
    ExpressionNodePtr getWKT() const;

    /**
     * @brief Infers the stamp of the STWITHIN expression node.
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

#endif// NES_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_STWITHINEXPRESSIONNODE_HPP_