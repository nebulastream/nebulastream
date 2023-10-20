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

#ifndef NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_GEOGRAPHYFIELDSACCESSEXPRESSIONNODE_HPP_
#define NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_GEOGRAPHYFIELDSACCESSEXPRESSIONNODE_HPP_

#include <Operators/Expressions/ExpressionNode.hpp>
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/Expressions/GeographyExpressions/GeographyExpressionNode.hpp>

namespace NES {

/**
 * @brief A geography field access expression represents expressions
 * which accesses two fields, latitude and longitude.
 */
class GeographyFieldsAccessExpressionNode : public ExpressionNode, public GeographyExpressionNode {
  public:
    explicit GeographyFieldsAccessExpressionNode(DataTypePtr stamp);
    /**
     * @brief Create a new GeographyFieldsAccess expression.
     * @param latitude is the latitude field.
     * @param longitude is the longitude field.
     */
    static ExpressionNodePtr create(FieldAccessExpressionNodePtr const& latitude, FieldAccessExpressionNodePtr const& longitude);

    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;

    /**
     * @brief set the children node of this expression.
     */
    void setChildren(ExpressionNodePtr const& latitude, ExpressionNodePtr const& longitude);

    /**
     * @brief gets the latitude (or the left child).
     */
    ExpressionNodePtr getLatitude() const;

    /**
     * @brief gets the longitude (the right child).
     */
    ExpressionNodePtr getLongitude() const;

    /**
     * @brief Infers the stamp of this geography fields access expression node.
     * Type inference expects float data types as operands.
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
    explicit GeographyFieldsAccessExpressionNode(const FieldAccessExpressionNodePtr& latitude,
                                                 const FieldAccessExpressionNodePtr& longitude);
    explicit GeographyFieldsAccessExpressionNode(GeographyFieldsAccessExpressionNode* other);
};

using GeographyFieldsAccessExpressionNodePtr = std::shared_ptr<GeographyFieldsAccessExpressionNode>;

}// namespace NES
#endif// NES_CORE_INCLUDE_NODES_EXPRESSIONS_GEOGRAPHYEXPRESSIONS_GEOGRAPHYFIELDSACCESSEXPRESSIONNODE_HPP_
