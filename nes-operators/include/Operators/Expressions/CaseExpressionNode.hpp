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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_EXPRESSIONS_CASEEXPRESSIONNODE_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_EXPRESSIONS_CASEEXPRESSIONNODE_HPP_
#include <Operators/Expressions/ExpressionNode.hpp>
namespace NES {
/**
 * @brief A case expression has at least one when expression and one default expression.
 * All when expressions are evaluated and the first one with a true condition is returned.
 */
class CaseExpressionNode : public ExpressionNode {
  public:
    explicit CaseExpressionNode(DataTypePtr stamp);
    ~CaseExpressionNode() noexcept override = default;

    /**
     * @brief Create a new Case expression node.
     * @param whenExps : a vector of when expression nodes.
     * @param defaultExp : the expression to select, if no when expression evaluates to a value
     */
    static ExpressionNodePtr create(std::vector<ExpressionNodePtr> const& whenExps, ExpressionNodePtr const& defaultExp);

    /**
     * @brief set the children nodes of this expression.
     * @param whenExps : a vector of when expression nodes.
     * @param defaultExp : the expression to select, if no when expression evaluates to a value
     */
    void setChildren(std::vector<ExpressionNodePtr> const& whenExps, ExpressionNodePtr const& defaultExp);

    /**
     * @brief gets the vector of when children.
     */
    std::vector<ExpressionNodePtr> getWhenChildren() const;

    /**
     * @brief gets the node representing the default child.
     */
    ExpressionNodePtr getDefaultExp() const;

    /**
     * @brief Infers the stamp of this expression node.
     * @param schema the current schema.
     */
    void inferStamp(SchemaPtr schema) override;

    [[nodiscard]] bool equal(NodePtr const& rhs) const final;
    [[nodiscard]] std::string toString() const final;

    /**
     * @brief Create a deep copy of this expression node.
     * @return ExpressionNodePtr
     */
    ExpressionNodePtr copy() final;

  protected:
    explicit CaseExpressionNode(CaseExpressionNode* other);
};

}// namespace NES
#endif // NES_OPERATORS_INCLUDE_OPERATORS_EXPRESSIONS_CASEEXPRESSIONNODE_HPP_
