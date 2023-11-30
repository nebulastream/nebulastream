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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_EXPRESSIONS_WHENEXPRESSIONNODE_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_EXPRESSIONS_WHENEXPRESSIONNODE_HPP_
#include <Operators/Expressions/BinaryExpressionNode.hpp>
namespace NES {
/**
 * @brief This node represents an When expression.
 * It is used as part of a case expression, if it evaluates the left child to true, the right child will be returned.
 */
class WhenExpressionNode final : public BinaryExpressionNode {
  public:
    explicit WhenExpressionNode(DataTypePtr stamp);
    ~WhenExpressionNode() noexcept final = default;

    /**
     * @brief Create a new When expression.
     */
    static ExpressionNodePtr create(ExpressionNodePtr const& left, ExpressionNodePtr const& right);

    /**
     * @brief Infers the stamp of this expression node.
     * @param typeInferencePhaseContext
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
    explicit WhenExpressionNode(WhenExpressionNode* other);
};

}// namespace NES

#endif  // NES_OPERATORS_INCLUDE_OPERATORS_EXPRESSIONS_WHENEXPRESSIONNODE_HPP_
