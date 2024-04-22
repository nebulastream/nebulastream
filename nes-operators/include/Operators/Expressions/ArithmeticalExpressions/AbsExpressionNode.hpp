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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_EXPRESSIONS_ARITHMETICALEXPRESSIONS_ABSEXPRESSIONNODE_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_EXPRESSIONS_ARITHMETICALEXPRESSIONS_ABSEXPRESSIONNODE_HPP_
#include <Operators/Expressions/ArithmeticalExpressions/ArithmeticalUnaryExpressionNode.hpp>
namespace NES {
/**
 * @brief This node represents an ABS (absolut value) expression.
 */
class AbsExpressionNode final : public ArithmeticalUnaryExpressionNode {
  public:
    explicit AbsExpressionNode(DataTypePtr stamp);
    ~AbsExpressionNode() noexcept override = default;
    /**
     * @brief Create a new ABS expression
     */
    [[nodiscard]] static ExpressionNodePtr create(ExpressionNodePtr const& child);
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;

    /**
     * @brief Infers the stamp of the expression given the current schema and the typeInferencePhaseContext.
     * @param typeInferencePhaseContext
     * @param schema
     */
    void inferStamp(SchemaPtr schema) override;

    /**
    * @brief Create a deep copy of this expression node.
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr copy() override;

  private:
    explicit AbsExpressionNode(AbsExpressionNode* other);
};

}// namespace NES

#endif // NES_OPERATORS_INCLUDE_OPERATORS_EXPRESSIONS_ARITHMETICALEXPRESSIONS_ABSEXPRESSIONNODE_HPP_
