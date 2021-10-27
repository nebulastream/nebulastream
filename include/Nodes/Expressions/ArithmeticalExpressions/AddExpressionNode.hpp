/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_NODES_EXPRESSIONS_ARITHMETICAL_EXPRESSIONS_ADD_EXPRESSION_NODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_ARITHMETICAL_EXPRESSIONS_ADD_EXPRESSION_NODE_HPP_
#include <Nodes/Expressions/ArithmeticalExpressions/ArithmeticalBinaryExpressionNode.hpp>
namespace NES {
/**
 * @brief This node represents an ADD expression.
 */
class AddExpressionNode final : public ArithmeticalBinaryExpressionNode {
  public:
    explicit AddExpressionNode(DataTypePtr stamp);
    ~AddExpressionNode() noexcept final = default;
    /**
     * @brief Create a new ADD expression
     */
    static ExpressionNodePtr create(ExpressionNodePtr const& left, ExpressionNodePtr const& right);
    [[nodiscard]] bool equal(NodePtr const& rhs) const final;
    [[nodiscard]] std::string toString() const final;

    /**
    * @brief Create a deep copy of this expression node.
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr copy() final;

  protected:
    explicit AddExpressionNode(AddExpressionNode* other);
};

}// namespace NES

#endif  // NES_INCLUDE_NODES_EXPRESSIONS_ARITHMETICAL_EXPRESSIONS_ADD_EXPRESSION_NODE_HPP_
