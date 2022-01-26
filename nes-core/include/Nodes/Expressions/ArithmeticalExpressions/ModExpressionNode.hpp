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

#ifndef NES_INCLUDE_NODES_EXPRESSIONS_ARITHMETICALEXPRESSIONS_MODEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_ARITHMETICALEXPRESSIONS_MODEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/ArithmeticalExpressions/ArithmeticalBinaryExpressionNode.hpp>
namespace NES {
/**
 * @brief This node represents an MODER expression.
 */
class ModExpressionNode final : public ArithmeticalBinaryExpressionNode {
  public:
    explicit ModExpressionNode(DataTypePtr stamp);
    ~ModExpressionNode() noexcept final = default;
    /**
         * @brief Create a new MODULO expression
         */
    static ExpressionNodePtr create(ExpressionNodePtr const& left, ExpressionNodePtr const& right);
    [[nodiscard]] bool equal(NodePtr const& rhs) const final;
    [[nodiscard]] std::string toString() const final;

    /**
         * @brief Determine returned datatype (-> UInt64/Double/ Throw exception for invalid inputs). Override ArithmeticalBinaryExpressionNode::inferStamp to tighten bounds.
         * @comment E.g. if the divisor in the modulo operation is a Int8, we can set the results to be Int8.
         * @comment More general: We set upperbound = max(abs(lowerbound_of_divisor), abs(upperbound_of_divisor)) and the lowerbound to the negation of the same maxiumum. This follows the mathematical definition and implementation in C.
         * @param schema: the current schema.
         */
    void inferStamp(SchemaPtr schema) final;

    /**
        * @brief Create a deep copy of this expression node.
        * @return ExpressionNodePtr
        */
    ExpressionNodePtr copy() final;

  protected:
    explicit ModExpressionNode(ModExpressionNode* other);
};

}// namespace NES

#endif  // NES_INCLUDE_NODES_EXPRESSIONS_ARITHMETICALEXPRESSIONS_MODEXPRESSIONNODE_HPP_
