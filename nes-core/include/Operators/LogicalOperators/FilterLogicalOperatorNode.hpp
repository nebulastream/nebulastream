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

#ifndef NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_FILTER_LOGICAL_OPERATOR_NODE_HPP_
#define NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_FILTER_LOGICAL_OPERATOR_NODE_HPP_

#include <Operators/LogicalOperators/LogicalUnaryOperatorNode.hpp>
#include <Nodes/Expressions/ExpressionNode.hpp>

namespace NES {

/**
 * @brief Filter operator, which contains an expression as a predicate.
 */
class FilterLogicalOperatorNode : public LogicalUnaryOperatorNode {
  public:
    explicit FilterLogicalOperatorNode(ExpressionNodePtr const&, OperatorId id);
    ~FilterLogicalOperatorNode() override = default;

    /**
   * @brief get the filter predicate.
   * @return PredicatePtr
   */
    ExpressionNodePtr getPredicate();

    /**
     * @brief check if two operators have the same filter predicate.
     * @param rhs the operator to compare
     * @return bool true if they are the same otherwise false
     */
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] bool isIdentical(NodePtr const& rhs) const override;
    std::string toString() const override;

    /**
    * @brief infers the input and out schema of this operator depending on its child.
    * @throws Exception the predicate expression has to return a boolean.
    * @return true if schema was correctly inferred
    */
    bool inferSchema() override;
    OperatorNodePtr copy() override;
    void inferStringSignature() override;

    std::string getClassName() override;

  private:
    ExpressionNodePtr predicate;
};

}// namespace NES
#endif// NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_FILTER_LOGICAL_OPERATOR_NODE_HPP_
