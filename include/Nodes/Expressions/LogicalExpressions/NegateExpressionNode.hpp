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

#ifndef NES_INCLUDE_NODES_EXPRESSIONS_UNARYEXPRESSIONS_NEGATEEXPRESSINNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_UNARYEXPRESSIONS_NEGATEEXPRESSINNODE_HPP_
#include <Nodes/Expressions/LogicalExpressions/LogicalUnaryExpressionNode.hpp>
namespace NES {

/**
 * @brief This node negates its child expression.
 */
class NegateExpressionNode : public LogicalUnaryExpressionNode {
  public:
    NegateExpressionNode();
    ~NegateExpressionNode() = default;

    /**
     * @brief Create a new negate expression
     */
    static ExpressionNodePtr create(const ExpressionNodePtr child);

    bool equal(const NodePtr rhs) const override;
    const std::string toString() const override;
    /**
     * @brief Infers the stamp of this logical negate expression node.
     * We assume that the children of this expression is a predicate.
     * @param schema the current schema.
     */
    void inferStamp(SchemaPtr schema) override;

    /**
    * @brief Create a deep copy of this expression node.
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr copy() override;

  protected:
    explicit NegateExpressionNode(NegateExpressionNode* other);
};
}// namespace NES

#endif//NES_INCLUDE_NODES_EXPRESSIONS_UNARYEXPRESSIONS_NEGATEEXPRESSINNODE_HPP_
