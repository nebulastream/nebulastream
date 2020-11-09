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

#ifndef NES_INCLUDE_NODES_EXPRESSIONS_UNARYEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_UNARYEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/ExpressionNode.hpp>
namespace NES {
/**
 * @brief A unary expression is used to represent expressions with one child.
 */
class UnaryExpressionNode : public ExpressionNode {
  public:
    UnaryExpressionNode(DataTypePtr stamp);

    /**
     * @brief set the child node of this expression.
     * @param child ExpressionNodePtr
     */
    void setChild(ExpressionNodePtr child);

    /**
     * @brief returns the child of this expression
     * @return
     */
    ExpressionNodePtr child() const;

    /**
    * @brief Create a deep copy of this expression node.
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr copy() override = 0;

  protected:
    explicit UnaryExpressionNode(UnaryExpressionNode* other);
};
}// namespace NES

#endif//NES_INCLUDE_NODES_EXPRESSIONS_UNARYEXPRESSIONNODE_HPP_
