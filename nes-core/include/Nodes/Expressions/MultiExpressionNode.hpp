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

#ifndef NES_INCLUDE_NODES_EXPRESSIONS_MULTIEXPRESSIONNODE_HPP
#define NES_INCLUDE_NODES_EXPRESSIONS_MULTIEXPRESSIONNODE_HPP

#include <Nodes/Expressions/ExpressionNode.hpp>
namespace NES {
/**
 * @brief A multi expression represents expressions with multiple children.
 */
class MultiExpressionNode : public ExpressionNode {
  public:
    ~MultiExpressionNode() noexcept override = default;

    /**
     * @brief set the children node of this expression.
     */
    void setChildren(std::vector<ExpressionNodePtr> const& children);

    /**
     * @brief gets the children.
     */
    std::vector<ExpressionNodePtr> children() const;

    /**
    * @brief Create a deep copy of this expression node.
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr copy() override = 0;

  protected:
    explicit MultiExpressionNode(DataTypePtr stamp);
    explicit MultiExpressionNode(MultiExpressionNode* other);
};

}// namespace NES
#endif//NES_INCLUDE_NODES_EXPRESSIONS_MULTIEXPRESSIONNODE_HPP
