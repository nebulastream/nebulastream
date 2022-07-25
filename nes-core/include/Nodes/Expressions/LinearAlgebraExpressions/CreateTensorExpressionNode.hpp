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

#ifndef NES_INCLUDE_NODES_EXPRESSIONS_ARITHMETICALEXPRESSIONS_CREATETENSOREXPRESSIONNODE_HPP
#define NES_INCLUDE_NODES_EXPRESSIONS_ARITHMETICALEXPRESSIONS_CREATETENSOREXPRESSIONNODE_HPP
#include <Nodes/Expressions/LinearAlgebraExpressions/LinearAlgebraTensorExpressionNode.hpp>

namespace NES {

/**
 * @brief This node represents a tensor creation expression.
 */
class CreateTensorExpressionNode final : public LinearAlgebraTensorExpressionNode {
  public:
    explicit CreateTensorExpressionNode(DataTypePtr stamp,
                                        const std::vector<size_t> shape,
                                        const TensorMemoryFormat tensorType);
    ~CreateTensorExpressionNode() noexcept final = default;
    /**
     * @brief Create a new tensor creation expression
     */
    static ExpressionNodePtr create(std::vector<ExpressionNodePtr> const expressionNodes,
                                    const std::vector<size_t> shape,
                                    const TensorMemoryFormat tensorType);
    [[nodiscard]] bool equal(NodePtr const& rhs) const final;
    [[nodiscard]] std::string toString() const final;

    /**
    * @brief Create a deep copy of this expression node.
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr copy() final;

    const std::vector<size_t>& getShape() const;
    void setShape(const std::vector<size_t>& shape);
    TensorMemoryFormat getTensorMemoryFormat() const;
    void setTensorMemoryFormat(TensorMemoryFormat tensorMemoryFormat);

  protected:
    explicit CreateTensorExpressionNode(CreateTensorExpressionNode* other);

  private:
    std::vector<size_t> shape;
    TensorMemoryFormat tensorMemoryFormat;
};

}// namespace NES
#endif//NES_INCLUDE_NODES_EXPRESSIONS_ARITHMETICALEXPRESSIONS_CREATETENSOREXPRESSIONNODE_HPP
