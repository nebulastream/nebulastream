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

#ifndef NES_INCLUDE_NODES_EXPRESSIONS_ARITHMETICALEXPRESSIONS_MULTIEXPRESSIONNODE_HPP
#define NES_INCLUDE_NODES_EXPRESSIONS_ARITHMETICALEXPRESSIONS_MULTIEXPRESSIONNODE_HPP
#include <Nodes/Expressions/ArithmeticalExpressions/ArithmeticalExpressionNode.hpp>
#include <Nodes/Expressions/MultiExpressionNode.hpp>
#include <Common/DataTypes/TensorType.hpp>
namespace NES {
/**
 * @brief This node represents an arithmetical expression with multiple children
 */
class LinearAlgebraTensorExpressionNode : public MultiExpressionNode, public ArithmeticalExpressionNode {

  public:
    /**
     * @brief Infers the stamp of this arithmetical expression node.
     * Currently the type inference is equal for all arithmetical expression and expects numerical data types as operands.
     * @param schema the current schema.
     */
    void inferStamp(SchemaPtr schema) override;

    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;

  protected:
    explicit LinearAlgebraTensorExpressionNode(DataTypePtr stamp);
    explicit LinearAlgebraTensorExpressionNode(LinearAlgebraTensorExpressionNode* other);

    explicit LinearAlgebraTensorExpressionNode(DataTypePtr stamp, std::vector<size_t> shape, TensorMemoryFormat tensorType);
    explicit LinearAlgebraTensorExpressionNode(LinearAlgebraTensorExpressionNode* other, std::vector<size_t> shape, TensorMemoryFormat tensorType);
    ~LinearAlgebraTensorExpressionNode() noexcept override = default;

  private:

    std::vector<size_t> shape = {};
    TensorMemoryFormat tensorType = DENSE;

};
}// namespace NES

#endif//NES_INCLUDE_NODES_EXPRESSIONS_ARITHMETICALEXPRESSIONS_MULTIEXPRESSIONNODE_HPP
