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

#ifndef NES_INCLUDE_NODES_EXPRESSIONS_ARITHMETICALEXPRESSIONS_ARITHMETICALEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_ARITHMETICALEXPRESSIONS_ARITHMETICALEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/BinaryExpressionNode.hpp>
namespace NES {
/**
 * @brief This node represents a arithmetical expression.
 */
class ArithmeticalExpressionNode : public BinaryExpressionNode {
  public:
    /**
     * @brief Infers the stamp of this arithmetical expression node.
     * Currently the type inference is equal for all arithmetical expression and expects numerical data types as operands.
     * @param schema the current schema.
     */
    void inferStamp(SchemaPtr schema) override;

    bool equal(NodePtr rhs) const override;
    const std::string toString() const override;

  protected:
    explicit ArithmeticalExpressionNode(DataTypePtr stamp);
    explicit ArithmeticalExpressionNode(ArithmeticalExpressionNode* other);
    ~ArithmeticalExpressionNode() = default;
};

}// namespace NES

#endif// NES_INCLUDE_NODES_EXPRESSIONS_ARITHMETICALEXPRESSIONS_ARITHMETICALEXPRESSIONNODE_HPP_
