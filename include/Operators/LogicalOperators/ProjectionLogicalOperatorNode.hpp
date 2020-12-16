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

#ifndef PROJECTION_LOGICAL_OPERATOR_NODE_HPP
#define PROJECTION_LOGICAL_OPERATOR_NODE_HPP

#include <API/UserAPIExpression.hpp>
#include <Operators/LogicalOperators/Arity/UnaryOperatorNode.hpp>
#include <Operators/OperatorForwardDeclaration.hpp>

namespace NES {

/**
 * @brief projection operator, which contains an resets the output schema
 */
class ProjectionLogicalOperatorNode : public UnaryOperatorNode {
  public:
    explicit ProjectionLogicalOperatorNode(std::vector<ExpressionItem> expressions, OperatorId id);
    ~ProjectionLogicalOperatorNode() = default;

    /**
     * @brief check if two operators have the same output schema
     * @param rhs the operator to compare
     * @return bool true if they are the same otherwise false
     */
    bool equal(const NodePtr rhs) const override;
    bool isIdentical(NodePtr rhs) const override;
    const std::string toString() const override;

    /**
    * @brief infers the input and out schema of this operator depending on its child.
    * @return true if schema was correctly inferred
    */
    bool inferSchema() override;
    OperatorNodePtr copy() override;

    std::vector<ExpressionItem> getExpressions();

  private:
    std::vector<ExpressionItem> expressions;
};

}// namespace NES
#endif// PROJECTION_LOGICAL_OPERATOR_NODE_HPP
