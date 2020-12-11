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

#ifndef Join_LOGICAL_OPERATOR_NODE_HPP
#define Join_LOGICAL_OPERATOR_NODE_HPP

#include <Operators/LogicalOperators/Arity/BinaryOperatorNode.hpp>
#include <Operators/OperatorForwardDeclaration.hpp>
#include <memory>
#include <z3++.h>

namespace NES {

/**
 * @brief Join operator, which contains an expression as a predicate.
 */
class JoinLogicalOperatorNode : public BinaryOperatorNode {
  public:
    explicit JoinLogicalOperatorNode(Join::LogicalJoinDefinitionPtr joinDefinition, OperatorId id);
    ~JoinLogicalOperatorNode() = default;

    bool isIdentical(NodePtr rhs) const override;
    const std::string toString() const override;
    //infer schema of two child operators
    bool inferSchema() override;
    OperatorNodePtr copy() override;
    bool equal(const NodePtr rhs) const override;



    Join::LogicalJoinDefinitionPtr getJoinDefinition();

  private:
    Join::LogicalJoinDefinitionPtr joinDefinition;
};
}// namespace NES
#endif// Join_LOGICAL_OPERATOR_NODE_HPP