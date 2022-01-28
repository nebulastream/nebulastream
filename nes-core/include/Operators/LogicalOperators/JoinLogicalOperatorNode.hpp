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

#ifndef NES_INCLUDE_OPERATORS_LOGICALOPERATORS_JOINLOGICALOPERATORNODE_HPP_
#define NES_INCLUDE_OPERATORS_LOGICALOPERATORS_JOINLOGICALOPERATORNODE_HPP_

#include <Operators/LogicalOperators/LogicalBinaryOperatorNode.hpp>

#include <memory>
#include <z3++.h>

namespace NES {

/**
 * @brief Join operator, which contains an expression as a predicate.
 */
class JoinLogicalOperatorNode : public LogicalBinaryOperatorNode {
  public:
    explicit JoinLogicalOperatorNode(Join::LogicalJoinDefinitionPtr joinDefinition, OperatorId id);
    ~JoinLogicalOperatorNode() override = default;

    /**
    * @brief get join definition.
    * @return LogicalJoinDefinition
    */
    Join::LogicalJoinDefinitionPtr getJoinDefinition();

    [[nodiscard]] bool isIdentical(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;
    //infer schema of two child operators
    bool inferSchema() override;
    OperatorNodePtr copy() override;
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    void inferStringSignature() override;

  private:
    Join::LogicalJoinDefinitionPtr joinDefinition;
};
}// namespace NES
#endif  // NES_INCLUDE_OPERATORS_LOGICALOPERATORS_JOINLOGICALOPERATORNODE_HPP_
