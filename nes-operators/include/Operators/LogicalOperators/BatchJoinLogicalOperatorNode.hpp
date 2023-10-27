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

#ifndef NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_BATCHJOINLOGICALOPERATORNODE_HPP_
#define NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_BATCHJOINLOGICALOPERATORNODE_HPP_

#include <Operators/LogicalOperators/LogicalBinaryOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalBatchJoinDefinition.hpp>
#include <memory>

namespace NES::Experimental {

/**
 * @brief Batch Join operator, which contains an expression as a predicate.
 */
class BatchJoinLogicalOperatorNode : public LogicalBinaryOperatorNode {
  public:
    explicit BatchJoinLogicalOperatorNode(Join::Experimental::LogicalBatchJoinDefinitionPtr batchJoinDefinition, OperatorId id);
    ~BatchJoinLogicalOperatorNode() override = default;

    /**
    * @brief get join definition.
    * @return LogicalJoinDefinition
    */
    Join::Experimental::LogicalBatchJoinDefinitionPtr getBatchJoinDefinition() const;

    [[nodiscard]] bool isIdentical(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;
    //infer schema of two child operators
    bool inferSchema() override;
    OperatorNodePtr copy() override;
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    void inferStringSignature() override;

  private:
    Join::Experimental::LogicalBatchJoinDefinitionPtr batchJoinDefinition;
};
}// namespace NES::Experimental
#endif// NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_BATCHJOINLOGICALOPERATORNODE_HPP_
