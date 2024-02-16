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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_BATCHJOINLOGICALOPERATORNODE_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_BATCHJOINLOGICALOPERATORNODE_HPP_

#include "BatchJoinDescriptor.hpp"
#include "Operators/LogicalOperators/LogicalBinaryOperator.hpp"
#include <memory>

namespace NES::Experimental {

/**
 * @brief Batch Join operator, which contains an expression as a predicate.
 */
class LogicalBatchJoinOperator : public LogicalBinaryOperator {
  public:
    explicit LogicalBatchJoinOperator(Join::Experimental::LogicalBatchJoinDescriptorPtr batchJoinDescriptor, OperatorId id);
    ~LogicalBatchJoinOperator() override = default;

    /**
    * @brief get join Descriptor.
    * @return JoinDescriptor
    */
    Join::Experimental::LogicalBatchJoinDescriptorPtr getBatchJoinDescriptor() const;

    [[nodiscard]] bool isIdentical(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;
    //infer schema of two child operators
    bool inferSchema() override;
    OperatorNodePtr copy() override;
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    void inferStringSignature() override;

  private:
    Join::Experimental::LogicalBatchJoinDescriptorPtr batchJoinDescriptor;
};
}// namespace NES::Experimental
#endif  // NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_BATCHJOINLOGICALOPERATORNODE_HPP_
