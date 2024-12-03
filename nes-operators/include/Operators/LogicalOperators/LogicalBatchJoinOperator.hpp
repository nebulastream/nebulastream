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

#pragma once

#include <memory>
#include <Identifiers/Identifiers.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalBatchJoinDescriptor.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperator.hpp>

namespace NES::Experimental
{

/**
 * @brief Batch Join operator, which contains an function as a predicate.
 */
class LogicalBatchJoinOperator : public LogicalBinaryOperator
{
public:
    explicit LogicalBatchJoinOperator(std::shared_ptr<Join::Experimental::LogicalBatchJoinDescriptor> batchJoinDefinition, OperatorId id);
    ~LogicalBatchJoinOperator() override = default;

    /**
    * @brief get join definition.
    * @return LogicalJoinDescriptor
    */
    std::shared_ptr<Join::Experimental::LogicalBatchJoinDescriptor> getBatchJoinDefinition() const;

    [[nodiscard]] bool isIdentical(const std::shared_ptr<Node>& rhs) const override;
    ///infer schema of two child operators
    bool inferSchema() override;
    std::shared_ptr<Operator> copy() override;
    [[nodiscard]] bool equal(const std::shared_ptr<Node>& rhs) const override;
    void inferStringSignature() override;

protected:
    [[nodiscard]] std::ostream& toDebugString(std::ostream& os) const override;
    [[nodiscard]] std::ostream& toQueryPlanString(std::ostream& os) const override;

private:
    std::shared_ptr<Join::Experimental::LogicalBatchJoinDescriptor> batchJoinDefinition;
};
}
