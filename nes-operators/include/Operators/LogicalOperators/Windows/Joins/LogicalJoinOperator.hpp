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
#include <Operators/AbstractOperators/OriginIdAssignmentOperator.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/WindowOperator.hpp>


namespace NES
{

/**
 * @brief Join operator, which contains an function as a predicate.
 */
class LogicalJoinOperator : public LogicalBinaryOperator, public OriginIdAssignmentOperator
{
public:
    explicit LogicalJoinOperator(Join::LogicalJoinDescriptorPtr joinDefinition, OperatorId id, OriginId originId = INVALID_ORIGIN_ID);
    ~LogicalJoinOperator() override = default;

    /**
    * @brief get join definition.
    * @return LogicalJoinDescriptor
    */
    Join::LogicalJoinDescriptorPtr getJoinDefinition() const;

    [[nodiscard]] bool isIdentical(NodePtr const& rhs) const override;
    ///infer schema of two child operators
    bool inferSchema() override;
    std::shared_ptr<Operator> copy() override;
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    void inferStringSignature() override;
    std::vector<OriginId> getOutputOriginIds() const override;
    void setOriginId(OriginId originId) override;
    const NodeFunctionPtr getJoinFunction() const;

    WindowMetaData windowMetaData;

protected:
    [[nodiscard]] std::string toString() const override;

private:
    const Join::LogicalJoinDescriptorPtr joinDefinition;
};
using LogicalJoinOperatorPtr = std::shared_ptr<LogicalJoinOperator>;
}
