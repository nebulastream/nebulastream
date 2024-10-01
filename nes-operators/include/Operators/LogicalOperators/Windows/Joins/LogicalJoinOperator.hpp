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

namespace NES
{

/**
 * @brief Join operator, which contains an expression as a predicate.
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
    [[nodiscard]] std::string toString() const override;
    ///infer schema of two child operators
    bool inferSchema() override;
    OperatorPtr copy() override;
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    void inferStringSignature() override;
    std::vector<OriginId> getOutputOriginIds() const override;
    void setOriginId(OriginId originId) override;

    /**
     * @brief Getter for the window start field name
     * @return std::string
     */
    const std::string& getWindowStartFieldName() const;

    /**
     * @brief Getter for the window end field name
     * @return std::string
     */
    const std::string& getWindowEndFieldName() const;

    /**
     * @brief Getter for join expression, i.e. a set of binary expressions
     * @return joinExpressions
     */
    const ExpressionNodePtr getJoinExpression() const;

    /**
     * @brief Sets the window start, end, and key field name during the serialization of the operator
     * @param windowStartFieldName
     * @param windowEndFieldName
     */
    void setWindowStartEndKeyFieldName(std::string_view windowStartFieldName, std::string_view windowEndFieldName);

private:
    const Join::LogicalJoinDescriptorPtr joinDefinition;
    std::string windowStartFieldName;
    std::string windowEndFieldName;
};
using LogicalJoinOperatorPtr = std::shared_ptr<LogicalJoinOperator>;
} /// namespace NES
