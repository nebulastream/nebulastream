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

#include <Execution/Expressions/Expression.hpp>
#include <Expressions/ExpressionNode.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalStreamJoinOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalBinaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{
/// This class represents the physical stream join probe operator and gets translated to a join probe operator
class PhysicalStreamJoinProbeOperator : public PhysicalStreamJoinOperator, public PhysicalBinaryOperator, public AbstractScanOperator
{
public:
    /// creates a PhysicalStreamJoinProbeOperator with a provided operatorId
    static PhysicalOperatorPtr create(
        OperatorId id,
        const SchemaPtr& leftSchema,
        const SchemaPtr& rightSchema,
        const SchemaPtr& outputSchema,
        const ExpressionNodePtr joinExpression,
        const std::string& windowStartFieldName,
        const std::string& windowEndFieldName,
        const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& operatorHandler,
        QueryCompilation::StreamJoinStrategy joinStrategy);

    /// Creates a PhysicalStreamJoinProbeOperator that retrieves a new operatorId by calling method
    static PhysicalOperatorPtr create(
        const SchemaPtr& leftSchema,
        const SchemaPtr& rightSchema,
        const SchemaPtr& outputSchema,
        ExpressionNodePtr joinExpression,
        const std::string& windowStartFieldName,
        const std::string& windowEndFieldName,
        const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& operatorHandler,
        QueryCompilation::StreamJoinStrategy joinStrategy);

    PhysicalStreamJoinProbeOperator(
        OperatorId id,
        const SchemaPtr& leftSchema,
        const SchemaPtr& rightSchema,
        const SchemaPtr& outputSchema,
        ExpressionNodePtr joinExpression,
        const std::string& windowStartFieldName,
        const std::string& windowEndFieldName,
        const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& operatorHandler,
        QueryCompilation::StreamJoinStrategy joinStrategy);

    /// Creates a string containing the name of this physical operator
    [[nodiscard]] std::string toString() const override;

    OperatorPtr copy() override;

    const Runtime::Execution::Operators::WindowMetaData& getWindowMetaData() const;

    Runtime::Execution::Operators::JoinSchema getJoinSchema();

    ExpressionNodePtr getJoinExpression() const;

protected:
    ExpressionNodePtr joinExpression;
    const Runtime::Execution::Operators::WindowMetaData windowMetaData;
};
}
