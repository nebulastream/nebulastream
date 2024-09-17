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

#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalStreamJoinProbeOperator.hpp>
#include <QueryCompiler/Phases/Translations/ExpressionProvider.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

PhysicalOperatorPtr PhysicalStreamJoinProbeOperator::create(
    OperatorId id,
    const SchemaPtr& leftSchema,
    const SchemaPtr& rightSchema,
    const SchemaPtr& outputSchema,
    ExpressionNodePtr joinExpression,
    const std::string& windowStartFieldName,
    const std::string& windowEndFieldName,
    const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& operatorHandler,
    QueryCompilation::StreamJoinStrategy joinStrategy)
{
    return std::make_shared<PhysicalStreamJoinProbeOperator>(
        id, leftSchema, rightSchema, outputSchema, joinExpression, windowStartFieldName, windowEndFieldName, operatorHandler, joinStrategy);
}

PhysicalOperatorPtr PhysicalStreamJoinProbeOperator::create(
    const SchemaPtr& leftSchema,
    const SchemaPtr& rightSchema,
    const SchemaPtr& outputSchema,
    ExpressionNodePtr joinExpression,
    const std::string& windowStartFieldName,
    const std::string& windowEndFieldName,
    const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& operatorHandler,
    QueryCompilation::StreamJoinStrategy joinStrategy)
{
    return create(
        getNextOperatorId(),
        leftSchema,
        rightSchema,
        outputSchema,
        joinExpression,
        windowStartFieldName,
        windowEndFieldName,
        operatorHandler,
        joinStrategy);
}

PhysicalStreamJoinProbeOperator::PhysicalStreamJoinProbeOperator(
    OperatorId id,
    const SchemaPtr& leftSchema,
    const SchemaPtr& rightSchema,
    const SchemaPtr& outputSchema,
    ExpressionNodePtr joinExpression,
    const std::string& windowStartFieldName,
    const std::string& windowEndFieldName,
    const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& operatorHandler,
    QueryCompilation::StreamJoinStrategy joinStrategy)
    : Operator(id)
    , PhysicalStreamJoinOperator(operatorHandler, joinStrategy)
    , PhysicalBinaryOperator(id, leftSchema, rightSchema, outputSchema)
    , joinExpression(joinExpression)
    , windowMetaData(windowStartFieldName, windowEndFieldName)
{
}

std::string PhysicalStreamJoinProbeOperator::toString() const
{
    std::stringstream out;
    out << std::endl;
    out << "PhysicalStreamJoinProbeOperator:\n";
    out << PhysicalBinaryOperator::toString();
    out << "windowStartFieldName: " << windowMetaData.windowStartFieldName << "\n";
    out << "windowEndFieldName: " << windowMetaData.windowEndFieldName << "\n";
    out << std::endl;
    return out.str();
}

OperatorPtr PhysicalStreamJoinProbeOperator::copy()
{
    return create(
        id,
        leftInputSchema,
        rightInputSchema,
        outputSchema,
        joinExpression,
        windowMetaData.windowStartFieldName,
        windowMetaData.windowEndFieldName,
        joinOperatorHandler,
        getJoinStrategy());
}

const Runtime::Execution::Operators::WindowMetaData& PhysicalStreamJoinProbeOperator::getWindowMetaData() const
{
    return windowMetaData;
}

Runtime::Execution::Operators::JoinSchema PhysicalStreamJoinProbeOperator::getJoinSchema()
{
    return Runtime::Execution::Operators::JoinSchema(getLeftInputSchema(), getRightInputSchema(), getOutputSchema());
}

ExpressionNodePtr PhysicalStreamJoinProbeOperator::getJoinExpression() const
{
    return joinExpression;
}

}
