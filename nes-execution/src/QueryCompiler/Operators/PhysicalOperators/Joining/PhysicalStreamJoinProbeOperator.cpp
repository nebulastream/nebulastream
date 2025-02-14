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

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <API/Schema.hpp>
#include <Execution/Functions/Function.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperators/Windows/WindowOperator.hpp>
#include <Operators/Operator.hpp>
#include <QueryCompiler/Configurations/Enums/CompilationStrategy.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalStreamJoinProbeOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalBinaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

PhysicalStreamJoinProbeOperator::PhysicalStreamJoinProbeOperator(
    const std::shared_ptr<Schema>& leftSchema,
    const std::shared_ptr<Schema>& rightSchema,
    const std::shared_ptr<Schema>& outputSchema,
    const std::shared_ptr<Runtime::Execution::Operators::StreamJoinOperatorHandler>& operatorHandler,
    const Configurations::StreamJoinStrategy joinStrategy,
    std::unique_ptr<Runtime::Execution::Functions::Function> joinFunction,
    const std::vector<std::string>& joinFieldNamesLeft,
    const std::vector<std::string>& joinFieldNamesRight,
    WindowMetaData windowMetaData,
    const OperatorId id)
    : Operator(id)
    , PhysicalBinaryOperator(id, leftSchema, rightSchema, outputSchema)
    , streamJoinOperatorHandler(operatorHandler)
    , joinStrategy(joinStrategy)
    , joinFunction(std::move(joinFunction))
    , joinFieldNamesLeft(joinFieldNamesLeft)
    , joinFieldNamesRight(joinFieldNamesRight)
    , windowMetaData(std::move(windowMetaData))
{
}

std::shared_ptr<Operator> PhysicalStreamJoinProbeOperator::copy()
{
    return std::make_shared<PhysicalStreamJoinProbeOperator>(
        leftInputSchema,
        rightInputSchema,
        outputSchema,
        streamJoinOperatorHandler,
        joinStrategy,
        std::move(joinFunction),
        joinFieldNamesLeft,
        joinFieldNamesRight,
        windowMetaData,
        id);
}

const std::shared_ptr<Runtime::Execution::Operators::StreamJoinOperatorHandler>&
PhysicalStreamJoinProbeOperator::getJoinOperatorHandler() const
{
    return streamJoinOperatorHandler;
}

Configurations::StreamJoinStrategy PhysicalStreamJoinProbeOperator::getJoinStrategy() const
{
    return joinStrategy;
}

std::unique_ptr<Runtime::Execution::Functions::Function> PhysicalStreamJoinProbeOperator::getJoinFunction()
{
    return std::move(joinFunction);
}

std::vector<std::string> PhysicalStreamJoinProbeOperator::getJoinFieldNameLeft() const
{
    return joinFieldNamesLeft;
}

std::vector<std::string> PhysicalStreamJoinProbeOperator::getJoinFieldNameRight() const
{
    return joinFieldNamesRight;
}

Runtime::Execution::JoinSchema PhysicalStreamJoinProbeOperator::getJoinSchema() const
{
    return {leftInputSchema, rightInputSchema, outputSchema};
}

const WindowMetaData& PhysicalStreamJoinProbeOperator::getWindowMetaData() const
{
    return windowMetaData;
}

}
