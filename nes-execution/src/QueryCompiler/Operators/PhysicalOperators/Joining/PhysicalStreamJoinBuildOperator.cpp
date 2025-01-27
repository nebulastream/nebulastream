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
#include <utility>
#include <API/Schema.hpp>
#include <Configurations/Enums/CompilationStrategy.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/Operator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalStreamJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>
#include <QueryCompiler/Phases/Translations/TimestampField.hpp>
#include <Util/Execution.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

PhysicalStreamJoinBuildOperator::PhysicalStreamJoinBuildOperator(
    const std::shared_ptr<Schema>& inputSchema,
    const std::shared_ptr<Schema>& outputSchema,
    const std::shared_ptr<Runtime::Execution::Operators::StreamJoinOperatorHandler>& operatorHandler,
    const QueryCompilation::StreamJoinStrategy joinStrategy,
    TimestampField timeStampField,
    const JoinBuildSideType buildSide,
    const OperatorId id)
    : Operator(id)
    , PhysicalUnaryOperator(id, inputSchema, outputSchema)
    , streamJoinOperatorHandler(operatorHandler)
    , joinStrategy(joinStrategy)
    , timeStampField(std::move(timeStampField))
    , buildSide(buildSide)
{
}

std::shared_ptr<Operator> PhysicalStreamJoinBuildOperator::copy()
{
    auto result = std::make_shared<PhysicalStreamJoinBuildOperator>(
        inputSchema, outputSchema, streamJoinOperatorHandler, joinStrategy, timeStampField, buildSide, id);
    result->addAllProperties(properties);
    return result;
}

const std::shared_ptr<Runtime::Execution::Operators::StreamJoinOperatorHandler>&
PhysicalStreamJoinBuildOperator::getJoinOperatorHandler() const
{
    return streamJoinOperatorHandler;
}

StreamJoinStrategy PhysicalStreamJoinBuildOperator::getJoinStrategy() const
{
    return joinStrategy;
}

const TimestampField& PhysicalStreamJoinBuildOperator::getTimeStampField() const
{
    return timeStampField;
}

JoinBuildSideType PhysicalStreamJoinBuildOperator::getBuildSide() const
{
    return buildSide;
}

}
