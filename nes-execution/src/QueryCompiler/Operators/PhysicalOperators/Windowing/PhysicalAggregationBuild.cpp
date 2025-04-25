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
#include <DataTypes/Schema.hpp>
#include <Execution/Operators/Streaming/WindowBasedOperatorHandler.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <Operators/Operator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalAggregationBuild.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

std::shared_ptr<PhysicalOperator> PhysicalAggregationBuild::create(
    const OperatorId id,
    const Schema& inputSchema,
    const Schema& outputSchema,
    std::shared_ptr<Windowing::LogicalWindowDescriptor> windowDefinition,
    std::shared_ptr<Runtime::Execution::Operators::WindowBasedOperatorHandler> windowHandler)
{
    return std::make_shared<PhysicalAggregationBuild>(PhysicalAggregationBuild(
        id, std::move(inputSchema), std::move(outputSchema), std::move(windowDefinition), std::move(windowHandler)));
}

std::shared_ptr<Operator> PhysicalAggregationBuild::copy()
{
    return create(id, inputSchema, outputSchema, windowDefinition, windowHandler);
}

PhysicalAggregationBuild::PhysicalAggregationBuild(
    OperatorId id,
    const Schema& inputSchema,
    const Schema& outputSchema,
    std::shared_ptr<Windowing::LogicalWindowDescriptor> windowDefinition,
    std::shared_ptr<Runtime::Execution::Operators::WindowBasedOperatorHandler> windowHandler)
    : Operator(id)
    , PhysicalWindowOperator(
          std::move(id), std::move(inputSchema), std::move(outputSchema), std::move(windowDefinition), std::move(windowHandler))
{
}

}
