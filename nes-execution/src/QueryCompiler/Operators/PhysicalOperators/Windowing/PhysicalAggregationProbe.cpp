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
#include <Execution/Operators/Streaming/WindowBasedOperatorHandler.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/WindowOperator.hpp>
#include <Operators/Operator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalAggregationProbe.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

WindowMetaData PhysicalAggregationProbe::getWindowMetaData() const
{
    return windowMetaData;
}

std::shared_ptr<PhysicalOperator> PhysicalAggregationProbe::create(
    const OperatorId id,
    Schema inputSchema,
    Schema outputSchema,
    std::shared_ptr<Windowing::LogicalWindowDescriptor> windowDefinition,
    std::shared_ptr<Runtime::Execution::Operators::WindowBasedOperatorHandler> windowHandler,
    WindowMetaData windowMetaData)
{
    return std::make_shared<PhysicalAggregationProbe>(PhysicalAggregationProbe(
        id,
        std::move(inputSchema),
        std::move(outputSchema),
        std::move(windowDefinition),
        std::move(windowHandler),
        std::move(windowMetaData)));
}

std::shared_ptr<Operator> PhysicalAggregationProbe::copy()
{
    return create(id, inputSchema, outputSchema, windowDefinition, windowHandler, windowMetaData);
}

PhysicalAggregationProbe::PhysicalAggregationProbe(
    OperatorId id,
    Schema inputSchema,
    Schema outputSchema,
    std::shared_ptr<Windowing::LogicalWindowDescriptor> windowDefinition,
    std::shared_ptr<Runtime::Execution::Operators::WindowBasedOperatorHandler> windowHandler,
    WindowMetaData windowMetaData)
    : Operator(id)
    , PhysicalWindowOperator(
          std::move(id), std::move(inputSchema), std::move(outputSchema), std::move(windowDefinition), std::move(windowHandler))
    , windowMetaData(std::move(windowMetaData))
{
}

}
