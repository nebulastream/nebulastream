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

#include <PhysicalOperator.hpp>

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

PhysicalOperatorWrapper::PhysicalOperatorWrapper(
    PhysicalOperator physicalOperator,
    Schema inputSchema,
    Schema outputSchema,
    MemoryLayoutType inputMemoryLayoutType,
    MemoryLayoutType outputMemoryLayoutType)
    : physicalOperator(std::move(physicalOperator))
    , inputMemoryLayoutType(inputMemoryLayoutType)
    , outputMemoryLayoutType(outputMemoryLayoutType)
    , inputSchema(inputSchema)
    , outputSchema(outputSchema)
    , pipelineLocation(PipelineLocation::INTERMEDIATE)
{
}

PhysicalOperatorWrapper::PhysicalOperatorWrapper(
    PhysicalOperator physicalOperator,
    Schema inputSchema,
    Schema outputSchema,
    MemoryLayoutType inputMemoryLayoutType,
    MemoryLayoutType outputMemoryLayoutType,
    const PipelineLocation pipelineLocation)
    : physicalOperator(std::move(physicalOperator))
    , inputMemoryLayoutType(inputMemoryLayoutType)
    , outputMemoryLayoutType(outputMemoryLayoutType)
    , inputSchema(inputSchema)
    , outputSchema(outputSchema)
    , pipelineLocation(pipelineLocation)
{
}

PhysicalOperatorWrapper::PhysicalOperatorWrapper(
    PhysicalOperator physicalOperator,
    Schema inputSchema,
    Schema outputSchema,
    MemoryLayoutType inputMemoryLayoutType,
    MemoryLayoutType outputMemoryLayoutType,
    std::optional<OperatorHandlerId> handlerId,
    std::optional<std::shared_ptr<OperatorHandler>> handler,
    const PipelineLocation pipelineLocation)
    : physicalOperator(std::move(physicalOperator))
    , inputMemoryLayoutType(inputMemoryLayoutType)
    , outputMemoryLayoutType(outputMemoryLayoutType)
    , inputSchema(inputSchema)
    , outputSchema(outputSchema)
    , handler(std::move(handler))
    , handlerId(std::move(handlerId))
    , pipelineLocation(pipelineLocation)
{
}

PhysicalOperatorWrapper::PhysicalOperatorWrapper(
    PhysicalOperator physicalOperator,
    Schema inputSchema,
    Schema outputSchema,
    MemoryLayoutType inputMemoryLayoutType,
    MemoryLayoutType outputMemoryLayoutType,
    std::optional<OperatorHandlerId> handlerId,
    std::optional<std::shared_ptr<OperatorHandler>> handler,
    const PipelineLocation pipelineLocation,
    std::vector<std::shared_ptr<PhysicalOperatorWrapper>> children)
    : physicalOperator(std::move(physicalOperator))
    , inputMemoryLayoutType(inputMemoryLayoutType)
    , outputMemoryLayoutType(outputMemoryLayoutType)
    , inputSchema(inputSchema)
    , outputSchema(outputSchema)
    , children(std::move(children))
    , handler(std::move(handler))
    , handlerId(std::move(handlerId))
    , pipelineLocation(pipelineLocation)
{
}

std::vector<std::shared_ptr<PhysicalOperatorWrapper>> PhysicalOperatorWrapper::getChildren() const
{
    return children;
}

std::string PhysicalOperatorWrapper::explain(ExplainVerbosity) const
{
    return fmt::format(
        "PhysicalOperatorWrapper("
        "Operator: {}, id: {}, "
        "InputSchema: {}, OutputSchema: {}, "
        "pipelineLocation: {} )",
        physicalOperator.toString(),
        physicalOperator.getId(),
        inputSchema,
        outputSchema,
        magic_enum::enum_name(pipelineLocation));
}

const PhysicalOperator& PhysicalOperatorWrapper::getPhysicalOperator() const
{
    return physicalOperator;
}

const std::optional<Schema>& PhysicalOperatorWrapper::getInputSchema() const
{
    return inputSchema;
}

const std::optional<Schema>& PhysicalOperatorWrapper::getOutputSchema() const
{
    return outputSchema;
}

const std::optional<MemoryLayoutType>& PhysicalOperatorWrapper::getInputMemoryLayoutType() const
{
    return inputMemoryLayoutType;
}

const std::optional<MemoryLayoutType>& PhysicalOperatorWrapper::getOutputMemoryLayoutType() const
{
    return outputMemoryLayoutType;
}

void PhysicalOperatorWrapper::addChild(const std::shared_ptr<PhysicalOperatorWrapper>& child)
{
    children.push_back(child);
}

void PhysicalOperatorWrapper::setChildren(const std::vector<std::shared_ptr<PhysicalOperatorWrapper>>& newChildren)
{
    children = newChildren;
}

const std::optional<std::shared_ptr<OperatorHandler>>& PhysicalOperatorWrapper::getHandler() const
{
    return handler;
}

const std::optional<OperatorHandlerId>& PhysicalOperatorWrapper::getHandlerId() const
{
    return handlerId;
}

PhysicalOperatorWrapper::PipelineLocation PhysicalOperatorWrapper::getPipelineLocation() const
{
    return pipelineLocation;
}

}
