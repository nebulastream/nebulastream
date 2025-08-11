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
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <Engine.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

PhysicalOperatorConcept::PhysicalOperatorConcept() : id(getNextPhysicalOperatorId())
{
}

PhysicalOperatorConcept::PhysicalOperatorConcept(OperatorId existingId) : id(existingId)
{
}

void PhysicalOperatorConcept::setup(ExecutionContext& executionContext, CompilationContext& compilationContext) const
{
    setupChild(executionContext, compilationContext);
}

void PhysicalOperatorConcept::open(ExecutionContext& executionContext, CompilationContext& compilationContext, RecordBuffer& rb) const
{
    openChild(executionContext, compilationContext, rb);
}

void PhysicalOperatorConcept::close(ExecutionContext& executionContext, CompilationContext& compilationContext, RecordBuffer& rb) const
{
    closeChild(executionContext, compilationContext, rb);
}

void PhysicalOperatorConcept::terminate(ExecutionContext& executionContext) const
{
    terminateChild(executionContext);
}

void PhysicalOperatorConcept::execute(ExecutionContext& executionContext, CompilationContext& compilationContext, Record& record) const
{
    executeChild(executionContext, compilationContext, record);
}

void PhysicalOperatorConcept::setupChild(ExecutionContext& executionContext, CompilationContext& compilationContext) const
{
    if (const auto child = getChild())
    {
        child.value().setup(executionContext, compilationContext);
    }
}
void PhysicalOperatorConcept::openChild(ExecutionContext& executionContext, CompilationContext& compilationContext, RecordBuffer& recordBuffer) const
{
    if (const auto child = getChild())
    {
        child.value().open(executionContext, compilationContext, recordBuffer);
    }
}
void PhysicalOperatorConcept::closeChild(ExecutionContext& executionContext, CompilationContext& compilationContext, RecordBuffer& recordBuffer) const
{
    if (const auto child = getChild())
    {
        child.value().close(executionContext, compilationContext, recordBuffer);
    }
}
void PhysicalOperatorConcept::executeChild(ExecutionContext& executionContext, CompilationContext& compilationContext, Record& record) const
{
    if (const auto child = getChild())
    {
        child.value().execute(executionContext, compilationContext, record);
    }
}

void PhysicalOperatorConcept::terminateChild(ExecutionContext& executionContext) const
{
    if (const auto child = getChild())
    {
        child.value().terminate(executionContext);
    }
}

PhysicalOperator::PhysicalOperator() = default;

PhysicalOperator::PhysicalOperator(const PhysicalOperator& other) = default;

PhysicalOperator::PhysicalOperator(PhysicalOperator&&) noexcept = default;

PhysicalOperator& PhysicalOperator::operator=(const PhysicalOperator& other)
{
    if (this != &other)
    {
        self = other.self;
    }
    return *this;
}

std::optional<PhysicalOperator> PhysicalOperator::getChild() const
{
    return self->getChild();
}

void PhysicalOperator::setChild(PhysicalOperator child) const
{
    self->setChild(std::move(child));
}

void PhysicalOperator::setup(ExecutionContext& executionContext, CompilationContext& compilationContext) const
{
    self->setup(executionContext, compilationContext);
}

void PhysicalOperator::open(ExecutionContext& executionContext, CompilationContext& compilationContext, RecordBuffer& recordBuffer) const
{
    self->open(executionContext, compilationContext, recordBuffer);
}

void PhysicalOperator::close(ExecutionContext& executionContext, CompilationContext& compilationContext, RecordBuffer& recordBuffer) const
{
    self->close(executionContext, compilationContext, recordBuffer);
}

void PhysicalOperator::terminate(ExecutionContext& executionContext) const
{
    self->terminate(executionContext);
}

void PhysicalOperator::execute(ExecutionContext& executionContext, CompilationContext& compilationContext, Record& record) const
{
    self->execute(executionContext, compilationContext, record);
}

std::string PhysicalOperator::toString() const
{
    return self->toString();
}

[[nodiscard]] OperatorId PhysicalOperator::getId() const
{
    return self->id;
}

PhysicalOperatorWrapper::PhysicalOperatorWrapper(PhysicalOperator physicalOperator, Schema inputSchema, Schema outputSchema)
    : physicalOperator(std::move(physicalOperator))
    , inputSchema(inputSchema)
    , outputSchema(outputSchema)
    , pipelineLocation(PipelineLocation::INTERMEDIATE)
{
}

PhysicalOperatorWrapper::PhysicalOperatorWrapper(
    PhysicalOperator physicalOperator, Schema inputSchema, Schema outputSchema, PipelineLocation pipelineLocation)
    : physicalOperator(std::move(physicalOperator))
    , inputSchema(inputSchema)
    , outputSchema(outputSchema)
    , pipelineLocation(pipelineLocation)
{
}

PhysicalOperatorWrapper::PhysicalOperatorWrapper(
    PhysicalOperator physicalOperator,
    Schema inputSchema,
    Schema outputSchema,
    std::optional<OperatorHandlerId> handlerId,
    std::optional<std::shared_ptr<OperatorHandler>> handler,
    PipelineLocation pipelineLocation)
    : physicalOperator(std::move(std::move(physicalOperator)))
    , inputSchema(inputSchema)
    , outputSchema(outputSchema)
    , handler(std::move(std::move(handler)))
    , handlerId(handlerId)
    , pipelineLocation(pipelineLocation)
{
}

PhysicalOperatorWrapper::PhysicalOperatorWrapper(
    PhysicalOperator physicalOperator,
    Schema inputSchema,
    Schema outputSchema,
    std::optional<OperatorHandlerId> handlerId,
    std::optional<std::shared_ptr<OperatorHandler>> handler,
    PipelineLocation pipelineLocation,
    std::vector<std::shared_ptr<PhysicalOperatorWrapper>> children)
    : physicalOperator(std::move(std::move(physicalOperator)))
    , inputSchema(inputSchema)
    , outputSchema(outputSchema)
    , children(std::move(children))
    , handler(std::move(std::move(handler)))
    , handlerId(handlerId)
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
