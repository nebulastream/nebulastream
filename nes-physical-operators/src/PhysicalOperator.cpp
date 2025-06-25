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

void PhysicalOperatorConcept::setup(ExecutionContext& executionCtx, const nautilus::engine::NautilusEngine& engine) const
{
    setupChild(executionCtx, engine);
}

void PhysicalOperatorConcept::open(ExecutionContext& executionCtx, RecordBuffer& rb) const
{
    openChild(executionCtx, rb);
}

void PhysicalOperatorConcept::close(ExecutionContext& executionCtx, RecordBuffer& rb) const
{
    closeChild(executionCtx, rb);
}

void PhysicalOperatorConcept::terminate(ExecutionContext& executionCtx) const
{
    terminateChild(executionCtx);
}

void PhysicalOperatorConcept::execute(ExecutionContext& executionCtx, Record& record) const
{
    executeChild(executionCtx, record);
}

void PhysicalOperatorConcept::setupChild(ExecutionContext& executionCtx, const nautilus::engine::NautilusEngine& engine) const
{
    if (const auto child = getChild())
    {
        child.value().setup(executionCtx, engine);
    }
}
void PhysicalOperatorConcept::openChild(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    if (const auto child = getChild())
    {
        child.value().open(executionCtx, recordBuffer);
    }
}
void PhysicalOperatorConcept::closeChild(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    if (const auto child = getChild())
    {
        child.value().close(executionCtx, recordBuffer);
    }
}
void PhysicalOperatorConcept::executeChild(ExecutionContext& executionCtx, Record& record) const
{
    if (const auto child = getChild())
    {
        child.value().execute(executionCtx, record);
    }
}

void PhysicalOperatorConcept::terminateChild(ExecutionContext& executionCtx) const
{
    if (const auto child = getChild())
    {
        child.value().terminate(executionCtx);
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

void PhysicalOperator::setChild(PhysicalOperator child)
{
    self->setChild(std::move(child));
}

void PhysicalOperator::setup(ExecutionContext& executionCtx, const nautilus::engine::NautilusEngine& engine) const
{
    self->setup(executionCtx, engine);
}

void PhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    self->open(executionCtx, recordBuffer);
}

void PhysicalOperator::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    self->close(executionCtx, recordBuffer);
}

void PhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    self->terminate(executionCtx);
}

void PhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    self->execute(executionCtx, record);
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
