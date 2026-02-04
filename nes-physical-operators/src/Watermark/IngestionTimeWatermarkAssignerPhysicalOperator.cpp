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
#include <Watermark/IngestionTimeWatermarkAssignerPhysicalOperator.hpp>

#include <optional>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Watermark/TimeFunction.hpp>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

IngestionTimeWatermarkAssignerPhysicalOperator::IngestionTimeWatermarkAssignerPhysicalOperator(IngestionTimeFunction timeFunction)
    : timeFunction(std::move(timeFunction)) { };

std::optional<PhysicalOperator> IngestionTimeWatermarkAssignerPhysicalOperator::getChild() const
{
    return child;
}

IngestionTimeWatermarkAssignerPhysicalOperator IngestionTimeWatermarkAssignerPhysicalOperator::withChild(PhysicalOperator child) const
{
    auto copy = *this;
    copy.child = std::move(child);
    return copy;
}

void IngestionTimeWatermarkAssignerPhysicalOperator::setup(ExecutionContext& ctx, CompilationContext& compCtx) const
{
    setupChild(ctx, compCtx);
}

void IngestionTimeWatermarkAssignerPhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    openChild(executionCtx, recordBuffer);
    timeFunction.open(executionCtx, recordBuffer);
    auto emptyRecord = Record();
    const auto tsField = [this](ExecutionContext& executionCtx)
    {
        auto emptyRecord = Record();
        return timeFunction.getTs(executionCtx, emptyRecord);
    }(executionCtx);
    if (const auto currentWatermark = executionCtx.watermarkTs; tsField > currentWatermark)
    {
        executionCtx.watermarkTs = tsField;
    }
}

void IngestionTimeWatermarkAssignerPhysicalOperator::close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const
{
    closeChild(ctx, recordBuffer);
}

void IngestionTimeWatermarkAssignerPhysicalOperator::terminate(ExecutionContext& ctx) const
{
    terminateChild(ctx);
}

void IngestionTimeWatermarkAssignerPhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    executeChild(executionCtx, record);
}

OperatorId IngestionTimeWatermarkAssignerPhysicalOperator::getId() const
{
    return id;
}

void IngestionTimeWatermarkAssignerPhysicalOperator::setupChild(
    ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    INVARIANT(child.has_value(), "Child operator is not set");
    child.value().setup(executionCtx, compilationContext);
}

void IngestionTimeWatermarkAssignerPhysicalOperator::openChild(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    INVARIANT(child.has_value(), "Child operator is not set");
    child.value().open(executionCtx, recordBuffer);
}

void IngestionTimeWatermarkAssignerPhysicalOperator::closeChild(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    INVARIANT(child.has_value(), "Child operator is not set");
    child.value().close(executionCtx, recordBuffer);
}

void IngestionTimeWatermarkAssignerPhysicalOperator::executeChild(ExecutionContext& executionCtx, Record& record) const
{
    INVARIANT(child.has_value(), "Child operator is not set");
    child.value().execute(executionCtx, record);
}

void IngestionTimeWatermarkAssignerPhysicalOperator::terminateChild(ExecutionContext& executionCtx) const
{
    INVARIANT(child.has_value(), "Child operator is not set");
    child.value().terminate(executionCtx);
}
}
