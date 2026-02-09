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
#include <Watermark/EventTimeWatermarkAssignerPhysicalOperator.hpp>

#include <memory>
#include <optional>
#include <utility>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/TimeFunction.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

EventTimeWatermarkAssignerPhysicalOperator::EventTimeWatermarkAssignerPhysicalOperator(EventTimeFunction timeFunction)
    : timeFunction(std::move(timeFunction)) { };

void EventTimeWatermarkAssignerPhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    openChild(executionCtx, recordBuffer);
    executionCtx.watermarkTs = nautilus::val<Timestamp>(Timestamp(Timestamp::INITIAL_VALUE));
    timeFunction.open(executionCtx, recordBuffer);
}

void EventTimeWatermarkAssignerPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    const auto tsField = timeFunction.getTs(ctx, record);
    if (tsField > ctx.watermarkTs)
    {
        ctx.watermarkTs = tsField;
    }
    /// call next operator
    executeChild(ctx, record);
}

void EventTimeWatermarkAssignerPhysicalOperator::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    closeChild(executionCtx, recordBuffer);
}

void EventTimeWatermarkAssignerPhysicalOperator::setup(ExecutionContext& ctx, CompilationContext& compCtx) const
{
    setupChild(ctx, compCtx);
}

void EventTimeWatermarkAssignerPhysicalOperator::terminate(ExecutionContext& ctx) const
{
    terminateChild(ctx);
}

std::optional<PhysicalOperator> EventTimeWatermarkAssignerPhysicalOperator::getChild() const
{
    return child;
}

EventTimeWatermarkAssignerPhysicalOperator EventTimeWatermarkAssignerPhysicalOperator::withChild(PhysicalOperator child) const
{
    auto copy = *this;
    copy.child = child;
    return copy;
}

OperatorId EventTimeWatermarkAssignerPhysicalOperator::getId() const
{
    return id;
}

void EventTimeWatermarkAssignerPhysicalOperator::setupChild(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    INVARIANT(child.has_value(), "Child operator is not set");
    child.value().setup(executionCtx, compilationContext);
}

void EventTimeWatermarkAssignerPhysicalOperator::openChild(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    INVARIANT(child.has_value(), "Child operator is not set");
    child.value().open(executionCtx, recordBuffer);
}

void EventTimeWatermarkAssignerPhysicalOperator::closeChild(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    INVARIANT(child.has_value(), "Child operator is not set");
    child.value().close(executionCtx, recordBuffer);
}

void EventTimeWatermarkAssignerPhysicalOperator::executeChild(ExecutionContext& executionCtx, Record& record) const
{
    INVARIANT(child.has_value(), "Child operator is not set");
    child.value().execute(executionCtx, record);
}

void EventTimeWatermarkAssignerPhysicalOperator::terminateChild(ExecutionContext& executionCtx) const
{
    INVARIANT(child.has_value(), "Child operator is not set");
    child.value().terminate(executionCtx);
}

}
