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
#include <optional>
#include <utility>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Common.hpp>
#include <Watermark/EventTimeWatermarkAssignerPhysicalOperator.hpp>
#include <Watermark/TimeFunction.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <OperatorState.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

struct WatermarkState final : OperatorState
{
    explicit WatermarkState() = default;
    nautilus::val<Timestamp> currentWatermark{Timestamp(Timestamp::INITIAL_VALUE)};
};

EventTimeWatermarkAssignerPhysicalOperator::EventTimeWatermarkAssignerPhysicalOperator(EventTimeFunction timeFunction)
    : timeFunction(std::move(std::move(timeFunction))) { };

void EventTimeWatermarkAssignerPhysicalOperator::open(ExecutionContext& executionContext, CompilationContext& compilationContext, RecordBuffer& recordBuffer) const
{
    openChild(executionContext, compilationContext, recordBuffer);
    executionContext.setLocalOperatorState(id, std::make_unique<WatermarkState>());
    timeFunction.open(executionContext, compilationContext, recordBuffer);
}

void EventTimeWatermarkAssignerPhysicalOperator::execute(ExecutionContext& executionContext, CompilationContext& compilationContext, Record& record) const
{
    auto* const state = dynamic_cast<WatermarkState*>(executionContext.getLocalState(id));
    const auto tsField = timeFunction.getTs(executionContext, compilationContext, record);
    if (tsField > state->currentWatermark)
    {
        state->currentWatermark = tsField;
    }
    /// call next operator
    executeChild(executionContext, compilationContext, record);
}

void EventTimeWatermarkAssignerPhysicalOperator::close(ExecutionContext& executionContext, CompilationContext& compilationContext, RecordBuffer& recordBuffer) const
{
    PRECONDITION(
        NES::Util::instanceOf<const WatermarkState>(*executionContext.getLocalState(id)),
        "Expects the local state to be of type WatermarkState");
    auto* const state = dynamic_cast<WatermarkState*>(executionContext.getLocalState(id));
    executionContext.watermarkTs = state->currentWatermark;
    PhysicalOperatorConcept::close(executionContext, compilationContext, recordBuffer);
}

std::optional<PhysicalOperator> EventTimeWatermarkAssignerPhysicalOperator::getChild() const
{
    return child;
}

void EventTimeWatermarkAssignerPhysicalOperator::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

}
