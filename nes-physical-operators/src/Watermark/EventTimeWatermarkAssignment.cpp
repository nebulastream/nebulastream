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
#include <Abstract/PhysicalOperator.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Common.hpp>
#include <Watermark/EventTimeWatermarkAssignment.hpp>
#include <Watermark/TimeFunction.hpp>
#include <ExecutionContext.hpp>
#include <OperatorState.hpp>

namespace NES
{

struct WatermarkState final : OperatorState
{
    explicit WatermarkState() = default;
    nautilus::val<Timestamp> currentWatermark = Timestamp(Timestamp::INITIAL_VALUE);
};

EventTimeWatermarkAssignment::EventTimeWatermarkAssignment(std::unique_ptr<TimeFunction> timeFunction)
    : timeFunction(std::move(timeFunction)) {};

void EventTimeWatermarkAssignment::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    PhysicalOperatorConcept::open(executionCtx, recordBuffer);
    executionCtx.setLocalOperatorState(this, std::make_unique<WatermarkState>());
    timeFunction->open(executionCtx, recordBuffer);
}

void EventTimeWatermarkAssignment::execute(ExecutionContext& ctx, Record& record) const
{
    const auto state = static_cast<WatermarkState*>(ctx.getLocalState(this));
    const auto tsField = timeFunction->getTs(ctx, record);
    if (tsField > state->currentWatermark)
    {
        state->currentWatermark = tsField;
    }
    /// call next operator
    PhysicalOperatorConcept::execute(ctx, record);
}

void EventTimeWatermarkAssignment::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    PRECONDITION(
        NES::Util::instanceOf<const WatermarkState>(*executionCtx.getLocalState(this)),
        "Expects the local state to be of type WatermarkState");
    const auto state = static_cast<WatermarkState*>(executionCtx.getLocalState(this));
    executionCtx.watermarkTs = state->currentWatermark;
    PhysicalOperatorConcept::close(executionCtx, recordBuffer);
}

}
