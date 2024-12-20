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

#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/OperatorState.hpp>
#include <Execution/Operators/Watermark/EventTimeWatermarkAssignment.hpp>
#include <Execution/Operators/Watermark/TimeFunction.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Common.hpp>

namespace NES::Runtime::Execution::Operators
{

struct WatermarkState final : OperatorState
{
    explicit WatermarkState() = default;
    nautilus::val<Timestamp> currentWatermark = Timestamp(Runtime::Timestamp::INITIAL_VALUE);
};

EventTimeWatermarkAssignment::EventTimeWatermarkAssignment(TimeFunctionPtr timeFunction) : timeFunction(std::move(timeFunction)){};

void EventTimeWatermarkAssignment::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    Operator::open(executionCtx, recordBuffer);
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
    child->execute(ctx, record);
}

void EventTimeWatermarkAssignment::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    PRECONDITION(
        NES::Util::instanceOf<const WatermarkState>(*executionCtx.getLocalState(this)),
        "Expects the local state to be of type WatermarkState");
    const auto state = static_cast<WatermarkState*>(executionCtx.getLocalState(this));
    recordBuffer.setWatermarkTs(state->currentWatermark);
    executionCtx.watermarkTs = state->currentWatermark;
    Operator::close(executionCtx, recordBuffer);
}

}
