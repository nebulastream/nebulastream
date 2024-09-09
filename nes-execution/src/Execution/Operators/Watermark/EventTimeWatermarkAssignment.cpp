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
<<<<<<<< HEAD:nes-execution/src/Execution/Operators/Watermark/EventTimeWatermarkAssignment.cpp
#include <Execution/Operators/Watermark/EventTimeWatermarkAssignment.hpp>
#include <Execution/Operators/Watermark/TimeFunction.hpp>
========
#include <Execution/Operators/Streaming/Watermark/EventTimeWatermarkAssignment.hpp>
#include <Execution/Operators/Streaming/Watermark/TimeFunction.hpp>
>>>>>>>> 68726787a8 (chore(Execution) Moved Watermark operators to own folder):nes-execution/src/Execution/Operators/Streaming/Watermark/EventTimeWatermarkAssignment.cpp
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Util/StdInt.hpp>

namespace NES::Runtime::Execution::Operators
{

class WatermarkState : public OperatorState
{
public:
    explicit WatermarkState() { }
    nautilus::val<uint64_t> currentWatermark = 0;
};

EventTimeWatermarkAssignment::EventTimeWatermarkAssignment(TimeFunctionPtr timeFunction) : timeFunction(std::move(timeFunction)) {};

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
    const auto state = static_cast<WatermarkState*>(executionCtx.getLocalState(this));
    executionCtx.setWatermarkTs(state->currentWatermark);
    Operator::close(executionCtx, recordBuffer);
}

}
