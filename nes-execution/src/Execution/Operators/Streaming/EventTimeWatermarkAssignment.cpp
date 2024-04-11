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
#include <Execution/Operators/Streaming/EventTimeWatermarkAssignment.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Util/StdInt.hpp>

namespace NES::Runtime::Execution::Operators {

class WatermarkState : public OperatorState {
  public:
    explicit WatermarkState() {}
    Value<> currentWatermark = Value<UInt64>(0_u64);
};

EventTimeWatermarkAssignment::EventTimeWatermarkAssignment(TimeFunctionPtr timeFunction)
    : timeFunction(std::move(timeFunction)){};

void EventTimeWatermarkAssignment::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const {
    // We have to do this here, as we do not want to set the statistic id of this build operator in the execution context
    if (hasChild()) {
        child->open(executionCtx, recordBuffer);
    }
    executionCtx.setLocalOperatorState(this, std::make_unique<WatermarkState>());
    timeFunction->open(executionCtx, recordBuffer);
}

void EventTimeWatermarkAssignment::execute(ExecutionContext& ctx, Record& record) const {
    auto state = (WatermarkState*) ctx.getLocalState(this);
    Value<> tsField = timeFunction->getTs(ctx, record);
    if (tsField > state->currentWatermark) {
        state->currentWatermark = tsField;
    }
    // call next operator
    child->execute(ctx, record);
}
void EventTimeWatermarkAssignment::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const {
    auto state = (WatermarkState*) executionCtx.getLocalState(this);
    executionCtx.setWatermarkTs(state->currentWatermark.as<UInt64>());
    Operator::close(executionCtx, recordBuffer);
}

}// namespace NES::Runtime::Execution::Operators