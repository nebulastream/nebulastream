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
#include <Execution/Operators/Streaming/IngestionTimeWatermarkAssignment.hpp>
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

IngestionTimeWatermarkAssignment::IngestionTimeWatermarkAssignment(TimeFunctionPtr timeFunction)
    : timeFunction(std::move(timeFunction)){};

void IngestionTimeWatermarkAssignment::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const {
    Operator::open(executionCtx, recordBuffer);
    executionCtx.setLocalOperatorState(this, std::make_unique<WatermarkState>());
    timeFunction->open(executionCtx, recordBuffer);
    auto state = (WatermarkState*) executionCtx.getLocalState(this);
    auto emptyRecord = Record();
    Value<> tsField = timeFunction->getTs(executionCtx, emptyRecord);
    if (tsField > state->currentWatermark) {
        state->currentWatermark = tsField;
    }
    // call next operator
    executionCtx.setWatermarkTs(state->currentWatermark.as<UInt64>());
}

void IngestionTimeWatermarkAssignment::execute(ExecutionContext& executionCtx, Record& record) const {
    child->execute(executionCtx, record);
}
void IngestionTimeWatermarkAssignment::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const {
    auto state = (WatermarkState*) executionCtx.getLocalState(this);
    executionCtx.setWatermarkTs(state->currentWatermark.as<UInt64>());
    Operator::close(executionCtx, recordBuffer);
}

}// namespace NES::Runtime::Execution::Operators