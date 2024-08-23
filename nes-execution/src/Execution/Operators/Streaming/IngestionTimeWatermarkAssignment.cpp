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

IngestionTimeWatermarkAssignment::IngestionTimeWatermarkAssignment(TimeFunctionPtr timeFunction)
    : timeFunction(std::move(timeFunction)){};

void IngestionTimeWatermarkAssignment::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const {
    // We have to do this here, as we do not want to set the statistic id of this build operator in the execution context
    if (hasChild()) {
        child->open(executionCtx, recordBuffer);
    }

    timeFunction->open(executionCtx, recordBuffer);
    auto emptyRecord = Record();
    auto tsField = timeFunction->getTs(executionCtx, emptyRecord);
    auto currentWatermark = executionCtx.getWatermarkTs();
    if (tsField > currentWatermark) {
        executionCtx.setWatermarkTs(tsField);
    }
}

void IngestionTimeWatermarkAssignment::execute(ExecutionContext& executionCtx, Record& record) const {
    child->execute(executionCtx, record);
}

}// namespace NES::Runtime::Execution::Operators
