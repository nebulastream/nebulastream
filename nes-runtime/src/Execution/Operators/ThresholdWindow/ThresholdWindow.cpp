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

#include <Execution/Aggregation/AggregationValue.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/ThresholdWindow/ThresholdWindow.hpp>
#include <Execution/Operators/ThresholdWindow/ThresholdWindowOperatorHandler.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Record.hpp>

namespace NES::Runtime::Execution::Operators {

extern "C" void incrementCount(void* state) {
    auto handler = (ThresholdWindowOperatorHandler*) state;
    handler->recordCount++;
}

extern "C" void setIsWindowOpen(void* state, bool isWindowOpen) {
    auto handler = (ThresholdWindowOperatorHandler*) state;
    handler->isWindowOpen = isWindowOpen;
}

extern "C" bool getIsWindowOpen(void* state) {
    auto handler = (ThresholdWindowOperatorHandler*) state;
    return handler->isWindowOpen;
}

extern "C" uint64_t getRecordCount(void* state) {
    auto handler = (ThresholdWindowOperatorHandler*) state;
    return handler->recordCount;
}

extern "C" void resetCount(void* state) {
    auto handler = (ThresholdWindowOperatorHandler*) state;
    handler->recordCount = 0;
}

extern "C" void lockWindowHandler(void* state) {
    auto handler = (ThresholdWindowOperatorHandler*) state;
    handler->mutex.lock();
}

extern "C" void unlockWindowHandler(void* state) {
    auto handler = (ThresholdWindowOperatorHandler*) state;
    handler->mutex.unlock();
}

extern "C" void* getAggregationValue(void* state) {
    auto handler = (ThresholdWindowOperatorHandler*) state;
    return (void*) handler->AggregationValue.get();
}

void NES::Runtime::Execution::Operators::ThresholdWindow::execute(ExecutionContext& ctx, Record& record) const {
    // Evaluate the threshold condition
    auto val = predicateExpression->execute(record);
    auto handler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    auto aggregationValueMemref = FunctionCall("getAggregationValue", getAggregationValue, handler);

    FunctionCall("lockWindowHandler", lockWindowHandler, handler);
    if (val) {
        auto aggregatedValue = aggregatedFieldAccessExpression->execute(record);
        FunctionCall("incrementCount", incrementCount, handler);
        aggregationFunction->lift(aggregationValueMemref, aggregatedValue);
        FunctionCall("setIsWindowOpen", setIsWindowOpen, handler, Value<Boolean>(true));
    } else {
        auto isWindowOpen = FunctionCall("getIsWindowOpen", getIsWindowOpen, handler);
        if (isWindowOpen) {
            auto recordCount = FunctionCall("getRecordCount", getRecordCount, handler);
            if (recordCount >= minCount) {
                auto aggregationResult = aggregationFunction->lower(aggregationValueMemref);
                auto resultRecord = Record({{aggregationResultFieldIdentifier, aggregationResult}});

                child->execute(ctx, resultRecord);
            }
            aggregationFunction->reset(aggregationValueMemref);
            FunctionCall("setIsWindowOpen", setIsWindowOpen, handler, Value<Boolean>(false));
        }
        FunctionCall("resetCount", resetCount, handler);
    }
    FunctionCall("unlockWindowHandler", unlockWindowHandler, handler);
}

}// namespace NES::Runtime::Execution::Operators