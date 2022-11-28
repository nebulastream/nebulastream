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
#include <Execution/Operators/ThresholdWindow/ThresholdWindow.hpp>
#include <Execution/Operators/ThresholdWindow/ThresholdWindowOperatorHandler.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <mutex>

namespace NES::Runtime::Execution::Operators {

extern "C" void addToSumAggregate(void* state, int64_t valueToAdd) {
    auto handler = (ThresholdWindowOperatorHandler*) state;
    handler->sum = handler->sum + valueToAdd;
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

extern "C" bool getMinCountFulfilled(void* state) {
    auto handler = (ThresholdWindowOperatorHandler*) state;
    return handler->recordCount >= handler->minCount;
}

extern "C" void setSumAggregate(void* state, int64_t valueToSet) {
    auto handler = (ThresholdWindowOperatorHandler*) state;
    handler->sum = valueToSet;
}

extern "C" int64_t getSumAggregate(void* state) {
    auto handler = (ThresholdWindowOperatorHandler*) state;
    return handler->sum;
}

extern "C" void lockWindowHandler(void* state) {
    auto handler = (ThresholdWindowOperatorHandler*) state;
    std::lock_guard<std::mutex> lock(handler->mutex);
}

void NES::Runtime::Execution::Operators::ThresholdWindow::execute(ExecutionContext& ctx, Record& record) const {
    // Evaluate the threshold condition
    auto val = predicateExpression->execute(record);
    auto handler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    FunctionCall("lockWindowHandler", lockWindowHandler, handler);
    if (val) {
        auto aggregatedValue = aggregatedFieldAccessExpression->execute(record);
        FunctionCall("addToSumAggregate", addToSumAggregate, handler, aggregatedValue.as<Int64>());
        FunctionCall("setIsWindowOpen", setIsWindowOpen, handler, Value<Boolean>(true));
    } else {
        auto isWindowOpen = FunctionCall("getIsWindowOpen", getIsWindowOpen, handler);
        auto minCountFulfilled = FunctionCall("getMinCountFulfilled", getMinCountFulfilled, handler);
        if (isWindowOpen && minCountFulfilled) {
            auto sumAggregation = FunctionCall("getSumAggregate", getSumAggregate, handler);
            auto aggregationResult = Record({{aggregationResultFieldIdentifier, sumAggregation}});
            FunctionCall("setSumAggregate", setSumAggregate, handler, Value<Int64>((int64_t) 0));
            FunctionCall("setIsWindowOpen", setIsWindowOpen, handler, Value<Boolean>(false));
            child->execute(ctx, aggregationResult);
        }
    }
}

}// namespace NES::Runtime::Execution::Operators