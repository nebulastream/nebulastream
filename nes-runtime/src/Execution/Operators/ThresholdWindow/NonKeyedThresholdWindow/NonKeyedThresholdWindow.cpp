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
#include <Execution/Aggregation/CountAggregation.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/ThresholdWindow/NonKeyedThresholdWindow/NonKeyedThresholdWindow.hpp>
#include <Execution/Operators/ThresholdWindow/NonKeyedThresholdWindow/NonKeyedThresholdWindowOperatorHandler.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Runtime::Execution::Operators {

extern "C" void incrementCount(void* state) {
    auto handler = (NonKeyedThresholdWindowOperatorHandler*) state;
    NES_TRACE("Called incrementCount: recordCount = {}", handler->recordCount + 1);
    handler->recordCount++;
}

extern "C" void setIsWindowOpen(void* state, bool isWindowOpen) {
    auto handler = (NonKeyedThresholdWindowOperatorHandler*) state;
    NES_TRACE("Called setIsWindowOpen: {}", isWindowOpen);
    handler->isWindowOpen = isWindowOpen;
}

extern "C" bool getIsWindowOpen(void* state) {
    auto handler = (NonKeyedThresholdWindowOperatorHandler*) state;
    NES_TRACE("Called getIsWindowOpen: isWindowOpen = {}", handler->isWindowOpen);
    return handler->isWindowOpen;
}

extern "C" uint64_t getRecordCount(void* state) {
    auto handler = (NonKeyedThresholdWindowOperatorHandler*) state;
    NES_TRACE("Called getRecordCount: recordCount = {}", handler->recordCount);
    return handler->recordCount;
}

extern "C" void resetCount(void* state) {
    auto handler = (NonKeyedThresholdWindowOperatorHandler*) state;
    NES_TRACE("Called resetCount");
    handler->recordCount = 0;
}

extern "C" void lockWindowHandler(void* state) {
    auto handler = (NonKeyedThresholdWindowOperatorHandler*) state;
    NES_TRACE("Called lockWindowHandler");
    handler->mutex.lock();
}

extern "C" void unlockWindowHandler(void* state) {
    auto handler = (NonKeyedThresholdWindowOperatorHandler*) state;
    NES_TRACE("Called unlockWindowHandler");
    handler->mutex.unlock();
}

extern "C" void* getAggregationValue(void* state, uint64_t i) {
    auto handler = (NonKeyedThresholdWindowOperatorHandler*) state;
    NES_TRACE("Called getAggregationValue: i = {}", i);
    return (void*) handler->AggregationValues[i].get();
}

NonKeyedThresholdWindow::NonKeyedThresholdWindow(
    Runtime::Execution::Expressions::ExpressionPtr predicateExpression,
    const std::vector<Nautilus::Record::RecordFieldIdentifier>& aggregationResultFieldIdentifiers,
    uint64_t minCount,
    const std::vector<std::shared_ptr<Aggregation::AggregationFunction>>& aggregationFunctions,
    uint64_t operatorHandlerIndex)
    : predicateExpression(std::move(predicateExpression)), aggregationResultFieldIdentifiers(aggregationResultFieldIdentifiers),
      minCount(minCount), operatorHandlerIndex(operatorHandlerIndex), aggregationFunctions(aggregationFunctions) {}

void NonKeyedThresholdWindow::execute(ExecutionContext& ctx, Record& record) const {
    NES_TRACE("Execute ThresholdWindow for received record {}", record.getAllFields().begin()->c_str())
    // Evaluate the threshold condition
    auto val = predicateExpression->execute(record);
    auto handler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    FunctionCall("lockWindowHandler", lockWindowHandler, handler);
    if (val) {
        NES_TRACE("Execute ThresholdWindow for valid predicate {}", val.getValue().toString())
        // Log the start of a threshold window
        auto allFieldNames = record.getAllFields();
        if (!FunctionCall("getIsWindowOpen", getIsWindowOpen, handler)) {
            NES_DEBUG("Threshold window starts, opening value:{}",
                      std::accumulate(allFieldNames.begin(),
                                      allFieldNames.end(),
                                      std::string{},
                                      [&record](std::string acc, std::string s) {
                                          return acc + s + "=" + record.read(s)->toString() + ",";
                                      }));
        }

        for (uint64_t i = 0; i < aggregationFunctions.size(); ++i) {
            auto aggregationValueState = FunctionCall("getAggregationValue", getAggregationValue, handler, Value<UInt64>(i));
            aggregationFunctions[i]->lift(aggregationValueState, record);
        }
        FunctionCall("incrementCount", incrementCount, handler);
        FunctionCall("setIsWindowOpen", setIsWindowOpen, handler, Value<Boolean>(true));
        FunctionCall("unlockWindowHandler", unlockWindowHandler, handler);
    } else {
        auto isWindowOpen = FunctionCall("getIsWindowOpen", getIsWindowOpen, handler);
        if (isWindowOpen) {
            auto recordCount = FunctionCall("getRecordCount", getRecordCount, handler);
            if (recordCount >= minCount) {
                auto resultRecord = Record();
                for (uint64_t i = 0; i < aggregationFunctions.size(); ++i) {
                    auto aggregationValueMemref =
                        FunctionCall("getAggregationValue", getAggregationValue, handler, Value<UInt64>(i));
                    aggregationFunctions[i]->lower(aggregationValueMemref, resultRecord);
                    aggregationFunctions[i]->reset(aggregationValueMemref);
                }
                FunctionCall("setIsWindowOpen", setIsWindowOpen, handler, Value<Boolean>(false));
                FunctionCall("resetCount", resetCount, handler);
                FunctionCall("unlockWindowHandler", unlockWindowHandler, handler);
                // Log the closing of window and along with agg result
                auto aggregatedValue = Value<Int64>(1_s64);// default value to aggregate (i.e., for countAgg)
                auto allFieldNames = record.getAllFields();
                NES_DEBUG("Threshold window ends, closing value:{} | aggVal:{}",
                          std::accumulate(allFieldNames.begin(),
                                          allFieldNames.end(),
                                          std::string{},
                                          [&record](std::string acc, std::string s) {
                                              return acc + s + "=" + record.read(s)->toString() + ",";
                                          }),
                          std::accumulate(aggregationResultFieldIdentifiers.begin(),
                                          aggregationResultFieldIdentifiers.end(),
                                          std::string{},
                                          [&resultRecord](std::string acc, std::string s) {
                                              return acc + " " + s + "=" + resultRecord.read(s)->toString();
                                          }));

                // crucial to release the handler here before we execute the rest of the pipeline
                child->execute(ctx, resultRecord);
            } else {
                // if the minCount is not reached, we still need to close the window, reset counter and release the lock if the handler
                FunctionCall("setIsWindowOpen", setIsWindowOpen, handler, Value<Boolean>(false));
                FunctionCall("resetCount", resetCount, handler);
                FunctionCall("unlockWindowHandler", unlockWindowHandler, handler);
            }
        }// end if isWindowOpen
        else {
            // if the window is closed, we reset the counter and release the handler
            FunctionCall("resetCount", resetCount, handler);
            FunctionCall("unlockWindowHandler", unlockWindowHandler, handler);
        }
    }
}

}// namespace NES::Runtime::Execution::Operators