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
#include <Execution/Aggregation/AvgAggregation.hpp>
#include <Execution/Aggregation/CountAggregation.hpp>
#include <Execution/Aggregation/MaxAggregation.hpp>
#include <Execution/Aggregation/MinAggregation.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/ThresholdWindow/KeyedThresholdWindow/KeyedThresholdWindow.hpp>
#include <Execution/Operators/ThresholdWindow/KeyedThresholdWindow/KeyedThresholdWindowOperatorHandler.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES::Runtime::Execution::Operators {

extern "C" void incrementKeyedThresholdWindowCount(void* state, uint32_t aggKey) {
    auto handler = (KeyedThresholdWindowOperatorHandler*) state;
    NES_TRACE("Called incrementCount: recordCount = {}", handler->keyedAggregationStates.at(aggKey).recordCount + 1);
    handler->keyedAggregationStates.at(aggKey).recordCount++;
}

extern "C" void setKeyedThresholdWindowIsWindowOpen(void* state, uint32_t aggKey, bool isWindowOpen) {
    auto handler = (KeyedThresholdWindowOperatorHandler*) state;
    NES_TRACE("Called setIsWindowOpen: {}", isWindowOpen);
    handler->keyedAggregationStates.at(aggKey).isWindowOpen = isWindowOpen;
}

extern "C" bool getIsKeyedThresholdWindowOpen(void* state, uint32_t aggKey) {
    auto handler = (KeyedThresholdWindowOperatorHandler*) state;
    NES_TRACE("Called getIsWindowOpen: isWindowOpen = {}", handler->keyedAggregationStates.at(aggKey).isWindowOpen);
    return handler->keyedAggregationStates.at(aggKey).isWindowOpen;
}

extern "C" uint64_t getKeyedThresholdWindowRecordCount(void* state, uint32_t aggKey) {
    auto handler = (KeyedThresholdWindowOperatorHandler*) state;
    NES_TRACE("Called getRecordCount: recordCount = {}", handler->keyedAggregationStates.at(aggKey).recordCount);
    return handler->keyedAggregationStates.at(aggKey).recordCount;
}

extern "C" void resetKeyedThresholdWindowCount(void* state, uint32_t aggKey) {
    auto handler = (KeyedThresholdWindowOperatorHandler*) state;
    NES_TRACE("Called resetCount");
    handler->keyedAggregationStates.at(aggKey).recordCount = 0;
}

extern "C" void lockKeyedThresholdWindowHandler(void* state, uint32_t aggKey) {
    auto handler = (KeyedThresholdWindowOperatorHandler*) state;
    NES_TRACE("Called lockWindowHandler");
    handler->keyedAggregationStates.at(aggKey).mutex.lock();
}

extern "C" void unlockKeyedThresholdWindowHandler(void* state, uint32_t aggKey) {
    auto handler = (KeyedThresholdWindowOperatorHandler*) state;
    NES_TRACE("Called unlockWindowHandler");
    handler->keyedAggregationStates.at(aggKey).mutex.unlock();
}

extern "C" void createStateIfNotExist(void* hanlderMemref, uint32_t aggKey) {
    auto handler = (KeyedThresholdWindowOperatorHandler*) hanlderMemref;
    NES_TRACE("Called createStateIfNotExist");
    // Create a key if not exist
    if (handler->keyedAggregationStates.find(aggKey) == handler->keyedAggregationStates.end()) {
        // key does not exist, create a new map entry with aggKey as key
        auto keyedThresholdWindowStates = KeyedThresholdWindowState();
        handler->keyedAggregationStates.insert(std::make_pair(aggKey, std::move(keyedThresholdWindowStates)));
    }
}

template<class T>
void addAggregationValues(void* state, uint32_t aggKey, uint32_t aggFunctionCount) {
    auto handler = (KeyedThresholdWindowOperatorHandler*) state;

    // Only add the aggregation value once per key
    if (handler->keyedAggregationStates.at(aggKey).aggregationValues.size() < aggFunctionCount) {
        auto aggVal = std::make_unique<T>();
        handler->keyedAggregationStates.at(aggKey).aggregationValues.push_back(std::move(aggVal));
    }
}

// TODO #3801: should support key data types other than uint32_t
extern "C" void* getKeyedAggregationValue(void* state, uint32_t aggKey, uint32_t aggFuncIdx) {
    auto handler = (KeyedThresholdWindowOperatorHandler*) state;
    NES_TRACE("Called getAggregationValue: for aggIdx = {} and aggKey = {}", aggFuncIdx, aggKey);
    return (void*) handler->keyedAggregationStates.at(aggKey).aggregationValues[aggFuncIdx].get();
}

KeyedThresholdWindow::KeyedThresholdWindow(
    Runtime::Execution::Expressions::ExpressionPtr predicateExpression,
    uint64_t minCount,
    const std::vector<Expressions::ExpressionPtr>& aggregatedFieldAccessExpressions,
    const Expressions::ExpressionPtr keyExpression,
    Nautilus::Record::RecordFieldIdentifier keyFieldIdentifier,
    const std::vector<Nautilus::Record::RecordFieldIdentifier>& aggregationResultFieldIdentifiers,
    const std::vector<std::shared_ptr<Aggregation::AggregationFunction>>& aggregationFunctions,
    uint64_t operatorHandlerIndex)
    : predicateExpression(std::move(predicateExpression)), aggregatedFieldAccessExpressions(aggregatedFieldAccessExpressions),
      aggregationResultFieldIdentifiers(aggregationResultFieldIdentifiers), keyExpression(keyExpression),
      keyFieldIdentifier(keyFieldIdentifier), minCount(minCount), operatorHandlerIndex(operatorHandlerIndex),
      aggregationFunctions(aggregationFunctions) {
    NES_ASSERT(this->aggregationFunctions.size() == this->aggregationResultFieldIdentifiers.size(),
               "The number of aggregation expression and aggregation functions need to be equals");
}

void KeyedThresholdWindow::execute(ExecutionContext& ctx, Record& record) const {
    NES_TRACE("Execute ThresholdWindow for received record {}", record.getAllFields().begin()->c_str())

    // derive key values
    Value<> keyValue = keyExpression->execute(record);

    // get the handler for this threshold window operator
    auto handler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);

    // Create the state for the current key if not exists
    FunctionCall("createStateIfNotExist", createStateIfNotExist, handler, keyValue.as<UInt32>());

    // Evaluate the threshold condition
    auto val = predicateExpression->execute(record);
    FunctionCall("lockWindowHandler", lockKeyedThresholdWindowHandler, handler, keyValue.as<UInt32>());

    auto aggFunctionsCount = Value<UInt32>((uint32_t) aggregationFunctions.size());
    for (uint32_t i = 0; i < aggregationFunctions.size(); ++i) {
        // check the aggregation function types
        // TODO #3801 the aggregation values and key data type should not be hard-coded
        if (std::dynamic_pointer_cast<Aggregation::SumAggregationFunction>(aggregationFunctions[i])) {
            FunctionCall("addAggregationValues",
                         addAggregationValues<Aggregation::SumAggregationValue<int64_t>>,
                         handler,
                         keyValue.as<UInt32>(),
                         aggFunctionsCount);
        } else if (std::dynamic_pointer_cast<Aggregation::MaxAggregationFunction>(aggregationFunctions[i])) {
            FunctionCall("addAggregationValues",
                         addAggregationValues<Aggregation::MaxAggregationValue<int64_t>>,
                         handler,
                         keyValue.as<UInt32>(),
                         aggFunctionsCount);
        } else if (std::dynamic_pointer_cast<Aggregation::MinAggregationFunction>(aggregationFunctions[i])) {
            FunctionCall("addAggregationValues",
                         addAggregationValues<Aggregation::MinAggregationValue<int64_t>>,
                         handler,
                         keyValue.as<UInt32>(),
                         aggFunctionsCount);
        } else if (std::dynamic_pointer_cast<Aggregation::CountAggregationFunction>(aggregationFunctions[i])) {
            FunctionCall("addAggregationValues",
                         addAggregationValues<Aggregation::CountAggregationValue<int64_t>>,
                         handler,
                         keyValue.as<UInt32>(),
                         aggFunctionsCount);
        } else if (std::dynamic_pointer_cast<Aggregation::AvgAggregationFunction>(aggregationFunctions[i])) {
            FunctionCall("addAggregationValues",
                         addAggregationValues<Aggregation::AvgAggregationValue<int64_t>>,
                         handler,
                         keyValue.as<UInt32>(),
                         aggFunctionsCount);
        } else {
            NES_ERROR("Unknown aggregation type");
            NES_THROW_RUNTIME_ERROR("Unknown aggregation type");
        }
    }

    if (val) {
        NES_TRACE("Execute ThresholdWindow for valid predicate {}", val.getValue().toString())
        for (uint32_t i = 0; i < aggregationFunctions.size(); ++i) {
            auto aggregationValueState = FunctionCall("getKeyedAggregationValue",
                                                      getKeyedAggregationValue,
                                                      handler,
                                                      keyValue.as<UInt32>(),
                                                      Value<UInt32>(i));
            aggregationFunctions[i]->lift(aggregationValueState, record);
        }

        FunctionCall("incrementKeyedThresholdWindowCount", incrementKeyedThresholdWindowCount, handler, keyValue.as<UInt32>());
        FunctionCall("setKeyedThresholdWindowIsWindowOpen",
                     setKeyedThresholdWindowIsWindowOpen,
                     handler,
                     keyValue.as<UInt32>(),
                     Value<Boolean>(true));
        FunctionCall("unlockKeyedThresholdWindowHandler", unlockKeyedThresholdWindowHandler, handler, keyValue.as<UInt32>());
    } else {
        auto isWindowOpen =
            FunctionCall("getIsKeyedThresholdWindowOpen", getIsKeyedThresholdWindowOpen, handler, keyValue.as<UInt32>());
        if (isWindowOpen) {
            auto recordCount = FunctionCall("getKeyedThresholdWindowRecordCount",
                                            getKeyedThresholdWindowRecordCount,
                                            handler,
                                            keyValue.as<UInt32>());

            if (recordCount >= minCount) {
                auto resultRecord = Record();
                for (uint32_t i = 0; i < aggregationFunctions.size(); ++i) {
                    auto aggregationValueMemref = FunctionCall("getKeyedAggregationValue",
                                                               getKeyedAggregationValue,
                                                               handler,
                                                               keyValue.as<UInt32>(),
                                                               Value<UInt32>(i));
                    aggregationFunctions[i]->lower(aggregationValueMemref, resultRecord);
                    resultRecord.write(keyFieldIdentifier, keyValue);
                    aggregationFunctions[i]->reset(aggregationValueMemref);
                }
                FunctionCall("setKeyedThresholdWindowIsWindowOpen",
                             setKeyedThresholdWindowIsWindowOpen,
                             handler,
                             keyValue.as<UInt32>(),
                             Value<Boolean>(false));
                FunctionCall("resetKeyedThresholdWindowCount", resetKeyedThresholdWindowCount, handler, keyValue.as<UInt32>());
                FunctionCall("unlockKeyedThresholdWindowHandler",
                             unlockKeyedThresholdWindowHandler,
                             handler,
                             keyValue.as<UInt32>());
                // crucial to release the handler here before we execute the rest of the pipeline
                child->execute(ctx, resultRecord);
            } else {
                // if the minCount is not reached, we still need to close the window, reset counter and release the lock if the handler
                FunctionCall("setKeyedThresholdWindowIsWindowOpen",
                             setKeyedThresholdWindowIsWindowOpen,
                             handler,
                             keyValue.as<UInt32>(),
                             Value<Boolean>(false));
                FunctionCall("resetKeyedThresholdWindowCount", resetKeyedThresholdWindowCount, handler, keyValue.as<UInt32>());
                FunctionCall("unlockKeyedThresholdWindowHandler",
                             unlockKeyedThresholdWindowHandler,
                             handler,
                             keyValue.as<UInt32>());
            }
        }// end if isWindowOpen
        else {
            // if the window is closed, we reset the counter and release the handler
            FunctionCall("resetKeyedThresholdWindowCount", resetKeyedThresholdWindowCount, handler, keyValue.as<UInt32>());
            FunctionCall("unlockKeyedThresholdWindowHandler", unlockKeyedThresholdWindowHandler, handler, keyValue.as<UInt32>());
        }
    }
}

}// namespace NES::Runtime::Execution::Operators