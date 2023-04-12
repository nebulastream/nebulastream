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

extern "C" void incrementKeyedThresholdWindowCount(void* state) {
    auto handler = (KeyedThresholdWindowOperatorHandler*) state;
    NES_TRACE("Called incrementCount: recordCount = " << handler->recordCount + 1);
    handler->recordCount++;
}

extern "C" void setKeyedThresholdWindowIsWindowOpen(void* state, bool isWindowOpen) {
    auto handler = (KeyedThresholdWindowOperatorHandler*) state;
    NES_TRACE("Called setIsWindowOpen: " << isWindowOpen);
    handler->isWindowOpen = isWindowOpen;
}

extern "C" bool getIsKeyedThresholdWindowOpen(void* state) {
    auto handler = (KeyedThresholdWindowOperatorHandler*) state;
    NES_TRACE("Called getIsWindowOpen: isWindowOpen = " << handler->isWindowOpen);
    return handler->isWindowOpen;
}

extern "C" uint64_t getKeyedThresholdWindowRecordCount(void* state) {
    auto handler = (KeyedThresholdWindowOperatorHandler*) state;
    NES_TRACE("Called getRecordCount: recordCount = " << handler->recordCount);
    return handler->recordCount;
}

extern "C" void resetKeyedThresholdWindowCount(void* state) {
    auto handler = (KeyedThresholdWindowOperatorHandler*) state;
    NES_TRACE("Called resetCount");
    handler->recordCount = 0;
    for (auto &aggregationValue : handler->AggregationValues){
        aggregationValue.clear();
    }
}

extern "C" void lockKeyedThresholdWindowHandler(void* state) {
    auto handler = (KeyedThresholdWindowOperatorHandler*) state;
    NES_TRACE("Called lockWindowHandler");
    handler->mutex.lock();
}

extern "C" void unlockKeyedThresholdWindowHandler(void* state) {
    auto handler = (KeyedThresholdWindowOperatorHandler*) state;
    NES_TRACE("Called unlockWindowHandler");
    handler->mutex.unlock();
}

extern "C" void*
getKeyedThresholdWindowAggregationValueOrCreate(void* state, uint64_t aggFuncIndex, uint32_t aggKey, uint8_t aggFunc) {
    (void) aggFunc;
    auto handler = (KeyedThresholdWindowOperatorHandler*) state;
    NES_TRACE("Called AggregationValue of aggFuncIndex: " << aggFuncIndex << " and aggKey: " << aggKey);

    // Check if key exists
    if (handler->AggregationValues[aggFuncIndex].find(aggKey) != handler->AggregationValues[aggFuncIndex].end()) {
        // key exist, retrieve the key
        return (void*) handler->AggregationValues[aggFuncIndex].at(aggKey).get();
    } else {
        // key does not exist, create a new map entry with aggKey as key
        // TODO #3608: find a better way to propagate the aggregation type and the data type as well
        if (aggFunc == 0) {
            auto aggVal = std::make_unique<Aggregation::SumAggregationValue<uint64_t>>();
            handler->AggregationValues[aggFuncIndex].insert(std::make_pair(aggKey, std::move(aggVal)));
        } else if (aggFunc == 1) {
            auto aggVal = std::make_unique<Aggregation::SumAggregationValue<uint64_t>>();
            handler->AggregationValues[aggFuncIndex].insert(std::make_pair(aggKey, std::move(aggVal)));
        } else if (aggFunc == 2) {
            auto aggVal = std::make_unique<Aggregation::CountAggregationValue<uint64_t>>();
            handler->AggregationValues[aggFuncIndex].insert(std::make_pair(aggKey, std::move(aggVal)));
        } else if (aggFunc == 3) {
            auto aggVal = std::make_unique<Aggregation::MinAggregationValue<uint64_t>>();
            handler->AggregationValues[aggFuncIndex].insert(std::make_pair(aggKey, std::move(aggVal)));
        } else if (aggFunc == 4) {
            auto aggVal = std::make_unique<Aggregation::AvgAggregationValue<uint64_t>>();
            handler->AggregationValues[aggFuncIndex].insert(std::make_pair(aggKey, std::move(aggVal)));
        } else {
            throw std::runtime_error("Unknown aggregation type");
        }

        // return the aggregation value
        return (void*) handler->AggregationValues[aggFuncIndex].at(aggKey).get();
    }
}

KeyedThresholdWindow::KeyedThresholdWindow(
    Runtime::Execution::Expressions::ExpressionPtr predicateExpression,
    uint64_t minCount,
    const std::vector<Expressions::ExpressionPtr>& aggregatedFieldAccessExpressions,
    const Expressions::ExpressionPtr keyExpression,
    const std::vector<Nautilus::Record::RecordFieldIdentifier>& aggregationResultFieldIdentifiers,
    const std::vector<std::shared_ptr<Aggregation::AggregationFunction>>& aggregationFunctions,
    std::unique_ptr<Nautilus::Interface::HashFunction> hashFunction,
    uint64_t operatorHandlerIndex)
    : predicateExpression(std::move(predicateExpression)), aggregatedFieldAccessExpressions(aggregatedFieldAccessExpressions),
      aggregationResultFieldIdentifiers(aggregationResultFieldIdentifiers), keyExpression(keyExpression), minCount(minCount),
      operatorHandlerIndex(operatorHandlerIndex), aggregationFunctions(aggregationFunctions),
      hashFunction(std::move(hashFunction)) {
    NES_ASSERT(this->aggregationFunctions.size() == this->aggregationResultFieldIdentifiers.size(),
               "The number of aggregation expression and aggregation functions need to be equals");
}

void KeyedThresholdWindow::execute(ExecutionContext& ctx, Record& record) const {
    NES_TRACE("Execute ThresholdWindow for received record " << record.getAllFields().begin()->c_str())

    // XX. derive key values
    Value<> keyValue = keyExpression->execute(record);

    // XX. calculate hash
    auto hash = hashFunction->calculate(keyValue);

    // Evaluate the threshold condition
    auto val = predicateExpression->execute(record);
    auto handler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    FunctionCall("lockWindowHandler", lockKeyedThresholdWindowHandler, handler);
    if (val) {
        NES_TRACE("Execute ThresholdWindow for valid predicate " << val.getValue().toString())
        for (uint64_t i = 0; i < aggregationFunctions.size(); ++i) {
            auto aggregationValueMemref = FunctionCall("getKeyedThresholdWindowAggregationValueOrCreate",
                                                       getKeyedThresholdWindowAggregationValueOrCreate,
                                                       handler,
                                                       Value<UInt64>(i),
                                                       hash,
                                                       Value<UInt8>((uint8_t) 0));
            auto aggregatedValue = Value<Int64>((int64_t) 1);// default value to aggregate (i.e., for countAgg)
            auto isCountAggregation = std::dynamic_pointer_cast<Aggregation::CountAggregationFunction>(aggregationFunctions[i]);
            // if the agg function is not a count, then get the aggregated value from the "onField" field
            // otherwise, just increment the count
            if (!isCountAggregation) {
                aggregatedValue = aggregatedFieldAccessExpressions[i]->execute(record);
            }
            NES_TRACE("lift the following value to agg" << aggregatedValue);
            aggregationFunctions[i]->lift(aggregationValueMemref, aggregatedValue);
        }
        FunctionCall("incrementKeyedThresholdWindowCount", incrementKeyedThresholdWindowCount, handler);
        FunctionCall("setKeyedThresholdWindowIsWindowOpen", setKeyedThresholdWindowIsWindowOpen, handler, Value<Boolean>(true));
        FunctionCall("unlockKeyedThresholdWindowHandler", unlockKeyedThresholdWindowHandler, handler);
    } else {
        auto isWindowOpen = FunctionCall("getIsKeyedThresholdWindowOpen", getIsKeyedThresholdWindowOpen, handler);
        if (isWindowOpen) {
            auto recordCount = FunctionCall("getKeyedThresholdWindowRecordCount", getKeyedThresholdWindowRecordCount, handler);
            if (recordCount >= minCount) {
                auto resultRecord = Record();
                for (uint64_t i = 0; i < aggregationFunctions.size(); ++i) {
                    // TODO #3608: an ugly way to state the aggregation type
                    uint8_t aggType = 0;
                    // determine the type of agg function
                    // TODO: #3608 also check the data type of the input and output of the aggregation
                    if (std::dynamic_pointer_cast<Aggregation::SumAggregationFunction>(aggregationFunctions[i])) {
                        aggType = (uint8_t) 0;
                    } else if (std::dynamic_pointer_cast<Aggregation::CountAggregationFunction>(aggregationFunctions[i])) {
                        aggType = (uint8_t) 1;
                    } else if (std::dynamic_pointer_cast<Aggregation::MaxAggregationFunction>(aggregationFunctions[i])) {
                        aggType = (uint8_t) 2;
                    } else if (std::dynamic_pointer_cast<Aggregation::MinAggregationFunction>(aggregationFunctions[i])) {
                        aggType = (uint8_t) 3;
                    } else if (std::dynamic_pointer_cast<Aggregation::AvgAggregationFunction>(aggregationFunctions[i])) {
                        aggType = (uint8_t) 4;
                    }

                    auto aggregationValueMemref = FunctionCall("getKeyedThresholdWindowAggregationValueOrCreate",
                                                               getKeyedThresholdWindowAggregationValueOrCreate,
                                                               handler,
                                                               Value<UInt64>(i),
                                                               hash,
                                                               Value<UInt8>(aggType));
                    auto aggregationResult = aggregationFunctions[i]->lower(aggregationValueMemref);
                    NES_TRACE("Write back result for" << aggregationResultFieldIdentifiers[i].c_str()
                                                      << "result: " << aggregationResult)
                    resultRecord.write(aggregationResultFieldIdentifiers[i], aggregationResult);
                    resultRecord.write("key", keyValue);//TODO: 3608: maybe the "key" field name should be inferred
                    aggregationFunctions[i]->reset(aggregationValueMemref);
                }
                FunctionCall("setKeyedThresholdWindowIsWindowOpen",
                             setKeyedThresholdWindowIsWindowOpen,
                             handler,
                             Value<Boolean>(false));
                FunctionCall("resetKeyedThresholdWindowCount", resetKeyedThresholdWindowCount, handler);
                FunctionCall("unlockKeyedThresholdWindowHandler", unlockKeyedThresholdWindowHandler, handler);
                // crucial to release the handler here before we execute the rest of the pipeline
                child->execute(ctx, resultRecord);
            } else {
                // if the minCount is not reached, we still need to close the window, reset counter and release the lock if the handler
                FunctionCall("setKeyedThresholdWindowIsWindowOpen",
                             setKeyedThresholdWindowIsWindowOpen,
                             handler,
                             Value<Boolean>(false));
                FunctionCall("resetKeyedThresholdWindowCount", resetKeyedThresholdWindowCount, handler);
                FunctionCall("unlockKeyedThresholdWindowHandler", unlockKeyedThresholdWindowHandler, handler);
            }
        }// end if isWindowOpen
        else {
            // if the window is closed, we reset the counter and release the handler
            FunctionCall("resetKeyedThresholdWindowCount", resetKeyedThresholdWindowCount, handler);
            FunctionCall("unlockKeyedThresholdWindowHandler", unlockKeyedThresholdWindowHandler, handler);
        }
    }
}

}// namespace NES::Runtime::Execution::Operators