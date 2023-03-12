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

#include <Execution/Operators/OutOfOrderRatio/OutOfOrderRatioOperator.hpp>
#include <Execution/Operators/OutOfOrderRatio/OutOfOrderRatioOperatorHandler.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Record.hpp>

namespace NES::Runtime::Execution::Operators {

extern "C" void lockOperatorHandler(void* state) {
    auto handler = (OutOfOrderRatioOperatorHandler*) state;
    handler->mutex.lock();
}

extern "C" void unlockOperatorHandler(void* state) {
    auto handler = (OutOfOrderRatioOperatorHandler*) state;
    handler->mutex.unlock();
}

extern "C" void incrementNumberOfRecords(void* state) {
    auto handler = (OutOfOrderRatioOperatorHandler*) state;
    handler->numberOfRecords++;
}

extern "C" uint64_t getNumberOfRecords(void* state) {
    auto handler = (OutOfOrderRatioOperatorHandler*) state;
    return handler->numberOfRecords;
}

extern "C" void incrementOutOfOrderCount(void* state) {
    auto handler = (OutOfOrderRatioOperatorHandler*) state;
    handler->numberOfOutOfOrderRecords++;
}

extern "C" uint64_t getOutOfOrderCount(void* state) {
    auto handler = (OutOfOrderRatioOperatorHandler*) state;
    return handler->numberOfOutOfOrderRecords;
}

extern "C" void setLastRecordTs(void* state, uint64_t lastRecordTs) {
    auto handler = (OutOfOrderRatioOperatorHandler*) state;
    handler->lastRecordTs = lastRecordTs;
}

extern "C" uint64_t getLastRecordTs(void* state) {
    auto handler = (OutOfOrderRatioOperatorHandler*) state;
    return handler->lastRecordTs;
}

void OutOfOrderRatioOperator::execute(ExecutionContext& ctx, Record& record) const {
    auto handler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    auto currentTs = timestampFieldAccessExpression->execute(record);

    FunctionCall("lockWindowHandler", lockOperatorHandler, handler);

    auto lastRecordTs = FunctionCall("getLastRecordTs", getLastRecordTs, handler);
    auto valCurrentTs = currentTs.as<UInt64>();

    FunctionCall("incrementNumberOfRecords", incrementNumberOfRecords, handler);

    if (valCurrentTs < lastRecordTs) {
        FunctionCall("incrementOutOfOrderCount", incrementOutOfOrderCount, handler);
    } else {
        FunctionCall("setLastRecordTs", setLastRecordTs, handler, valCurrentTs);
    }

    FunctionCall("unlockWindowHandler", unlockOperatorHandler, handler);
    child->execute(ctx, record);
}
}// namespace NES::Runtime::Execution::Operators