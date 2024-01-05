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
#include <Execution/Operators/Streaming/Join/StreamJoinBuildBucketing.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <numeric>

namespace NES::Runtime::Execution::Operators {

void* getAllWindowsToFillProxy(void* ptrOpHandler,
                               uint64_t ts,
                               uint64_t workerId,
                               uint64_t joinStrategyInt,
                               uint64_t windowingStrategyInt) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = StreamJoinOperator::getSpecificOperatorHandler<StreamJoinOperatorHandlerBucketing>(ptrOpHandler,
                                                                                                         joinStrategyInt,
                                                                                                         windowingStrategyInt);
    return opHandler->getAllWindowsToFillForTs(ts, workerId);
}

// TODO ask Philipp why I can not write this directly in Nautilus. How can I cast a UINT64 to INT64?
uint64_t calcNumWindowsProxy(uint64_t ts, uint64_t windowSize, uint64_t windowSlide) {
    int64_t timestamp = ts;
    int64_t remainder = (timestamp % windowSlide);
    int64_t lastStart = (timestamp - remainder);
    int64_t lowerBound = timestamp - windowSize;

    uint64_t numWindows = 0;
    int64_t start = lastStart;
    for (; start >= 0 && start > lowerBound; start -= windowSlide) {
        numWindows++;
    }

    return numWindows;
}

Value<UInt64> StreamJoinBuildBucketing::calcNumWindows(Value<UInt64>& ts) const {
    return Nautilus::FunctionCall("calcNumWindowsProxy",
                                  calcNumWindowsProxy,
                                  ts,
                                  Value<UInt64>(windowSize),
                                  Value<UInt64>(windowSlide));
}

Value<UInt64> StreamJoinBuildBucketing::calcLastStartForTs(Value<UInt64>& ts) const {
    auto remainder = (ts % windowSlide);
    Value<UInt64> lastStart(0_u64);
    lastStart = (ts - remainder);
    return lastStart;
}

Value<UInt64> StreamJoinBuildBucketing::getMinWindowStartForTs(Value<UInt64>& ts) const {
    auto lastStart = calcLastStartForTs(ts);
    auto numWindows = calcNumWindows(ts);
    return (lastStart - (numWindows - 1_u64) * windowSlide).as<UInt64>();
}

Value<UInt64> StreamJoinBuildBucketing::getMaxWindowStartForTs(Value<UInt64>& ts) const {
    auto lastStart = calcLastStartForTs(ts);
    return (lastStart + windowSize).as<UInt64>();
}

Value<Boolean> StreamJoinBuildBucketing::checkIfLocalStateUpToDate(Value<UInt64>& ts,
                                                                   LocalStateBucketing* localStateBucketing) const {
    Value<UInt64> newMinWindowStart = getMinWindowStartForTs(ts);
    Value<UInt64> newMaxWindowEnd = getMaxWindowStartForTs(ts);

    Value<Boolean> sameMinWindowStart = (newMinWindowStart == localStateBucketing->minWindowStart).as<Boolean>();
    Value<Boolean> sameMaxWindowEnd = (newMaxWindowEnd == localStateBucketing->maxWindowEnd).as<Boolean>();
    return (sameMinWindowStart && sameMaxWindowEnd).as<Boolean>();
}

void* getDefaultMemRefProxy() { return nullptr; }

void StreamJoinBuildBucketing::open(ExecutionContext& ctx, RecordBuffer&) const {
    auto opHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    Value<MemRef> allWindowsToFill = Nautilus::FunctionCall("getDefaultMemRef", getDefaultMemRefProxy);
    ctx.setLocalOperatorState(this, std::make_unique<LocalStateBucketing>(allWindowsToFill));
}

void StreamJoinBuildBucketing::execute(ExecutionContext& ctx, Record& record) const {
    auto joinState = dynamic_cast<LocalStateBucketing*>(ctx.getLocalState(this));
    Value<UInt64> timestampVal = timeFunction->getTs(ctx, record);
    Value<UInt64> workerId = ctx.getWorkerId();

    if (!checkIfLocalStateUpToDate(timestampVal, joinState)) {
        // If the current local state is not up-to-date anymore then we have to update it
        auto opHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
        updateLocalState(joinState, opHandlerMemRef, timestampVal, workerId);
    }

    // Iterating over all windows and then inserting the current record in all windows
    for (Value<UInt64> curWindowIdx = 0_u64; curWindowIdx < joinState->numWindowsToFill; curWindowIdx = curWindowIdx + 1_u64) {
        insertRecordForWindow(joinState->allWindowsToFill, curWindowIdx, workerId, record);
    }
}

void StreamJoinBuildBucketing::updateLocalState(LocalStateBucketing* localStateBucketing,
                                                Value<MemRef>& opHandlerMemRef,
                                                Value<UInt64>& ts,
                                                Value<UInt64>& workerId) const {
    localStateBucketing->numWindowsToFill = calcNumWindows(ts);
    localStateBucketing->allWindowsToFill =
        Nautilus::FunctionCall("getAllWindowsToFillProxy",
                               getAllWindowsToFillProxy,
                               opHandlerMemRef,
                               ts,
                               workerId,
                               Value<UInt64>(to_underlying<QueryCompilation::StreamJoinStrategy>(joinStrategy)),
                               Value<UInt64>(to_underlying<QueryCompilation::WindowingStrategy>(windowingStrategy)));
    localStateBucketing->minWindowStart = getMinWindowStartForTs(ts);
    localStateBucketing->maxWindowEnd = getMaxWindowStartForTs(ts);
}

StreamJoinBuildBucketing::StreamJoinBuildBucketing(const uint64_t operatorHandlerIndex,
                                                   const SchemaPtr& schema,
                                                   const std::string& joinFieldName,
                                                   const QueryCompilation::JoinBuildSideType joinBuildSide,
                                                   const uint64_t entrySize,
                                                   TimeFunctionPtr timeFunction,
                                                   QueryCompilation::StreamJoinStrategy joinStrategy,
                                                   QueryCompilation::WindowingStrategy windowingStrategy,
                                                   uint64_t windowSize,
                                                   uint64_t windowSlide)
    : StreamJoinBuild(operatorHandlerIndex,
                      schema,
                      joinFieldName,
                      joinBuildSide,
                      entrySize,
                      std::move(timeFunction),
                      joinStrategy,
                      windowingStrategy),
      windowSize(windowSize), windowSlide(windowSlide) {}

}// namespace NES::Runtime::Execution::Operators