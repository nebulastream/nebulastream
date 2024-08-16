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
#include <Execution/Aggregation/AggregationFunction.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedWindowEmitAction.hpp>

namespace NES::Runtime::Execution::Operators {

int8_t* getGlobalSliceState(void* combinedSlice);
void deleteNonKeyedSlice(void* slice);

NonKeyedWindowEmitAction::NonKeyedWindowEmitAction(
    const std::vector<std::shared_ptr<Aggregation::AggregationFunction>>& aggregationFunctions,
    const std::string& startTsFieldName,
    const std::string& endTsFieldName,
    OriginId resultOriginId)
    : aggregationFunctions(aggregationFunctions), startTsFieldName(startTsFieldName), endTsFieldName(endTsFieldName),
      resultOriginId(resultOriginId) {}

void NonKeyedWindowEmitAction::emitSlice(ExecutionContext& ctx,
                                         ExecuteOperatorPtr& child,
                                         ExecDataUInt64Ptr& windowStart,
                                         ExecDataUInt64Ptr& windowEnd,
                                         ExecDataUInt64Ptr& sequenceNumber,
                                         ExecDataUInt64Ptr& chunkNumber,
                                         ExecDataBooleanPtr& lastChunk,
                                         ObjRefVal<void>& globalSlice) const {
    ctx.setWatermarkTs(windowStart);
    ctx.setOrigin(resultOriginId.getRawValue());
    ctx.setSequenceNumber(sequenceNumber->valueAsType<uint64_t>());
    ctx.setChunkNumber(chunkNumber->valueAsType<uint64_t>());
    ctx.setLastChunk(lastChunk->valueAsType<bool>());

    auto windowState = nautilus::invoke(getGlobalSliceState, globalSlice);
    Record resultWindow;
    resultWindow.write(startTsFieldName, windowStart);
    resultWindow.write(endTsFieldName, windowEnd);
    UInt64Val stateOffset = 0;
    for (const auto& function : nautilus::static_iterable(aggregationFunctions)) {
        auto valuePtr = windowState + stateOffset;
        function->lower(valuePtr, resultWindow);
        stateOffset = stateOffset + function->getSize();
    }
    child->execute(ctx, resultWindow);

    nautilus::invoke(deleteNonKeyedSlice, globalSlice);
}
}// namespace NES::Runtime::Execution::Operators
