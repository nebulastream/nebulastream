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
                                         VarVal& windowStart,
                                         VarVal& windowEnd,
                                         VarVal& sequenceNumber,
                                         VarVal& chunkNumber,
                                         VarVal& lastChunk,
                                         ObjRefVal<void>& globalSlice) const {
    ctx.setWatermarkTs(windowStart.cast<UInt64Val>());
    ctx.setOrigin(resultOriginId.getRawValue());
    ctx.setSequenceNumber(sequenceNumber.cast<UInt64Val>());
    ctx.setChunkNumber(chunkNumber.cast<UInt64Val>());
    ctx.setLastChunk(lastChunk.cast<BooleanVal>());

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
