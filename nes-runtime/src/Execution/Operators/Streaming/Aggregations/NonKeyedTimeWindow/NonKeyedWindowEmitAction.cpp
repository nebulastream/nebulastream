#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedWindowEmitAction.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>

namespace NES::Runtime::Execution::Operators {

void* getGlobalSliceState(void* combinedSlice);
void deleteNonKeyedSlice(void* slice);

NonKeyedWindowEmitAction::NonKeyedWindowEmitAction(
    const std::vector<std::shared_ptr<Aggregation::AggregationFunction>>& aggregationFunctions,
    const std::string& startTsFieldName,
    const std::string& endTsFieldName,
    uint64_t resultOriginId)
    : aggregationFunctions(aggregationFunctions), startTsFieldName(startTsFieldName), endTsFieldName(endTsFieldName),
      resultOriginId(resultOriginId) {}

void NonKeyedWindowEmitAction::emitSlice(ExecutionContext& ctx,
                                         ExecuteOperatorPtr& child,
                                         Value<UInt64>& windowStart,
                                         Value<UInt64>& windowEnd,
                                         Value<UInt64>& sequenceNumber,
                                         Value<MemRef>& globalSlice) const {
    NES_DEBUG("Emit window: {}-{}-{}", windowStart->toString(), windowEnd->toString(), sequenceNumber->toString());
    ctx.setWatermarkTs(windowEnd);
    ctx.setOrigin(resultOriginId);
    ctx.setSequenceNumber(sequenceNumber);
    auto windowState = Nautilus::FunctionCall("getNonKeyedSliceState", getGlobalSliceState, globalSlice);
    Record resultWindow;
    resultWindow.write(startTsFieldName, windowStart);
    resultWindow.write(endTsFieldName, windowEnd);
    uint64_t stateOffset = 0;
    for (const auto& function : aggregationFunctions) {
        auto valuePtr = windowState + stateOffset;
        function->lower(valuePtr.as<MemRef>(), resultWindow);
        stateOffset = stateOffset + function->getSize();
    }
    child->execute(ctx, resultWindow);

    Nautilus::FunctionCall("deleteNonKeyedSlice", deleteNonKeyedSlice, globalSlice);
}
}// namespace NES::Runtime::Execution::Operators