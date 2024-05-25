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
#include <Execution/Operators/Streaming/StatisticCollection/CountMin/CountMinBuild.hpp>
#include <Execution/Operators/Streaming/StatisticCollection/CountMin/CountMinOperatorHandler.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Hash/H3Hash.hpp>
#include <Statistics/StatisticKey.hpp>
#include <Statistics/Synopses/CountMinStatistic.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Runtime::Execution::Operators {

void* getCountMinRefProxy(void* ptrOpHandler,
                          Statistic::StatisticMetricHash metricHash,
                          StatisticId statisticId,
                          WorkerThreadId workerThreadId,
                          uint64_t timestamp) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<CountMinOperatorHandler*>(ptrOpHandler);

    const auto statisticHash = Statistic::StatisticKey::combineStatisticIdWithMetricHash(metricHash, statisticId);
    return opHandler->getStatistic(workerThreadId, statisticHash, timestamp).get();
}

void* getCountersRefProxy(void* ptrCountMin) {
    NES_ASSERT2_FMT(ptrCountMin != nullptr, "opHandler context should not be null!");
    auto* countMin = static_cast<Statistic::CountMinStatistic*>(ptrCountMin);
    return countMin->getStatisticData();
}

void* getH3SeedsProxy(void* ptrOpHandler) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<CountMinOperatorHandler*>(ptrOpHandler);

    // This const_cast is fine, as we do not change the values
    return reinterpret_cast<int8_t*>(const_cast<uint64_t*>(opHandler->getH3Seeds().data()));
}

void checkCountMinSketchesSendingProxy(void* ptrOpHandler,
                                       void* ptrPipelineCtx,
                                       uint64_t watermarkTs,
                                       uint64_t sequenceNumber,
                                       uint64_t chunkNumber,
                                       bool lastChunk,
                                       uint64_t originId,
                                       Statistic::StatisticMetricHash metricHash,
                                       StatisticId statisticId) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    NES_ASSERT2_FMT(ptrPipelineCtx != nullptr, "pipeline context should not be null");
    auto* opHandler = static_cast<CountMinOperatorHandler*>(ptrOpHandler);
    auto* pipelineCtx = static_cast<PipelineExecutionContext*>(ptrPipelineCtx);

    // Calling the operator handler method now
    const BufferMetaData bufferMetaData(watermarkTs, {sequenceNumber, chunkNumber, lastChunk}, OriginId(originId));
    const auto statisticHash = Statistic::StatisticKey::combineStatisticIdWithMetricHash(metricHash, statisticId);
    opHandler->checkStatisticsSending(bufferMetaData, statisticHash, pipelineCtx);
}

void CountMinBuild::updateLocalState(ExecutionContext& ctx, CountMinLocalState& localState, const Value<UInt64>& timestamp) const {
    // Check, if we have to increment the old synopsis reference with the currentIncrement of the local state
    if (localState.currentIncrement > 0_u64) {
        Nautilus::FunctionCall("incrementObservedTuplesStatisticProxy",
                               incrementObservedTuplesStatisticProxy,
                               localState.synopsisReference,
                               localState.currentIncrement);
    }

    // We have to get the slice and the memRef for the counters for the current timestamp
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    auto workerId = ctx.getWorkerId();
    auto sliceReference = Nautilus::FunctionCall("getCountMinRefProxy",
                                                 getCountMinRefProxy,
                                                 operatorHandlerMemRef,
                                                 Value<UInt64>(metricHash),
                                                 ctx.getCurrentStatisticId(),
                                                 workerId,
                                                 timestamp);

    auto countersReference = Nautilus::FunctionCall("getCountersRefProxy",
                                                    getCountersRefProxy,
                                                    sliceReference);

    // We have to get the synopsis start and end timestamp
    auto startTs = Nautilus::FunctionCall("getSynopsisStartProxy",
                                         getSynopsisStartProxy,
                                         operatorHandlerMemRef,
                                         Value<UInt64>(metricHash),
                                         ctx.getCurrentStatisticId(),
                                         workerId,
                                         timestamp);
    auto endTs = Nautilus::FunctionCall("getSynopsisEndProxy",
                                        getSynopsisEndProxy,
                                        operatorHandlerMemRef,
                                        Value<UInt64>(metricHash),
                                        ctx.getCurrentStatisticId(),
                                        workerId,
                                        timestamp);

    // Updating the local join state
    localState.synopsisStartTs = startTs;
    localState.synopsisEndTs = endTs;
    localState.synopsisReference = sliceReference;
    localState.countersMemRef = countersReference;
    localState.currentIncrement = 0_u64;
}

void CountMinBuild::execute(ExecutionContext& ctx, Record& record) const {
    auto countMinLocalState = dynamic_cast<CountMinLocalState*>(ctx.getLocalState(this));


    // 1. Get the memRef to the CountMin sketch, if not in the local state
    const auto timestampVal = timeFunction->getTs(ctx, record);
    if (!countMinLocalState->correctSynopsesForTimestamp(timestampVal)) {
        updateLocalState(ctx, *countMinLocalState, timestampVal);
    }

    // 2. Updating the count min sketch for this record
    for (Value<UInt64> row(0_u64); row < depth; row = row + 1_u64) {
        // 2.1 We calculate a MemRef to the first h3Seeds of the current row
        Value<UInt64> h3SeedsOffSet((row * sizeOfOneRowInBytes).as<UInt64>());
        Value<MemRef> h3SeedsThisRow = (countMinLocalState->h3SeedsMemRef + h3SeedsOffSet).as<MemRef>();

        // 2.2. Hashing the current value
        auto valToTrack = record.read(fieldToTrackFieldName);
        Value<UInt64> calcHash = h3HashFunction->calculateWithState(valToTrack, h3SeedsThisRow);
        Value<UInt64> col = (calcHash % Value<UInt64>(width)).as<UInt64>();


        // 2.3. Calculating the memory address of the counter we want to increment
        using namespace Statistic;
        Value<UInt64> counterOffset = (((col) + (row * width)) * (uint64_t)sizeof(CountMinStatistic::COUNTER_DATA_TYPE)).as<UInt64>();
        Value<MemRef> counterMemRef = (countMinLocalState->countersMemRef + counterOffset).as<MemRef>();

        // 2.4. Incrementing the counter by one
        counterMemRef.store(counterMemRef.load<UInt64>() + 1_u64);
    }

    // 3. Incrementing the current increment of the local state so that we reduce the number of function calls
    countMinLocalState->currentIncrement = countMinLocalState->currentIncrement + 1_u64;
}

void CountMinBuild::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const {
    // We have to do this here, as we do not want to set the statistic id of this build operator in the execution context
    if (hasChild()) {
        child->open(executionCtx, recordBuffer);
    }

    // Creating a count min local state
    auto nullPtrRef1 = Nautilus::FunctionCall("getNullPtrMemRefProxy", getNullPtrMemRefProxy);
    auto nullPtrRef2 = Nautilus::FunctionCall("getNullPtrMemRefProxy", getNullPtrMemRefProxy);
    auto nullPtrRef3 = Nautilus::FunctionCall("getNullPtrMemRefProxy", getNullPtrMemRefProxy);
    auto localState = std::make_unique<CountMinLocalState>(nullPtrRef1, nullPtrRef2, nullPtrRef3);

    // Adding the h3SeedsMemRef to the local state
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    auto h3SeedsMemRef = Nautilus::FunctionCall("getH3SeedsProxy", getH3SeedsProxy, operatorHandlerMemRef);
    localState->h3SeedsMemRef = h3SeedsMemRef;

    // Setting the local state
    executionCtx.setLocalOperatorState(this, std::move(localState));
}

void CountMinBuild::close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    auto countMinLocalState = dynamic_cast<CountMinLocalState*>(ctx.getLocalState(this));
    // Check, if we have to increment the old synopsis reference with the currentIncrement of the local state
    if (countMinLocalState->currentIncrement > 0_u64) {
        Nautilus::FunctionCall("incrementObservedTuplesStatisticProxy",
                               incrementObservedTuplesStatisticProxy,
                               countMinLocalState->synopsisReference,
                               countMinLocalState->currentIncrement);
    }

    // Update the watermark for the count min build operator and send the created statistics upward
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    Nautilus::FunctionCall("checkCountMinSketchesSendingProxy",
                           checkCountMinSketchesSendingProxy,
                           operatorHandlerMemRef,
                           ctx.getPipelineContext(),
                           ctx.getWatermarkTs(),
                           ctx.getSequenceNumber(),
                           ctx.getChunkNumber(),
                           ctx.getLastChunk(),
                           ctx.getOriginId(),
                           Value<UInt64>(metricHash),
                           ctx.getCurrentStatisticId());
    Operator::close(ctx, recordBuffer);
}

CountMinBuild::CountMinBuild(const uint64_t operatorHandlerIndex,
                             const std::string_view fieldToTrackFieldName,
                             const uint64_t numberOfBitsInKey,
                             const uint64_t width,
                             const uint64_t depth,
                             const Statistic::StatisticMetricHash metricHash,
                             TimeFunctionPtr timeFunction,
                             const uint64_t numberOfBitsInHashValue)
    : operatorHandlerIndex(operatorHandlerIndex), fieldToTrackFieldName(fieldToTrackFieldName),
      sizeOfOneRowInBytes(((numberOfBitsInKey * numberOfBitsInHashValue) / 8)), width(width), depth(depth),
      metricHash(metricHash), timeFunction(std::move(timeFunction)),
      h3HashFunction(std::make_unique<Interface::H3Hash>(numberOfBitsInKey)) {}

}// namespace NES::Runtime::Execution::Operators
