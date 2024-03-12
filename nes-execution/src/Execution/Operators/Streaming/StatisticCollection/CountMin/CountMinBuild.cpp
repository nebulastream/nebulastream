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
#include <Operators/LogicalOperators/StatisticCollection/Statistics/Synopses/CountMinStatistic.hpp>
#include <StatisticCollection/StatisticKey.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Hash/H3Hash.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Runtime::Execution::Operators {

void* getCountMinRefProxy(void* ptrOpHandler, Statistic::MetricHash metricHash, StatisticId statisticId,
                          uint64_t workerId, uint64_t timestamp)  {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<CountMinOperatorHandler*>(ptrOpHandler);

    const auto statisticHash = Statistic::StatisticKey::combineStatisticIdWithMetricHash(metricHash, statisticId);
    return opHandler->getStatistic(workerId, statisticHash, timestamp).get();
}

void* getH3SeedsProxy(void* ptrOpHandler) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<CountMinOperatorHandler*>(ptrOpHandler);

    // This const_cast is fine, as we do not change the values
    return reinterpret_cast<int8_t*>(const_cast<uint64_t*>(opHandler->getH3Seeds().data()));
}

void updateCountMinProxy(void* ptrCountMin, uint64_t row, uint64_t col) {
    NES_ASSERT2_FMT(ptrCountMin != nullptr, "countMin should not be null!");
    auto* countMin = static_cast<Statistic::CountMinStatistic*>(ptrCountMin);
    countMin->update(row, col);
}

void checkCountMinSketchesSendingProxy(void* ptrOpHandler,
                                       void* ptrPipelineCtx,
                                       uint64_t watermarkTs,
                                       uint64_t sequenceNumber,
                                       uint64_t chunkNumber,
                                       bool lastChunk,
                                       uint64_t originId,
                                       Statistic::MetricHash metricHash,
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


void CountMinBuild::execute(ExecutionContext& ctx, Record& record) const {
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);

    // 1. Get the memRef to the CountMin sketch
    auto timestampVal = timeFunction->getTs(ctx, record);
    auto countMinMemRef = Nautilus::FunctionCall("getCountMinRefProxy", getCountMinRefProxy,
                                                 operatorHandlerMemRef, Value<UInt64>(metricHash), ctx.getCurrentStatisticId(),
                                                 ctx.getWorkerId(), timestampVal);

    // 2. Updating the count min sketch for this record
    auto h3SeedsMemRef = Nautilus::FunctionCall("getH3SeedsProxy", getH3SeedsProxy, operatorHandlerMemRef);
    for (Value<UInt64> row(0_u64); row < depth; row = row + 1_u64) {
        // 2.1 We calculate a MemRef to the first h3Seeds of the current row
        Value<UInt64> h3SeedsOffSet((row * sizeOfOneRowInBytes).as<UInt64>());
        Value<MemRef> h3SeedsThisRow = (h3SeedsMemRef + h3SeedsOffSet).as<MemRef>();

        // 2.2. Hashing the current value
        auto valToTrack = record.read(fieldToTrackFieldName);
        Value<UInt64> calcHash = h3HashFunction->calculateWithState(valToTrack, h3SeedsThisRow);
        Value<UInt64> col = (calcHash % Value<UInt64>(width)).as<UInt64>();

        // 2.3. Having this function call for each record for each row is not efficient. But we take care of the efficiency later
        Nautilus::FunctionCall("updateCountMinProxy", updateCountMinProxy, countMinMemRef, row, col);
    }
}

void CountMinBuild::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const {
    // We have to do this here, as we do not want to set the statistic id of this build operator in the execution context
    if (hasChild()) {
        child->open(executionCtx, recordBuffer);
    }
}


void CountMinBuild::close(ExecutionContext& ctx, RecordBuffer&) const {
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
}

CountMinBuild::CountMinBuild(const uint64_t operatorHandlerIndex,
                             const std::string_view fieldToTrackFieldName,
                             const uint64_t numberOfBitsInKey,
                             const uint64_t width,
                             const uint64_t depth,
                             const Statistic::MetricHash metricHash,
                             TimeFunctionPtr timeFunction,
                             const uint64_t numberOfBitsInHashValue)
    : operatorHandlerIndex(operatorHandlerIndex), fieldToTrackFieldName(fieldToTrackFieldName),
      sizeOfOneRowInBytes(((numberOfBitsInKey * numberOfBitsInHashValue) / 8)), width(width), depth(depth),
      metricHash(metricHash), timeFunction(std::move(timeFunction)),
      h3HashFunction(std::make_unique<Interface::H3Hash>(numberOfBitsInKey)) {}

}// namespace NES::Runtime::Execution::Operators