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
#include <Execution/Operators/Streaming/StatisticCollection/EquiWidthHistogram/EquiWidthHistogramBuild.hpp>
#include <Execution/Operators/Streaming/StatisticCollection/EquiWidthHistogram/EquiWidthHistogramOperatorHandler.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Statistics/Synopses/EquiWidthHistogramStatistic.hpp>

namespace NES::Runtime::Execution::Operators {

void* getEquiWidthHistogramRefProxy(void* ptrOpHandler, Statistic::StatisticMetricHash metricHash, StatisticId statisticId,
                                    WorkerThreadId workerId, uint64_t timestamp)  {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<EquiWidthHistogramOperatorHandler*>(ptrOpHandler);

    const auto statisticHash = Statistic::StatisticKey::combineStatisticIdWithMetricHash(metricHash, statisticId);
    return opHandler->getStatistic(workerId, statisticHash, timestamp).get();
}


void checkEquiWidthHistogramSendingProxy(void* ptrOpHandler,
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
    auto* opHandler = static_cast<EquiWidthHistogramOperatorHandler*>(ptrOpHandler);
    auto* pipelineCtx = static_cast<PipelineExecutionContext*>(ptrPipelineCtx);
    
    // Calling the operator handler method now
    const BufferMetaData bufferMetaData(watermarkTs, {sequenceNumber, chunkNumber, lastChunk}, OriginId(originId));
    const auto statisticHash = Statistic::StatisticKey::combineStatisticIdWithMetricHash(metricHash, statisticId);
    opHandler->checkStatisticsSending(bufferMetaData, statisticHash, pipelineCtx);
}

void updateEquiWidthHistogramProxy(void* ptrEquiWidthHistMemRef, int64_t lowerBound, int64_t upperBound, uint64_t count) {
    NES_ASSERT2_FMT(ptrEquiWidthHistMemRef != nullptr, "equiWidthHistMemRef context should not be null");
    auto* equiWidthHist = static_cast<Statistic::EquiWidthHistogramStatistic*>(ptrEquiWidthHistMemRef);
    equiWidthHist->update(lowerBound, upperBound, count);
}

void EquiWidthHistogramBuild::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const {
    // We have to do this here, as we do not want to set the statistic id of this build operator in the execution context
    if (hasChild()) {
        child->open(executionCtx, recordBuffer);
    }
}

void EquiWidthHistogramBuild::execute(ExecutionContext& ctx, Record& record) const {
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);

    //Get the memRef to the EquiWidthHistogram sketch
    auto timestampVal = timeFunction->getTs(ctx, record);
    auto equiWidthHistMemRef = Nautilus::FunctionCall("getEquiWidthHistogramRefProxy", getEquiWidthHistogramRefProxy,
                                                 operatorHandlerMemRef, Value<UInt64>(metricHash), ctx.getCurrentStatisticId(),
                                                 ctx.getWorkerThreadId(), timestampVal);

    const auto value = record.read(fieldToTrackFieldName);
    const auto lowerBound = value - (value % binWidth);
    const auto upperBound = lowerBound + binWidth;
    Nautilus::FunctionCall("updateEquiWidthHistogramProxy",
                           updateEquiWidthHistogramProxy,
                           equiWidthHistMemRef,
                           Value<UInt64>(lowerBound.as<UInt64>()),
                           Value<UInt64>(upperBound.as<UInt64>()),
                           Value<UInt64>(1_u64));
}


void EquiWidthHistogramBuild::close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    // Update the watermark for the equi-width histogram build operator and send the created statistics upward
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    Nautilus::FunctionCall("checkEquiWidthHistogramSendingProxy",
                           checkEquiWidthHistogramSendingProxy,
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

EquiWidthHistogramBuild::EquiWidthHistogramBuild(const uint64_t operatorHandlerIndex,
                                                 const std::string& fieldToTrackFieldName,
                                                 const uint64_t binWidth,
                                                 const Statistic::StatisticMetricHash metricHash,
                                                 TimeFunctionPtr timeFunction)
    : operatorHandlerIndex(operatorHandlerIndex), fieldToTrackFieldName(fieldToTrackFieldName), binWidth(binWidth),
      metricHash(metricHash), timeFunction(std::move(timeFunction)) {}
} // namespace NES::Runtime::Execution::Operators