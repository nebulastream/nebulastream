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

#include <Execution/Operators/Streaming/StatisticCollection/HyperLogLog/HyperLogLogBuild.hpp>
#include <Execution/Operators/Streaming/StatisticCollection/HyperLogLog/HyperLogLogOperatorHandler.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Statistics/Synopses/HyperLogLogStatistic.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Runtime::Execution::Operators {

void* getHLLRefProxy(void* ptrOpHandler, Statistic::StatisticMetricHash metricHash, StatisticId statisticId,
                     uint64_t workerId, uint64_t timestamp) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<HyperLogLogOperatorHandler*>(ptrOpHandler);

    const auto statisticHash = Statistic::StatisticKey::combineStatisticIdWithMetricHash(metricHash, statisticId);
    return opHandler->getStatistic(workerId, statisticHash, timestamp).get();
}

void updateHLLProxy(void* ptrHLL, uint64_t hash) {
    NES_ASSERT2_FMT(ptrHLL != nullptr, "HyperLogLog should not be null!");
    auto* hll = static_cast<Statistic::HyperLogLogStatistic*>(ptrHLL);
    hll->update(hash);
}

void checkHLLSketchesSendingProxy(void* ptrOpHandler,
                                  void* ptrPipelineCtx,
                                  uint64_t watermarkTs,
                                  uint64_t sequenceNumber,
                                  uint64_t chunkNumber,
                                  bool lastChunk,
                                  OriginId originId,
                                  Statistic::StatisticMetricHash metricHash,
                                  StatisticId statisticId) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    NES_ASSERT2_FMT(ptrPipelineCtx != nullptr, "pipeline context should not be null");
    auto* opHandler = static_cast<HyperLogLogOperatorHandler*>(ptrOpHandler);
    auto* pipelineCtx = static_cast<PipelineExecutionContext*>(ptrPipelineCtx);

    // Calling the operator handler method now
    const BufferMetaData bufferMetaData(watermarkTs, {sequenceNumber, chunkNumber, lastChunk}, originId);
    const auto statisticHash = Statistic::StatisticKey::combineStatisticIdWithMetricHash(metricHash, statisticId);
    opHandler->checkStatisticsSending(bufferMetaData, statisticHash, pipelineCtx);}


void HyperLogLogBuild::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const {
    // We have to do this here, as we do not want to set the statistic id of this build operator in the execution context
    if (hasChild()) {
        child->open(executionCtx, recordBuffer);
    }
}

void HyperLogLogBuild::execute(ExecutionContext& ctx, Record& record) const {
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);

    // 1. Get the memRef to the CountMin sketch
    auto timestampVal = timeFunction->getTs(ctx, record);
    auto hllMemRef = Nautilus::FunctionCall("getHLLRefProxy", getHLLRefProxy,
                                            operatorHandlerMemRef, Value<UInt64>(metricHash),
                                            ctx.getCurrentStatisticId(),
                                            ctx.getWorkerId(), timestampVal);

    // 2. Updating the hyperloglog sketch for this record
    Value<UInt64> hash = murmurHash->calculate(record.read(fieldToTrackFieldName));
    Nautilus::FunctionCall("updateHLLProxy", updateHLLProxy, hllMemRef, hash);
}

void HyperLogLogBuild::close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    // Update the watermark for the HyperLogLog build operator and send the created statistics upward
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    Nautilus::FunctionCall("checkHLLSketchesSendingProxy",
                           checkHLLSketchesSendingProxy,
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

HyperLogLogBuild::HyperLogLogBuild(const uint64_t operatorHandlerIndex,
                                   const std::string_view fieldToTrackFieldName,
                                   const Statistic::StatisticMetricHash metricHash,
                                   TimeFunctionPtr timeFunction)
    : operatorHandlerIndex(operatorHandlerIndex), fieldToTrackFieldName(fieldToTrackFieldName),
      metricHash(metricHash), timeFunction(std::move(timeFunction)),
      murmurHash(std::make_unique<Nautilus::Interface::MurMur3HashFunction>()) {}

}// namespace NES::Runtime::Execution::Operators