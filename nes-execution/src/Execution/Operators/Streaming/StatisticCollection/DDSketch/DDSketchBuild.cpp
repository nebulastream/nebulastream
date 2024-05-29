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
#include <Execution/Expressions/ConstantValueExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/StatisticCollection/DDSketch/DDSketchBuild.hpp>
#include <Execution/Operators/Streaming/StatisticCollection/DDSketch/DDSketchOperatorHandler.hpp>
#include <Execution/Operators/Streaming/StatisticCollection/SynopsisLocalState.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Statistics/StatisticKey.hpp>
#include <Statistics/Synopses/DDSketchStatistic.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Runtime::Execution::Operators {

void* getDDSketchRefProxy(void* ptrOpHandler, Statistic::StatisticMetricHash metricHash, StatisticId statisticId,
                          WorkerThreadId workerId, uint64_t timestamp)  {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<DDSketchOperatorHandler*>(ptrOpHandler);

    const auto statisticHash = Statistic::StatisticKey::combineStatisticIdWithMetricHash(metricHash, statisticId);
    return opHandler->getStatistic(workerId, statisticHash, timestamp).get();
}

void checkDDSketchesSendingProxy(void* ptrOpHandler,
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
    auto* opHandler = static_cast<DDSketchOperatorHandler*>(ptrOpHandler);
    auto* pipelineCtx = static_cast<PipelineExecutionContext*>(ptrPipelineCtx);

    // Calling the operator handler method now
    const BufferMetaData bufferMetaData(watermarkTs, {sequenceNumber, chunkNumber, lastChunk}, OriginId(originId));
    const auto statisticHash = Statistic::StatisticKey::combineStatisticIdWithMetricHash(metricHash, statisticId);
    opHandler->checkStatisticsSending(bufferMetaData, statisticHash, pipelineCtx);
}

void addValueToDDSketchProxy(void* ptrDDSketch, double, double valueOffSet, int64_t ) {
    NES_ASSERT2_FMT(ptrDDSketch != nullptr, "ddSketch should not be null!");
    auto* ddSketch = static_cast<Statistic::DDSketchStatistic*>(ptrDDSketch);

    // This cast is fine, as we already perform a floor operation in the execute() by calling the calcLogFloorIndex expression
    ddSketch->addValue(static_cast<Statistic::DDSketchStatistic::LogFloorIndex>(valueOffSet));
}

void DDSketchBuild::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const {
    // We have to do this here, as we do not want to set the statistic id of this build operator in the execution context
    if (hasChild()) {
        child->open(executionCtx, recordBuffer);
    }

    // Creating a dd-sketch local state
    auto nullPtrRef = Nautilus::FunctionCall("getNullPtrMemRefProxy", getNullPtrMemRefProxy);
    auto localState = std::make_unique<SynopsisLocalState>(0_u64, 0_u64, nullPtrRef);

    // Setting the local state
    executionCtx.setLocalOperatorState(this, std::move(localState));
}

void DDSketchBuild::updateLocalState(ExecutionContext& ctx, SynopsisLocalState& localState, const Value<UInt64>& timestamp) const {
    // We have to get the slice for the current timestamp
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    auto workerThreadId = ctx.getWorkerThreadId();
    auto sliceReference = Nautilus::FunctionCall("getDDSketchRefProxy", getDDSketchRefProxy,
                                                 operatorHandlerMemRef, Value<UInt64>(metricHash), ctx.getCurrentStatisticId(),
                                                 ctx.getWorkerThreadId(), timestamp);

    // We have to get the synopsis start and end timestamp
    auto startTs = Nautilus::FunctionCall("getSynopsisStartProxy",
                                          getSynopsisStartProxy,
                                          operatorHandlerMemRef,
                                          Value<UInt64>(metricHash),
                                          ctx.getCurrentStatisticId(),
                                          workerThreadId,
                                          timestamp);
    auto endTs = Nautilus::FunctionCall("getSynopsisEndProxy",
                                        getSynopsisEndProxy,
                                        operatorHandlerMemRef,
                                        Value<UInt64>(metricHash),
                                        ctx.getCurrentStatisticId(),
                                        workerThreadId,
                                        timestamp);

    // Updating the local join state
    localState.synopsisStartTs = startTs;
    localState.synopsisEndTs = endTs;
    localState.synopsisReference = sliceReference;
}

void DDSketchBuild::execute(ExecutionContext& ctx, Record& record) const {
    auto localState = dynamic_cast<SynopsisLocalState*>(ctx.getLocalState(this));

    // 1. Get the memRef to the DD-Sketch
    const auto timestampVal = timeFunction->getTs(ctx, record);
    if (!localState->correctSynopsesForTimestamp(timestampVal)) {
        updateLocalState(ctx, *localState, timestampVal);
    }

    // 2. Calculate the correct index
    auto value = record.read(fieldToTrackFieldName);

    // 3. If the value is not null, we have to calculate the log floor index and depending on the sign of the value
    // we have to increment or decrement the log floor index by one
    if (lessThanZero->execute(record)) {
        Value<Double> logFloorIndex = calcLogFloorIndex->execute(record).as<Double>();
        Value<Double> logFloorIndexOffSet = (-1.0 * logFloorIndex).as<Double>();
        logFloorIndexOffSet = logFloorIndexOffSet - 1.0;
        FunctionCall("addValueToDDSketchProxy", addValueToDDSketchProxy, localState->synopsisReference, logFloorIndex, logFloorIndexOffSet, value.as<UInt64>());
    } else if (greaterThanZero->execute(record)) {
        Value<Double> logFloorIndex = calcLogFloorIndex->execute(record).as<Double>();
        Value<Double> logFloorIndexOffSet = (logFloorIndex + 1.0).as<Double>();
        FunctionCall("addValueToDDSketchProxy", addValueToDDSketchProxy, localState->synopsisReference, logFloorIndex, logFloorIndexOffSet, value.as<UInt64>());
    } else {
        Value<Double> logFloorIndex = 0.0;
        FunctionCall("addValueToDDSketchProxy", addValueToDDSketchProxy, localState->synopsisReference, logFloorIndex, Value<Double>(0.0), value.as<UInt64>());
    }
}

void DDSketchBuild::close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    // Update the watermark for the dd-sketch build operator and send the created statistics upward
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    Nautilus::FunctionCall("checkDDSketchesSendingProxy",
                           checkDDSketchesSendingProxy,
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

DDSketchBuild::DDSketchBuild(const uint64_t operatorHandlerIndex,
                             const std::string& fieldToTrackFieldName,
                             const Statistic::StatisticMetricHash metricHash,
                             TimeFunctionPtr timeFunction,
                             const Expressions::ExpressionPtr& calcLogFloorIndex,
                             const Expressions::ExpressionPtr& greaterThanZero,
                             const Expressions::ExpressionPtr& lessThanZero)
    : operatorHandlerIndex(operatorHandlerIndex), fieldToTrackFieldName(fieldToTrackFieldName),
      metricHash(metricHash), timeFunction(std::move(timeFunction)), calcLogFloorIndex(calcLogFloorIndex),
      greaterThanZero(greaterThanZero), lessThanZero(lessThanZero) {}

} // namespace NES::Runtime::Execution::Operators