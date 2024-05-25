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
#include <API/Schema.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Statistics/Synopses/ReservoirSampleStatistic.hpp>
#include <Execution/Operators/Streaming/StatisticCollection/ReservoirSample/ReservoirSampleBuild.hpp>
#include <Execution/Operators/Streaming/StatisticCollection/ReservoirSample/ReservoirSampleOperatorHandler.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Statistics/StatisticKey.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Get the (index+1) we should write the next tuple to. If we return 0, this tuple should not be part of the sample.
 * @param ptrReservoirSampleStatistic
 * @return uint64_t
 */
uint64_t getNextIndexProxy(void* ptrReservoirSampleStatistic) {
    NES_ASSERT2_FMT(ptrReservoirSampleStatistic != nullptr, "reservoir sample context should not be null!");
    auto* reservoirSample = static_cast<Statistic::ReservoirSampleStatistic*>(ptrReservoirSampleStatistic);
    reservoirSample->incrementObservedTuples(1); //Incrementing the observed tuples as we have seen one more
    const auto nextIdx = reservoirSample->getNextRandomInteger();
    if (nextIdx < reservoirSample->getSampleSize()) {
        return nextIdx + 1;
    } else {
        return 0;
    }
}

void* getReservoirBaseAddressRefProxy(void* ptrReservoirSample) {
    NES_ASSERT2_FMT(ptrReservoirSample != nullptr, "reservoir sample should not be null!");
    auto* sample = static_cast<Statistic::ReservoirSampleStatistic*>(ptrReservoirSample);
    return sample->getStatisticData();
}

void* getReservoirSampleRefProxy(void* ptrOpHandler, const uint64_t metricHash, const uint64_t statisticId,
                                 const WorkerThreadId workerId, const uint64_t timestamp) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<ReservoirSampleOperatorHandler*>(ptrOpHandler);

    const auto statisticHash = Statistic::StatisticKey::combineStatisticIdWithMetricHash(metricHash, statisticId);
    return opHandler->getStatistic(workerId, statisticHash, timestamp).get();
}

void checkReservoirSampleSendingProxy(void* ptrOpHandler,
                                      void* ptrPipelineCtx,
                                      const uint64_t watermarkTs,
                                      const uint64_t sequenceNumber,
                                      const uint64_t chunkNumber,
                                      const bool lastChunk,
                                      const uint64_t originId,
                                      const uint64_t metricHash,
                                      const StatisticId statisticId) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    NES_ASSERT2_FMT(ptrPipelineCtx != nullptr, "pipeline context should not be null");
    auto* opHandler = static_cast<ReservoirSampleOperatorHandler*>(ptrOpHandler);
    auto* pipelineCtx = static_cast<PipelineExecutionContext*>(ptrPipelineCtx);

    // Calling the operator handler method now
    const BufferMetaData bufferMetaData(watermarkTs, {sequenceNumber, chunkNumber, lastChunk}, OriginId(originId));
    const auto statisticHash = Statistic::StatisticKey::combineStatisticIdWithMetricHash(metricHash, statisticId);
    opHandler->checkStatisticsSending(bufferMetaData, statisticHash, pipelineCtx);
}

void ReservoirSampleBuild::updateLocalState(ExecutionContext& ctx,
                                            ReservoirSampleLocalState& localState,
                                            const Value<UInt64>& timestamp) const {
    // We have to get the slice for the current timestamp
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    auto workerId = ctx.getWorkerId();
    auto sliceReference = Nautilus::FunctionCall("getReservoirSampleRefProxy",
                                                 getReservoirSampleRefProxy,
                                                 operatorHandlerMemRef,
                                                 Value<UInt64>(metricHash),
                                                 ctx.getCurrentStatisticId(),
                                                 workerId,
                                                 timestamp);
    auto reservoirMemRef = Nautilus::FunctionCall("getReservoirBaseAddressRefProxy",
                                                  getReservoirBaseAddressRefProxy,
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
    localState.sampleBaseAddress = reservoirMemRef;
}

void ReservoirSampleBuild::execute(ExecutionContext& ctx, Record& record) const {
    auto reservoirSampleLocalState = dynamic_cast<ReservoirSampleLocalState*>(ctx.getLocalState(this));

    // 1. Get the memRef to the Reservoir sample and to the underlying reservoir, if not in the local state
    const auto timestampVal = timeFunction->getTs(ctx, record);
    if (!reservoirSampleLocalState->correctSynopsesForTimestamp(timestampVal)) {
        updateLocalState(ctx, *reservoirSampleLocalState, timestampVal);
    }

    // 2. Get the index we should write the next tuple to
    Value<UInt64> indexInSample = Nautilus::FunctionCall("getNextIndexProxy", getNextIndexProxy, reservoirSampleLocalState->synopsisReference);

    // 3. If the index is 0, we do not write the record to the sample
    if (indexInSample != 0_u64) {
        indexInSample = indexInSample - 1;
        // We are using the memory provider, as we do not support variable sized data currently.
        // The memory provider was written as a connection to a tuple buffer. We do not store the data as a tuple buffer
        // and therefore, this only works for fixed data types.
        reservoirMemoryProvider->write(indexInSample, reservoirSampleLocalState->sampleBaseAddress, record);
    }
}

void ReservoirSampleBuild::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const {
    // We have to do this here, as we do not want to set the statistic id of this build operator in the execution context
    if (hasChild()) {
        child->open(executionCtx, recordBuffer);
    }

    // Creating a count min local state
    auto nullPtrRef1 = Nautilus::FunctionCall("getNullPtrMemRefProxy", getNullPtrMemRefProxy);
    auto nullPtrRef2 = Nautilus::FunctionCall("getNullPtrMemRefProxy", getNullPtrMemRefProxy);
    auto localState = std::make_unique<ReservoirSampleLocalState>(nullPtrRef1, nullPtrRef2);

    // Setting the local state
    executionCtx.setLocalOperatorState(this, std::move(localState));
}

void ReservoirSampleBuild::close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    // Update the watermark for the ReservoirSample build operator and send the created statistics upward
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    Nautilus::FunctionCall("checkReservoirSampleSendingProxy",
                           checkReservoirSampleSendingProxy,
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

ReservoirSampleBuild::ReservoirSampleBuild(const uint64_t operatorHandlerIndex,
                                           const Statistic::StatisticMetricHash metricHash,
                                           TimeFunctionPtr timeFunction,
                                           uint64_t sampleSize,
                                           const SchemaPtr schema)
    : operatorHandlerIndex(operatorHandlerIndex), metricHash(metricHash), timeFunction(std::move(timeFunction)),
      reservoirMemoryProvider(Execution::MemoryProvider::MemoryProvider::createMemoryProvider(sampleSize * schema->getSchemaSizeInBytes(), schema)) {}

}// namespace NES::Runtime::Execution::Operators