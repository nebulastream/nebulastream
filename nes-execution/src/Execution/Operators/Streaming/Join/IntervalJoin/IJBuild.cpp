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

#include <API/AttributeField.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/IntervalJoin/IJBuild.hpp>
#include <Execution/Operators/Streaming/Join/IntervalJoin/IJOperatorHandler.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <iostream>

namespace NES::Runtime::Execution::Operators {

//// FUNCTION CALLS (ProxyFunctions)

/**
 * @brief Updates the sliceState of all slices and emits buffers, if the slices can be emitted
 */
void checkIntervalsTriggerProxy(void* ptrOpHandler,
                                void* ptrPipelineCtx,
                                void* ptrWorkerCtx,
                                uint64_t watermarkTs,
                                uint64_t sequenceNumber,
                                uint64_t chunkNumber,
                                bool lastChunk,
                                uint64_t originId) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    NES_ASSERT2_FMT(ptrPipelineCtx != nullptr, "pipeline context should not be null");
    NES_ASSERT2_FMT(ptrWorkerCtx != nullptr, "worker context should not be null");

    auto* ijOperatorHandler = static_cast<IJOperatorHandler*>(ptrOpHandler);
    NES_DEBUG("Got join operator handler {}", ijOperatorHandler->toString());
    auto* pipelineCtx = static_cast<PipelineExecutionContext*>(ptrPipelineCtx);
    NES_DEBUG("Got pipelineContext {}", pipelineCtx->toString());
    auto* workerCtx = static_cast<WorkerContext*>(ptrWorkerCtx);

    //update last seen watermark by this worker
    NES_DEBUG("Current watermark is {}", watermarkTs);
    ijOperatorHandler->updateWatermarkForWorker(watermarkTs, workerCtx->getId());
    auto minWatermark = ijOperatorHandler->getMinWatermarkForWorker();

    BufferMetaData bufferMetaData(minWatermark, {sequenceNumber, chunkNumber, lastChunk}, OriginId(originId));
    ijOperatorHandler->checkAndTriggerIntervals(bufferMetaData, pipelineCtx);
}

//// END FUNCTION CALLS (ProxyFunctions)

IJBuild::IJBuild(const uint64_t operatorHandlerIndex,
                 const SchemaPtr buildSideSchema,
                 const SchemaPtr otherSideSchema,
                 TimeFunctionPtr timeFunction)
    : operatorHandlerIndex(operatorHandlerIndex), buildSideSchema(buildSideSchema), otherSideSchema(otherSideSchema),
      timeFunction(std::move(timeFunction)) {}

void IJBuild::close(ExecutionContext& ctx, RecordBuffer&) const {
    NES_DEBUG("finished executing all records of record buffer");
    // Update the watermark and trigger slices
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);

    Nautilus::FunctionCall("checkIntervalsTriggerProxy",
                           checkIntervalsTriggerProxy,
                           operatorHandlerMemRef,
                           ctx.getPipelineContext(),
                           ctx.getWorkerContext(),
                           ctx.getWatermarkTs(),
                           ctx.getSequenceNumber(),
                           ctx.getChunkNumber(),
                           ctx.getLastChunk(),
                           ctx.getOriginId());
}
}// namespace NES::Runtime::Execution::Operators
