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
#include <Execution/Operators/Streaming/Join/StreamJoinBuild.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Updates the sliceState of all slices and emits buffers, if the slices can be emitted
 */
void checkWindowsTriggerProxy(void* ptrOpHandler,
                              void* ptrPipelineCtx,
                              void* ptrWorkerCtx,
                              uint64_t watermarkTs,
                              uint64_t sequenceNumber,
                              uint64_t chunkNumber,
                              bool lastChunk,
                              uint64_t originId,
                              uint64_t joinStrategyInt,
                              uint64_t windowingStrategy) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    NES_ASSERT2_FMT(ptrPipelineCtx != nullptr, "pipeline context should not be null");
    NES_ASSERT2_FMT(ptrWorkerCtx != nullptr, "worker context should not be null");

    auto* opHandler = StreamJoinOperator::getSpecificOperatorHandler(ptrOpHandler, joinStrategyInt, windowingStrategy);
    auto* pipelineCtx = static_cast<PipelineExecutionContext*>(ptrPipelineCtx);
    auto* workerCtx = static_cast<WorkerContext*>(ptrWorkerCtx);

    //update last seen watermark by this worker
    // opHandler->updateWatermarkForWorker(watermarkTs, workerCtx->getId());
    // auto minWatermark = opHandler->getMinWatermarkForWorker();

    BufferMetaData bufferMetaData(watermarkTs, {sequenceNumber, chunkNumber, lastChunk}, OriginId(originId));
    opHandler->checkAndTriggerWindows(bufferMetaData, pipelineCtx);
}

void StreamJoinBuild::close(ExecutionContext& ctx, RecordBuffer&) const {
    // Update the watermark for the nlj operator and trigger slices
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    Nautilus::FunctionCall("checkWindowsTriggerProxy",
                           checkWindowsTriggerProxy,
                           operatorHandlerMemRef,
                           ctx.getPipelineContext(),
                           ctx.getWorkerContext(),
                           ctx.getWatermarkTs(),
                           ctx.getSequenceNumber(),
                           ctx.getChunkNumber(),
                           ctx.getLastChunk(),
                           ctx.getOriginId(),
                           Value<UInt64>(to_underlying<QueryCompilation::StreamJoinStrategy>(joinStrategy)),
                           Value<UInt64>(to_underlying<QueryCompilation::WindowingStrategy>(windowingStrategy)));
}

StreamJoinBuild::StreamJoinBuild(const uint64_t operatorHandlerIndex,
                                 const SchemaPtr& schema,
                                 const QueryCompilation::JoinBuildSideType joinBuildSide,
                                 const uint64_t entrySize,
                                 TimeFunctionPtr timeFunction,
                                 QueryCompilation::StreamJoinStrategy joinStrategy,
                                 QueryCompilation::WindowingStrategy windowingStrategy)
    : StreamJoinOperator(joinStrategy, windowingStrategy), operatorHandlerIndex(operatorHandlerIndex), schema(schema),
      joinBuildSide(joinBuildSide), entrySize(entrySize), timeFunction(std::move(timeFunction)) {}

}// namespace NES::Runtime::Execution::Operators
