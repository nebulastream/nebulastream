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
#include <Execution/Operators/ReorderTupleBuffersOperator.hpp>
#include <Execution/Operators/ReorderTupleBuffersOperatorHandler.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Nautilus interface to process next tuple buffer
 */
void checkAndEmitTupleBufferProxy(void* ptrOpHandler, void* ptrPipelineCtx, void* ptrWorkerCtx, void* tupleBuffer) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    NES_ASSERT2_FMT(ptrPipelineCtx != nullptr, "pipeline context should not be null");
    NES_ASSERT2_FMT(ptrWorkerCtx != nullptr, "worker context should not be null");

    auto* opHandler = static_cast<ReorderTupleBuffersOperatorHandler*>(ptrOpHandler);
    auto* pipelineCtx = static_cast<PipelineExecutionContext*>(ptrPipelineCtx);
    auto* workerCtx = static_cast<WorkerContext*>(ptrWorkerCtx);
    auto* tb = static_cast<Runtime::TupleBuffer*>(tupleBuffer);

    opHandler->processBuffer(pipelineCtx, *workerCtx, *tb);
}

ReorderTupleBuffersOperator::ReorderTupleBuffersOperator(uint64_t operatorHandlerIndex)
    : operatorHandlerIndex(operatorHandlerIndex){};

void ReorderTupleBuffersOperator::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    Operator::open(ctx, recordBuffer);

    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    Nautilus::FunctionCall("checkAndEmitTupleBufferProxy",
                           checkAndEmitTupleBufferProxy,
                           operatorHandlerMemRef,
                           ctx.getPipelineContext(),
                           ctx.getWorkerContext(),
                           recordBuffer.getReference());
}
}// namespace NES::Runtime::Execution::Operators
