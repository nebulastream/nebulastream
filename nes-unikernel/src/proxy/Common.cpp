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

#include <Runtime/Execution/UnikernelPipelineExecutionContext.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstdint>
#include <proxy/common.hpp>

EXT_C uint64_t getWorkerIdProxy(void* workerContext) {
    TRACE_PROXY_FUNCTION_NO_ARG;
    auto* wc = (NES::Runtime::WorkerContext*) workerContext;
    return wc->getId();
}

EXT_C void* allocateBufferProxy(void* worker_context_ptr) {
    TRACE_PROXY_FUNCTION_NO_ARG;
    auto wc = static_cast<NES::Runtime::WorkerContext*>(worker_context_ptr);
    // We allocate a new tuple buffer for the runtime.
    // As we can only return it to operator code as a ptr we create a new TupleBuffer on the heap.
    // This increases the reference counter in the buffer.
    // When the heap allocated buffer is not required anymore, the operator code has to clean up the allocated memory to prevent memory leaks.
    auto buffer = wc->getBufferProvider()->getBufferBlocking();
    auto* tb = new NES::Runtime::TupleBuffer(buffer);
    return tb;
}

EXT_C void emitBufferProxy(void*, void* pc_ptr, void* tupleBuffer) {
    TRACE_PROXY_FUNCTION_NO_ARG
    auto* tb = (NES::Runtime::TupleBuffer*) tupleBuffer;
    auto pipeline_context = static_cast<NES::Unikernel::UnikernelPipelineExecutionContext*>(pc_ptr);
    if (tb->getNumberOfTuples() != 0) {
        pipeline_context->emit(*tb);
    }
    tb->release();
}

EXT_C void* getGlobalOperatorHandlerProxy(void* pc, uint64_t index) {
    TRACE_PROXY_FUNCTION(index);
    auto pipelineCtx = static_cast<NES::Unikernel::UnikernelPipelineExecutionContext*>(pc);
    return pipelineCtx->getOperatorHandler(index);
}
