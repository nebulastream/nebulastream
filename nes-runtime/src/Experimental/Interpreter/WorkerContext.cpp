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
#include <Experimental/Interpreter/WorkerContext.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>

namespace NES::Nautilus {

WorkerContext::WorkerContext(Value<MemRef> workerContextRef) : workerContextRef(workerContextRef) {}

extern "C" void* allocateBufferProxy(void* workerContext) {
    auto* wc = (Runtime::WorkerContext*) workerContext;
    // we allocate a new tuple buffer on the heap and call retain to prevent the deletion of the reference.
    // todo check with ventura
    auto buffer = wc->allocateTupleBuffer();
    auto* tb = new Runtime::TupleBuffer(buffer);
    return tb;
}

Value<MemRef> WorkerContext::allocateBuffer() { return FunctionCall<>("allocateBuffer", allocateBufferProxy, workerContextRef); }

Value<MemRef> WorkerContext::getWorkerContextRef() const { return this->workerContextRef; }

extern "C" uint64_t getWorkerIdProxy(void* workerContext) {
    auto* wc = (Runtime::WorkerContext*) workerContext;
    return wc->getId();
}

Value<UInt64> WorkerContext::getWorkerId() { return FunctionCall<>("getWorkerId", getWorkerIdProxy, workerContextRef); }

}// namespace NES::Nautilus