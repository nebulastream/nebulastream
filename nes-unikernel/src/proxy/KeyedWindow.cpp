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
#include <Execution/Operators/Streaming/Aggregations/AppendToSliceStoreHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>
#include <Runtime/Execution/UnikernelPipelineExecutionContext.hpp>
#include <cstdint>
#include <cstring>
#include <proxy/common.hpp>

EXT_C bool memeq(void* ptr1, void* ptr2, uint64_t size) { return std::memcmp(ptr1, ptr2, size) == 0; }
EXT_C void* getKeyedSliceStoreProxy(void* op, uint64_t workerId) {
    TRACE_PROXY_FUNCTION(workerId);
    auto handler = static_cast<NES::Runtime::Execution::Operators::KeyedSlicePreAggregationHandler*>(op);
    return handler->getThreadLocalSliceStore(workerId);
}
EXT_C void* findKeyedSliceStateByTsProxy(void* ss, uint64_t ts) {
    auto sliceStore = static_cast<NES::Runtime::Execution::Operators::KeyedThreadLocalSliceStore*>(ss);
    TRACE_PROXY_FUNCTION(ts);
    return sliceStore->findSliceByTs(ts)->getState().get();
}

EXT_C void appendToGlobalSliceStoreKeyed(void* ss, void* slicePtr) {
    auto handler = static_cast<
        NES::Runtime::Execution::Operators::AppendToSliceStoreHandler<NES::Runtime::Execution::Operators::KeyedSlice>*>(ss);
    auto slice = std::unique_ptr<NES::Runtime::Execution::Operators::KeyedSlice>(
        (NES::Runtime::Execution::Operators::KeyedSlice*) slicePtr);
    TRACE_PROXY_FUNCTION(*slice);
    handler->appendToGlobalSliceStore(std::move(slice));
}
EXT_C void setupWindowHandlerKeyed(void* ss, void* ctx, uint64_t keySize, uint64_t valueSize) {
    auto handler = static_cast<NES::Runtime::Execution::Operators::KeyedSlicePreAggregationHandler*>(ss);
    auto pipelineExecutionContext = static_cast<NES::Unikernel::UnikernelPipelineExecutionContext*>(ctx);
    TRACE_PROXY_FUNCTION(keySize, valueSize);
    handler->setup(*pipelineExecutionContext, keySize, valueSize);
}

EXT_C void setupKeyedSliceMergingHandler(void* ss, void* ctx, uint64_t keySize, uint64_t valueSize) {
    auto handler = static_cast<NES::Runtime::Execution::Operators::KeyedSliceMergingHandler*>(ss);
    auto pipelineExecutionContext = static_cast<NES::Unikernel::UnikernelPipelineExecutionContext*>(ctx);
    TRACE_PROXY_FUNCTION(keySize, valueSize);
    handler->setup(*pipelineExecutionContext, keySize, valueSize);
}
static inline uint64_t hashInt(uint64_t x) {
    x ^= x >> 33U;
    x *= UINT64_C(0xff51afd7ed558ccd);
    x ^= x >> 33U;
    x *= UINT64_C(0xc4ceb9fe1a85ec53);
    x ^= x >> 33U;
    return x;
}

template<typename T>
static inline uint64_t hashValue(uint64_t seed, T value) {
    // Combine two hashes by XORing them
    // As done by duckDB https://github.com/duckdb/duckdb/blob/09f803d3ad2972e36b15612c4bc15d65685a743e/src/include/duckdb/common/types/hash.hpp#L42
    return seed ^ hashInt(value);
}

extern "C" void* findChainProxy(void* state, uint64_t hash) {
    TRACE_PROXY_FUNCTION(hash);
    auto hashMap = (NES::Nautilus::Interface::ChainedHashMap*) state;
    return hashMap->findChain(hash);
}

extern "C" void* insetProxy(void* state, uint64_t hash) {
    TRACE_PROXY_FUNCTION(hash);
    auto hashMap = (NES::Nautilus::Interface::ChainedHashMap*) state;
    return hashMap->insertEntry(hash);
}

EXT_C void triggerKeyedThreadLocalWindow(void* op,
                                         void* wctx,
                                         void* pctx,
                                         uint64_t originId,
                                         uint64_t sequenceNumber,
                                         uint64_t watermarkTs) {
    auto handler = static_cast<NES::Runtime::Execution::Operators::KeyedSlicePreAggregationHandler*>(op);
    auto workerContext = static_cast<NES::Runtime::WorkerContext*>(wctx);
    auto pipelineExecutionContext = static_cast<NES::Unikernel::UnikernelPipelineExecutionContext*>(pctx);
    TRACE_PROXY_FUNCTION(originId, sequenceNumber, watermarkTs);
    handler->trigger(*workerContext, *pipelineExecutionContext, originId, sequenceNumber, watermarkTs);
}

EXT_C uint64_t hashValueUI64(uint64_t seed, uint64_t value) {
    TRACE_PROXY_FUNCTION(seed, value);
    return hashValue(seed, value);
}

EXT_C void* createKeyedState(void* op, void* sliceMergeTaskPtr) {
    auto handler = static_cast<NES::Runtime::Execution::Operators::KeyedSliceMergingHandler*>(op);
    auto sliceMergeTask =
        static_cast<NES::Runtime::Execution::Operators::SliceMergeTask<NES::Runtime::Execution::Operators::KeyedSlice>*>(
            sliceMergeTaskPtr);
    TRACE_PROXY_FUNCTION(*sliceMergeTask);
    auto globalState = handler->createGlobalSlice(sliceMergeTask);
    // we give nautilus the ownership, thus deletePartition must be called.
    return globalState.release();
}
EXT_C void deleteSliceKeyed(void* gs) {
    auto globalSlice = static_cast<NES::Runtime::Execution::Operators::KeyedSlice*>(gs);
    TRACE_PROXY_FUNCTION(*globalSlice);
    delete globalSlice;
}
EXT_C void* getKeyedSliceState(void* gs) {
    auto globalSlice = static_cast<NES::Runtime::Execution::Operators::KeyedSlice*>(gs);
    TRACE_PROXY_FUNCTION(*globalSlice);
    return globalSlice->getState().get();
}

EXT_C void* getKeyedSliceStateFromTask(void* smt, uint64_t index) {
    auto task =
        static_cast<NES::Runtime::Execution::Operators::SliceMergeTask<NES::Runtime::Execution::Operators::KeyedSlice>*>(smt);
    TRACE_PROXY_FUNCTION(*task, index);
    return task->slices[index].get()->getState().get();
}

EXT_C uint64_t getKeyedNumberOfSlicesFromTask(void* smt) {
    auto task =
        static_cast<NES::Runtime::Execution::Operators::SliceMergeTask<NES::Runtime::Execution::Operators::KeyedSlice>*>(smt);
    TRACE_PROXY_FUNCTION(*task);
    return task->slices.size();
}

EXT_C void freeKeyedSliceMergeTask(void* smt) {
    auto task =
        static_cast<NES::Runtime::Execution::Operators::SliceMergeTask<NES::Runtime::Execution::Operators::KeyedSlice>*>(smt);
    TRACE_PROXY_FUNCTION(*task);
    task->~SliceMergeTask();
}
extern "C" void* insertProxy(void* state, uint64_t hash) {
    auto hashMap = (NES::Nautilus::Interface::ChainedHashMap*) state;
    TRACE_PROXY_FUNCTION(hash);
    return hashMap->insertEntry(hash);
}
EXT_C void* getPageProxy(void* hmPtr, uint64_t pageIndex) {
    auto hashMap = (NES::Nautilus::Interface::ChainedHashMap*) hmPtr;
    TRACE_PROXY_FUNCTION(pageIndex);
    return hashMap->getPage(pageIndex);
}

EXT_C void triggerSlidingWindowsKeyed(void* sh, void* wctx, void* pctx, uint64_t sequenceNumber, uint64_t sliceEnd) {
    auto handler = static_cast<
        NES::Runtime::Execution::Operators::AppendToSliceStoreHandler<NES::Runtime::Execution::Operators::KeyedSlice>*>(sh);
    auto workerContext = static_cast<NES::Runtime::WorkerContext*>(wctx);
    auto pipelineExecutionContext = static_cast<NES::Unikernel::UnikernelPipelineExecutionContext*>(pctx);
    TRACE_PROXY_FUNCTION(sequenceNumber, sliceEnd);
    handler->triggerSlidingWindows(*workerContext, *pipelineExecutionContext, sequenceNumber, sliceEnd);
}
