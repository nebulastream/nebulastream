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
#define EXT_C extern "C"

EXT_C bool memeq(void* ptr1, void* ptr2, uint64_t size) { return std::memcmp(ptr1, ptr2, size) == 0; }
EXT_C void* getKeyedSliceStoreProxy(void* op, uint64_t workerId) {
    auto handler = static_cast<NES::Runtime::Execution::Operators::KeyedSlicePreAggregationHandler*>(op);
    return handler->getThreadLocalSliceStore(workerId);
}
EXT_C void* findKeyedSliceStateByTsProxy(void* ss, uint64_t ts) {
    auto sliceStore = static_cast<NES::Runtime::Execution::Operators::KeyedThreadLocalSliceStore*>(ss);
    return sliceStore->findSliceByTs(ts)->getState().get();
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
    auto hashMap = (NES::Nautilus::Interface::ChainedHashMap*) state;
    return hashMap->findChain(hash);
}

extern "C" void* insetProxy(void* state, uint64_t hash) {
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
    handler->trigger(*workerContext, *pipelineExecutionContext, originId, sequenceNumber, watermarkTs);
}

EXT_C uint64_t hashValueUI64(uint64_t seed, uint64_t value) { return hashValue(seed, value); }

EXT_C void* createKeyedState(void* op, void* sliceMergeTaskPtr) {
    auto handler = static_cast<NES::Runtime::Execution::Operators::KeyedSliceMergingHandler*>(op);
    auto sliceMergeTask =
        static_cast<NES::Runtime::Execution::Operators::SliceMergeTask<NES::Runtime::Execution::Operators::KeyedSlice>*>(
            sliceMergeTaskPtr);
    auto globalState = handler->createGlobalSlice(sliceMergeTask);
    // we give nautilus the ownership, thus deletePartition must be called.
    return globalState.release();
}
EXT_C void deleteSlice(void* gs) {
    auto globalSlice = static_cast<NES::Runtime::Execution::Operators::KeyedSlice*>(gs);
    delete globalSlice;
}
EXT_C void* getKeyedSliceState(void* gs) {
    auto globalSlice = static_cast<NES::Runtime::Execution::Operators::KeyedSlice*>(gs);
    return globalSlice->getState().get();
}

EXT_C void* getKeyedSliceStateFromTask(void* smt, uint64_t index) {
    auto task =
        static_cast<NES::Runtime::Execution::Operators::SliceMergeTask<NES::Runtime::Execution::Operators::KeyedSlice>*>(smt);
    return task->slices[index].get()->getState().get();
}

EXT_C uint64_t getKeyedNumberOfSlicesFromTask(void* smt) {
    auto task =
        static_cast<NES::Runtime::Execution::Operators::SliceMergeTask<NES::Runtime::Execution::Operators::KeyedSlice>*>(smt);
    return task->slices.size();
}

EXT_C void freeKeyedSliceMergeTask(void* smt) {
    auto task =
        static_cast<NES::Runtime::Execution::Operators::SliceMergeTask<NES::Runtime::Execution::Operators::KeyedSlice>*>(smt);
    task->~SliceMergeTask();
}
extern "C" void* insertProxy(void* state, uint64_t hash) {
    auto hashMap = (NES::Nautilus::Interface::ChainedHashMap*) state;
    return hashMap->insertEntry(hash);
}
EXT_C void* getPageProxy(void* hmPtr, uint64_t pageIndex) {
    auto hashMap = (NES::Nautilus::Interface::ChainedHashMap*) hmPtr;
    return hashMap->getPage(pageIndex);
}

EXT_C void appendToGlobalSliceStoreKeyed(void* ss, void* slicePtr) {
    auto handler = static_cast<
        NES::Runtime::Execution::Operators::AppendToSliceStoreHandler<NES::Runtime::Execution::Operators::KeyedSlice>*>(ss);
    auto slice = std::unique_ptr<NES::Runtime::Execution::Operators::KeyedSlice>(
        (NES::Runtime::Execution::Operators::KeyedSlice*) slicePtr);
    handler->appendToGlobalSliceStore(std::move(slice));
}

EXT_C void triggerSlidingWindowsKeyed(void* sh, void* wctx, void* pctx, uint64_t sequenceNumber, uint64_t sliceEnd) {
    auto handler = static_cast<
        NES::Runtime::Execution::Operators::AppendToSliceStoreHandler<NES::Runtime::Execution::Operators::KeyedSlice>*>(sh);
    auto workerContext = static_cast<NES::Runtime::WorkerContext*>(wctx);
    auto pipelineExecutionContext = static_cast<NES::Unikernel::UnikernelPipelineExecutionContext*>(pctx);
    handler->triggerSlidingWindows(*workerContext, *pipelineExecutionContext, sequenceNumber, sliceEnd);
}
