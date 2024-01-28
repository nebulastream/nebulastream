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

#ifndef PROXYFUNCTIONS_HPP
#define PROXYFUNCTIONS_HPP
#include <Configurations/Enums/WindowingStrategy.hpp>
#include <Execution/Operators/Streaming/Aggregations/AppendToSliceStoreHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Slicing/NLJOperatorHandlerSlicing.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <proxy/common.hpp>

PROXY_FN uint64_t getWorkerIdProxy(void* workerContext) {
    TRACE_PROXY_FUNCTION_NO_ARG;
    auto* wc = static_cast<NES::Runtime::WorkerContext*>(workerContext);
    return wc->getId();
}

PROXY_FN void* allocateBufferProxy(void* worker_context_ptr) {
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

PROXY_FN void emitBufferProxy(void*, void* pc_ptr, void* tupleBuffer) {
    TRACE_PROXY_FUNCTION_NO_ARG
    auto* tb = static_cast<NES::Runtime::TupleBuffer*>(tupleBuffer);
    auto pipeline_context = static_cast<NES::Unikernel::UnikernelPipelineExecutionContext*>(pc_ptr);
    if (tb->getNumberOfTuples() != 0) {
        pipeline_context->emit(*tb);
    }
    tb->release();
}

PROXY_FN void* getGlobalOperatorHandlerProxy(void* pc, uint64_t index) {
    TRACE_PROXY_FUNCTION(index);
    auto pipelineCtx = static_cast<NES::Unikernel::UnikernelPipelineExecutionContext*>(pc);
    return pipelineCtx->getOperatorHandler(index);
}

/**
     * @brief This method casts from a void* pointer depending on the join and window strategy to the correct derived class
     * and then pack to a parent class. This is necessary, as we do not always exactly know the child class.
     * @tparam OutputClass class to be casted to
     * @param ptrOpHandler
     * @param joinStrategyInt
     * @param windowingStrategyInt
     * @return OutputClass*
     */
template<typename OutputClass = NES::Runtime::Execution::Operators::StreamJoinOperatorHandler>
static NES::Runtime::Execution::Operators::NLJOperatorHandler*
getSpecificOperatorHandler(void* ptrOpHandler, uint64_t joinStrategyInt, uint64_t windowingStrategyInt) {
    NES_ASSERT(magic_enum::enum_value<NES::QueryCompilation::StreamJoinStrategy>(joinStrategyInt)
                   == NES::QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN,
               "Only NLJ is supported");
    NES_ASSERT(magic_enum::enum_value<NES::QueryCompilation::WindowingStrategy>(windowingStrategyInt)
                   == NES::QueryCompilation::WindowingStrategy::SLICING,
               "Only Slicing is supported");
    return static_cast<NES::Runtime::Execution::Operators::NLJOperatorHandlerSlicing*>(ptrOpHandler);
}

PROXY_FN uint64_t getNLJSliceStartProxy(void* ptrNljSlice) {
    NES_ASSERT2_FMT(ptrNljSlice != nullptr, "nlj slice pointer should not be null!");
    auto* nljSlice = static_cast<NES::Runtime::Execution::NLJSlice*>(ptrNljSlice);
    TRACE_PROXY_FUNCTION(*nljSlice);
    return nljSlice->getSliceStart();
}

PROXY_FN uint64_t getNLJSliceEndProxy(void* ptrNljSlice) {
    NES_ASSERT2_FMT(ptrNljSlice != nullptr, "nlj slice pointer should not be null!");
    auto* nljSlice = static_cast<NES::Runtime::Execution::NLJSlice*>(ptrNljSlice);
    TRACE_PROXY_FUNCTION(*nljSlice)
    return nljSlice->getSliceEnd();
}

PROXY_FN void* getCurrentWindowProxy(void* ptrOpHandler) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<NES::Runtime::Execution::Operators::NLJOperatorHandlerSlicing*>(ptrOpHandler);
    TRACE_PROXY_FUNCTION_NO_ARG;
    return opHandler->getCurrentSliceOrCreate();
}

PROXY_FN void* getNLJSliceRefProxy(void* ptrOpHandler, uint64_t timestamp) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<NES::Runtime::Execution::Operators::NLJOperatorHandlerSlicing*>(ptrOpHandler);
    TRACE_PROXY_FUNCTION(timestamp)
    return opHandler->getSliceByTimestampOrCreateIt(timestamp).get();
}

PROXY_FN void* getNLJPagedVectorProxy(void* ptrNljSlice, uint64_t workerId, uint64_t joinBuildSideInt) {
    NES_ASSERT2_FMT(ptrNljSlice != nullptr, "nlj slice pointer should not be null!");
    auto joinBuildSide = magic_enum::enum_cast<NES::QueryCompilation::JoinBuildSideType>(joinBuildSideInt).value();
    auto* nljSlice = static_cast<NES::Runtime::Execution::NLJSlice*>(ptrNljSlice);
    TRACE_PROXY_FUNCTION(workerId, magic_enum::enum_name(joinBuildSide));
    switch (joinBuildSide) {
        case NES::QueryCompilation::JoinBuildSideType::Left: return nljSlice->getPagedVectorRefLeft(workerId);
        case NES::QueryCompilation::JoinBuildSideType::Right: return nljSlice->getPagedVectorRefRight(workerId);
    }
}

PROXY_FN void allocateNewPageProxy(void* pagedVectorPtr) {
    auto* pagedVector = (NES::Nautilus::Interface::PagedVector*) pagedVectorPtr;
    TRACE_PROXY_FUNCTION(*pagedVector);
    pagedVector->appendPage();
}

PROXY_FN void* getPagedVectorPageProxy(void* pagedVectorPtr, uint64_t pagePos) {
    auto* pagedVector = (NES::Nautilus::Interface::PagedVector*) pagedVectorPtr;
    TRACE_PROXY_FUNCTION(*pagedVector, pagePos);
    return pagedVector->getPages()[pagePos];
}

PROXY_FN void checkWindowsTriggerProxy(void* ptrOpHandler,
                                       void* ptrPipelineCtx,
                                       void* ptrWorkerCtx,
                                       uint64_t watermarkTs,
                                       uint64_t sequenceNumber,
                                       NES::OriginId originId,
                                       uint64_t joinStrategyInt,
                                       uint64_t windowingStrategyInt) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    NES_ASSERT2_FMT(ptrPipelineCtx != nullptr, "pipeline context should not be null");
    NES_ASSERT2_FMT(ptrWorkerCtx != nullptr, "worker context should not be null");
    TRACE_PROXY_FUNCTION(watermarkTs, sequenceNumber, originId, joinStrategyInt, windowingStrategyInt);

    auto* opHandler = getSpecificOperatorHandler(ptrOpHandler, joinStrategyInt, windowingStrategyInt);
    auto* pipelineCtx = static_cast<NES::Unikernel::UnikernelPipelineExecutionContext*>(ptrPipelineCtx);
    auto* workerCtx = static_cast<NES::Runtime::WorkerContext*>(ptrWorkerCtx);

    //update last seen watermark by this worker
    opHandler->updateWatermarkForWorker(watermarkTs, workerCtx->getId());
    auto minWatermark = opHandler->getMinWatermarkForWorker();

    NES::Runtime::Execution::Operators::BufferMetaData bufferMetaData(minWatermark, sequenceNumber, originId);
    opHandler->checkAndTriggerWindows(bufferMetaData, pipelineCtx);
}

PROXY_FN void
triggerAllWindowsProxy(void* ptrOpHandler, void* ptrPipelineCtx, uint64_t joinStrategyInt, uint64_t windowingStrategyInt) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    NES_ASSERT2_FMT(ptrPipelineCtx != nullptr, "pipeline context should not be null");

    TRACE_PROXY_FUNCTION(joinStrategyInt, windowingStrategyInt);
    auto* opHandler = getSpecificOperatorHandler(ptrOpHandler, joinStrategyInt, windowingStrategyInt);
    auto* pipelineCtx = static_cast<NES::Unikernel::UnikernelPipelineExecutionContext*>(ptrPipelineCtx);

    opHandler->triggerAllSlices(pipelineCtx);
}

PROXY_FN void setNumberOfWorkerThreadsProxy(void* ptrOpHandler,
                                            void* ptrPipelineContext,
                                            uint64_t joinStrategyInt,
                                            uint64_t windowingStrategyInt) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    NES_ASSERT2_FMT(ptrPipelineContext != nullptr, "pipeline context should not be null!");

    TRACE_PROXY_FUNCTION(joinStrategyInt, windowingStrategyInt);
    auto* opHandler = getSpecificOperatorHandler(ptrOpHandler, joinStrategyInt, windowingStrategyInt);
    auto* pipelineCtx = static_cast<NES::Unikernel::UnikernelPipelineExecutionContext*>(ptrPipelineContext);

    auto numberOfWorkers = pipelineCtx->getNumberOfWorkerThreads();
    std::cout << opHandler << std::endl;
    std::cout << pipelineCtx << std::endl;
    std::cout << numberOfWorkers << std::endl;
    opHandler->setNumberOfWorkerThreads(numberOfWorkers);
}

PROXY_FN void* getNLJSliceRefFromIdProxy(void* ptrOpHandler, uint64_t sliceIdentifier) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");
    const auto opHandler = static_cast<NES::Runtime::Execution::Operators::NLJOperatorHandlerSlicing*>(ptrOpHandler);
    TRACE_PROXY_FUNCTION(sliceIdentifier);
    auto slice = opHandler->getSliceBySliceIdentifier(sliceIdentifier);
    if (slice.has_value()) {
        return slice.value().get();
    }
    // For now this is fine. We should handle this as part of issue #4016
    NES_ERROR("Could not find a slice with the id: {}", sliceIdentifier);
    return nullptr;
}

PROXY_FN uint64_t getNLJWindowStartProxy(void* ptrNLJWindowTriggerTask) {
    NES_ASSERT2_FMT(ptrNLJWindowTriggerTask != nullptr, "ptrNLJWindowTriggerTask should not be null");
    auto task = static_cast<NES::Runtime::Execution::Operators::EmittedNLJWindowTriggerTask*>(ptrNLJWindowTriggerTask);
    TRACE_PROXY_FUNCTION(*task);
    return task->windowInfo.windowStart;
}

PROXY_FN uint64_t getNLJWindowEndProxy(void* ptrNLJWindowTriggerTask) {
    NES_ASSERT2_FMT(ptrNLJWindowTriggerTask != nullptr, "ptrNLJWindowTriggerTask should not be null");
    auto task = static_cast<NES::Runtime::Execution::Operators::EmittedNLJWindowTriggerTask*>(ptrNLJWindowTriggerTask);
    TRACE_PROXY_FUNCTION(*task);
    return task->windowInfo.windowEnd;
}

PROXY_FN uint64_t getSliceIdNLJProxy(void* ptrNLJWindowTriggerTask, uint64_t joinBuildSideInt) {
    NES_ASSERT2_FMT(ptrNLJWindowTriggerTask != nullptr, "ptrNLJWindowTriggerTask should not be null");
    auto joinBuildSide = magic_enum::enum_cast<NES::QueryCompilation::JoinBuildSideType>(joinBuildSideInt).value();
    auto task = static_cast<NES::Runtime::Execution::Operators::EmittedNLJWindowTriggerTask*>(ptrNLJWindowTriggerTask);
    TRACE_PROXY_FUNCTION(*task, joinBuildSideInt);

    if (joinBuildSide == NES::QueryCompilation::JoinBuildSideType::Left) {
        return task->leftSliceIdentifier;
    } else if (joinBuildSide == NES::QueryCompilation::JoinBuildSideType::Right) {
        return task->rightSliceIdentifier;
    } else {
        NES_THROW_RUNTIME_ERROR("Not Implemented");
    }
}

/**
 * @brief Deletes all slices that are not valid anymore
 */
PROXY_FN void deleteAllSlicesProxy(void* ptrOpHandler,
                                   uint64_t watermarkTs,
                                   uint64_t sequenceNumber,
                                   NES::OriginId originId,
                                   uint64_t joinStrategyInt,
                                   uint64_t windowingStrategyInt) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = getSpecificOperatorHandler(ptrOpHandler, joinStrategyInt, windowingStrategyInt);
    TRACE_PROXY_FUNCTION(watermarkTs, sequenceNumber, originId, joinStrategyInt, windowingStrategyInt);
    NES::Runtime::Execution::Operators::BufferMetaData bufferMetaData(watermarkTs, sequenceNumber, originId);
    opHandler->deleteSlices(bufferMetaData);
}

PROXY_FN bool memeq(void* ptr1, void* ptr2, uint64_t size) { return std::memcmp(ptr1, ptr2, size) == 0; }
PROXY_FN void* getKeyedSliceStoreProxy(void* op, uint64_t workerId) {
    TRACE_PROXY_FUNCTION(workerId);
    auto handler = static_cast<NES::Runtime::Execution::Operators::KeyedSlicePreAggregationHandler*>(op);
    return handler->getThreadLocalSliceStore(workerId);
}
PROXY_FN void* findKeyedSliceStateByTsProxy(void* ss, uint64_t ts) {
    auto sliceStore = static_cast<NES::Runtime::Execution::Operators::KeyedThreadLocalSliceStore*>(ss);
    TRACE_PROXY_FUNCTION(ts);
    return sliceStore->findSliceByTs(ts)->getState().get();
}

PROXY_FN void appendToGlobalSliceStoreKeyed(void* ss, void* slicePtr) {
    auto handler = static_cast<
        NES::Runtime::Execution::Operators::AppendToSliceStoreHandler<NES::Runtime::Execution::Operators::KeyedSlice>*>(ss);
    auto slice = std::unique_ptr<NES::Runtime::Execution::Operators::KeyedSlice>(
        (NES::Runtime::Execution::Operators::KeyedSlice*) slicePtr);
    TRACE_PROXY_FUNCTION(*slice);
    handler->appendToGlobalSliceStore(std::move(slice));
}
PROXY_FN void setupWindowHandlerKeyed(void* ss, void* ctx, uint64_t keySize, uint64_t valueSize) {
    auto handler = static_cast<NES::Runtime::Execution::Operators::KeyedSlicePreAggregationHandler*>(ss);
    auto pipelineExecutionContext = static_cast<NES::Unikernel::UnikernelPipelineExecutionContext*>(ctx);
    TRACE_PROXY_FUNCTION(keySize, valueSize);
    handler->setup(*pipelineExecutionContext, keySize, valueSize);
}

PROXY_FN void setupKeyedSliceMergingHandler(void* ss, void* ctx, uint64_t keySize, uint64_t valueSize) {
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

PROXY_FN void* findChainProxy(void* state, uint64_t hash) {
    TRACE_PROXY_FUNCTION(hash);
    auto hashMap = (NES::Nautilus::Interface::ChainedHashMap*) state;
    return hashMap->findChain(hash);
}

PROXY_FN void* insetProxy(void* state, uint64_t hash) {
    TRACE_PROXY_FUNCTION(hash);
    auto hashMap = (NES::Nautilus::Interface::ChainedHashMap*) state;
    return hashMap->insertEntry(hash);
}

PROXY_FN void triggerKeyedThreadLocalWindow(void* op,
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

PROXY_FN uint64_t hashValueUI64(uint64_t seed, uint64_t value) {
    TRACE_PROXY_FUNCTION(seed, value);
    return hashValue(seed, value);
}

PROXY_FN void* createKeyedState(void* op, void* sliceMergeTaskPtr) {
    auto handler = static_cast<NES::Runtime::Execution::Operators::KeyedSliceMergingHandler*>(op);
    auto sliceMergeTask =
        static_cast<NES::Runtime::Execution::Operators::SliceMergeTask<NES::Runtime::Execution::Operators::KeyedSlice>*>(
            sliceMergeTaskPtr);
    TRACE_PROXY_FUNCTION(*sliceMergeTask);
    auto globalState = handler->createGlobalSlice(sliceMergeTask);
    // we give nautilus the ownership, thus deletePartition must be called.
    return globalState.release();
}
PROXY_FN void deleteSliceKeyed(void* gs) {
    auto globalSlice = static_cast<NES::Runtime::Execution::Operators::KeyedSlice*>(gs);
    TRACE_PROXY_FUNCTION(*globalSlice);
    delete globalSlice;
}
PROXY_FN void* getKeyedSliceState(void* gs) {
    auto globalSlice = static_cast<NES::Runtime::Execution::Operators::KeyedSlice*>(gs);
    TRACE_PROXY_FUNCTION(*globalSlice);
    return globalSlice->getState().get();
}

PROXY_FN void* getKeyedSliceStateFromTask(void* smt, uint64_t index) {
    auto task =
        static_cast<NES::Runtime::Execution::Operators::SliceMergeTask<NES::Runtime::Execution::Operators::KeyedSlice>*>(smt);
    TRACE_PROXY_FUNCTION(*task, index);
    return task->slices[index].get()->getState().get();
}

PROXY_FN uint64_t getKeyedNumberOfSlicesFromTask(void* smt) {
    auto task =
        static_cast<NES::Runtime::Execution::Operators::SliceMergeTask<NES::Runtime::Execution::Operators::KeyedSlice>*>(smt);
    TRACE_PROXY_FUNCTION(*task);
    return task->slices.size();
}

PROXY_FN void freeKeyedSliceMergeTask(void* smt) {
    auto task =
        static_cast<NES::Runtime::Execution::Operators::SliceMergeTask<NES::Runtime::Execution::Operators::KeyedSlice>*>(smt);
    TRACE_PROXY_FUNCTION(*task);
    task->~SliceMergeTask();
}
PROXY_FN void* insertProxy(void* state, uint64_t hash) {
    auto hashMap = (NES::Nautilus::Interface::ChainedHashMap*) state;
    TRACE_PROXY_FUNCTION(hash);
    return hashMap->insertEntry(hash);
}
PROXY_FN void* getPageProxy(void* hmPtr, uint64_t pageIndex) {
    auto hashMap = (NES::Nautilus::Interface::ChainedHashMap*) hmPtr;
    TRACE_PROXY_FUNCTION(pageIndex);
    return hashMap->getPage(pageIndex);
}

PROXY_FN void triggerSlidingWindowsKeyed(void* sh, void* wctx, void* pctx, uint64_t sequenceNumber, uint64_t sliceEnd) {
    auto handler = static_cast<
        NES::Runtime::Execution::Operators::AppendToSliceStoreHandler<NES::Runtime::Execution::Operators::KeyedSlice>*>(sh);
    auto workerContext = static_cast<NES::Runtime::WorkerContext*>(wctx);
    auto pipelineExecutionContext = static_cast<NES::Unikernel::UnikernelPipelineExecutionContext*>(pctx);
    TRACE_PROXY_FUNCTION(sequenceNumber, sliceEnd);
    handler->triggerSlidingWindows(*workerContext, *pipelineExecutionContext, sequenceNumber, sliceEnd);
}

PROXY_FN void* NES__Runtime__TupleBuffer__getBuffer(void* thisPtr) {
    auto* thisPtr_ = (NES::Runtime::TupleBuffer*) thisPtr;
    return thisPtr_->getBuffer();
};
PROXY_FN uint64_t NES__Runtime__TupleBuffer__getBufferSize(void* thisPtr) {
    auto* thisPtr_ = (NES::Runtime::TupleBuffer*) thisPtr;
    return thisPtr_->getBufferSize();
};
PROXY_FN uint64_t NES__Runtime__TupleBuffer__getNumberOfTuples(void* thisPtr) {
    auto* thisPtr_ = (NES::Runtime::TupleBuffer*) thisPtr;
    return thisPtr_->getNumberOfTuples();
};
PROXY_FN __attribute__((always_inline)) void NES__Runtime__TupleBuffer__setNumberOfTuples(void* thisPtr,
                                                                                          uint64_t numberOfTuples) {
    NES::Runtime::TupleBuffer* tupleBuffer = static_cast<NES::Runtime::TupleBuffer*>(thisPtr);
    tupleBuffer->setNumberOfTuples(numberOfTuples);
}
PROXY_FN uint64_t NES__Runtime__TupleBuffer__getOriginId(void* thisPtr) {
    auto* thisPtr_ = (NES::Runtime::TupleBuffer*) thisPtr;
    return thisPtr_->getOriginId();
};
PROXY_FN void NES__Runtime__TupleBuffer__setOriginId(void* thisPtr, uint64_t value) {
    auto* thisPtr_ = (NES::Runtime::TupleBuffer*) thisPtr;
    thisPtr_->setOriginId(value);
};

PROXY_FN uint64_t NES__Runtime__TupleBuffer__Watermark(void* thisPtr) {
    auto* thisPtr_ = (NES::Runtime::TupleBuffer*) thisPtr;
    return thisPtr_->getWatermark();
};
PROXY_FN void NES__Runtime__TupleBuffer__setWatermark(void* thisPtr, uint64_t value) {
    auto* thisPtr_ = (NES::Runtime::TupleBuffer*) thisPtr;
    thisPtr_->setWatermark(value);
};
PROXY_FN uint64_t NES__Runtime__TupleBuffer__getCreationTimestampInMS(void* thisPtr) {
    auto* thisPtr_ = (NES::Runtime::TupleBuffer*) thisPtr;
    return thisPtr_->getCreationTimestampInMS();
};
PROXY_FN void NES__Runtime__TupleBuffer__setSequenceNr(void* thisPtr, uint64_t sequenceNumber) {
    auto* thisPtr_ = (NES::Runtime::TupleBuffer*) thisPtr;
    return thisPtr_->setSequenceNumber(sequenceNumber);
};
PROXY_FN uint64_t NES__Runtime__TupleBuffer__getSequenceNumber(void* thisPtr) {
    auto* thisPtr_ = (NES::Runtime::TupleBuffer*) thisPtr;
    return thisPtr_->getSequenceNumber();
}
PROXY_FN void NES__Runtime__TupleBuffer__setCreationTimestampInMS(void* thisPtr, uint64_t value) {
    auto* thisPtr_ = (NES::Runtime::TupleBuffer*) thisPtr;
    return thisPtr_->setCreationTimestampInMS(value);
}

PROXY_FN void* getSliceStoreProxy(void* op, uint64_t workerId) {
    auto handler = static_cast<NES::Runtime::Execution::Operators::NonKeyedSlicePreAggregationHandler*>(op);
    NES_DEBUG("getSliceStoreProxy");
    return handler->getThreadLocalSliceStore(workerId);
}

PROXY_FN void* findSliceStateByTsProxy(void* ss, uint64_t ts) {
    auto sliceStore = static_cast<NES::Runtime::Execution::Operators::NonKeyedThreadLocalSliceStore*>(ss);
    NES_DEBUG("findSliceStateByTsProxy(ts: {})", ts);
    auto slice = sliceStore->findSliceByTs(ts).get();
    auto state = slice->getState().get();
    return state->ptr;
}

PROXY_FN void triggerThreadLocalStateProxy(void* op,
                                           void* wctx,
                                           void* pctx,
                                           uint64_t,
                                           uint64_t originId,
                                           uint64_t sequenceNumber,
                                           uint64_t watermarkTs) {
    auto handler = static_cast<NES::Runtime::Execution::Operators::NonKeyedSlicePreAggregationHandler*>(op);
    auto workerContext = static_cast<NES::Runtime::WorkerContext*>(wctx);
    auto pipelineExecutionContext = static_cast<NES::Runtime::Execution::PipelineExecutionContext*>(pctx);
    handler->trigger(*workerContext, *pipelineExecutionContext, originId, sequenceNumber, watermarkTs);
}

PROXY_FN void* createGlobalState(void* op, void* sliceMergeTaskPtr) {
    auto handler = static_cast<NES::Runtime::Execution::Operators::NonKeyedSliceMergingHandler*>(op);
    auto sliceMergeTask =
        static_cast<NES::Runtime::Execution::Operators::SliceMergeTask<NES::Runtime::Execution::Operators::NonKeyedSlice>*>(
            sliceMergeTaskPtr);
    auto globalState = handler->createGlobalSlice(sliceMergeTask);
    TRACE_PROXY_FUNCTION_NO_ARG
    // we give nautilus the ownership, thus deletePartition must be called.
    return globalState.release();
};

PROXY_FN void setupSliceMergingHandler(void* ss, void* ctx, uint64_t size) {
    auto handler = static_cast<NES::Runtime::Execution::Operators::NonKeyedSliceMergingHandler*>(ss);
    auto pipelineExecutionContext = static_cast<NES::Unikernel::UnikernelPipelineExecutionContext*>(ctx);
    TRACE_PROXY_FUNCTION(size);
    handler->setup(*pipelineExecutionContext, size);
}
PROXY_FN void* getDefaultMergingState(void* ss) {
    auto handler = static_cast<NES::Runtime::Execution::Operators::NonKeyedSliceMergingHandler*>(ss);
    TRACE_PROXY_FUNCTION_NO_ARG;
    return handler->getDefaultState()->ptr;
}
PROXY_FN void setupWindowHandlerNonKeyed(void* ss, void* ctx, uint64_t size) {
    auto handler = static_cast<NES::Runtime::Execution::Operators::NonKeyedSlicePreAggregationHandler*>(ss);
    auto pipelineExecutionContext = static_cast<NES::Unikernel::UnikernelPipelineExecutionContext*>(ctx);
    TRACE_PROXY_FUNCTION_NO_ARG;
    handler->setup(*pipelineExecutionContext, size);
}
PROXY_FN void* getDefaultState(void* ss) {
    auto handler = static_cast<NES::Runtime::Execution::Operators::NonKeyedSlicePreAggregationHandler*>(ss);
    TRACE_PROXY_FUNCTION_NO_ARG;
    return handler->getDefaultState()->ptr;
}

PROXY_FN void* getGlobalSliceState(void* gs) {
    auto globalSlice = static_cast<NES::Runtime::Execution::Operators::NonKeyedSlice*>(gs);
    TRACE_PROXY_FUNCTION(*globalSlice);
    return globalSlice->getState()->ptr;
};

PROXY_FN uint64_t getNonKeyedNumberOfSlices(void* smt) {
    auto task =
        static_cast<NES::Runtime::Execution::Operators::SliceMergeTask<NES::Runtime::Execution::Operators::NonKeyedSlice>*>(smt);
    NES_DEBUG("getNonKeyedNumberOfSlices({})", *task);
    return task->slices.size();
};
PROXY_FN void* getNonKeyedSliceState(void* smt, uint64_t index) {
    auto task =
        static_cast<NES::Runtime::Execution::Operators::SliceMergeTask<NES::Runtime::Execution::Operators::NonKeyedSlice>*>(smt);
    NES_DEBUG("getNonKeyedSliceState({})", *task);
    return task->slices[index].get()->getState()->ptr;
}
PROXY_FN void freeNonKeyedSliceMergeTask(void* smt) {
    auto task =
        static_cast<NES::Runtime::Execution::Operators::SliceMergeTask<NES::Runtime::Execution::Operators::NonKeyedSlice>*>(smt);
    NES_DEBUG("freeNonKeyedSliceMergeTask({})", *task);
    task->~SliceMergeTask();
}
PROXY_FN void deleteNonKeyedSlice(void* slice) {
    auto deleteNonKeyedSlice = static_cast<NES::Runtime::Execution::Operators::NonKeyedSlice*>(slice);
    NES_DEBUG("deleteNonKeyedSlice({})", *deleteNonKeyedSlice);
    delete deleteNonKeyedSlice;
}

PROXY_FN void appendToGlobalSliceStoreNonKeyed(void* ss, void* slicePtr) {
    auto handler = static_cast<
        NES::Runtime::Execution::Operators::AppendToSliceStoreHandler<NES::Runtime::Execution::Operators::NonKeyedSlice>*>(ss);
    auto slice = std::unique_ptr<NES::Runtime::Execution::Operators::NonKeyedSlice>(
        (NES::Runtime::Execution::Operators::NonKeyedSlice*) slicePtr);
    NES_DEBUG("appendToGlobalSliceStoreNonKeyed({})", *slice);
    handler->appendToGlobalSliceStore(std::move(slice));
}

PROXY_FN void triggerSlidingWindowsNonKeyed(void* sh, void* wctx, void* pctx, uint64_t sequenceNumber, uint64_t sliceEnd) {
    auto handler = static_cast<
        NES::Runtime::Execution::Operators::AppendToSliceStoreHandler<NES::Runtime::Execution::Operators::NonKeyedSlice>*>(sh);
    auto workerContext = static_cast<NES::Runtime::WorkerContext*>(wctx);
    auto pipelineExecutionContext = static_cast<NES::Unikernel::UnikernelPipelineExecutionContext*>(pctx);
    NES_DEBUG("triggerSlidingWindowsNonKeyed(seq:{} end:{})", sequenceNumber, sliceEnd);
    handler->triggerSlidingWindows(*workerContext, *pipelineExecutionContext, sequenceNumber, sliceEnd);
}
#endif//PROXYFUNCTIONS_HPP
