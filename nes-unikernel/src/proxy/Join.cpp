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

#include <Configurations/Enums/WindowingStrategy.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJSlice.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Slicing/NLJOperatorHandlerSlicing.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <cstdint>
#include <proxy/common.hpp>

/**
     * @brief This method casts from a void* pointer depending on the join and window strategy to the correct derived class
     * and then pack to a parent class. This is necessary, as we do not always exactly know the child class.
     * @tparam OutputClass class to be casted to
     * @param ptrOpHandler
     * @param joinStrategyInt
     * @param windowingStrategyInt
     * @return OutputClass*
     */
// template<typename OutputClass = NES::Runtime::Execution::Operators::StreamJoinOperatorHandler>
static_assert(sizeof(NES::Runtime::Execution::Operators::NLJOperatorHandlerSlicing) == 280);
static_assert(sizeof(std::tuple<NES::Runtime::Execution::Operators::NLJOperatorHandlerSlicing, NES::Runtime::Execution::Operators::NLJOperatorHandlerSlicing>) == 560);
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

EXT_C uint64_t getNLJSliceStartProxy(void* ptrNljSlice) {
    NES_ASSERT2_FMT(ptrNljSlice != nullptr, "nlj slice pointer should not be null!");
    auto* nljSlice = static_cast<NES::Runtime::Execution::NLJSlice*>(ptrNljSlice);
    TRACE_PROXY_FUNCTION(*nljSlice);
    return nljSlice->getSliceStart();
}

EXT_C uint64_t getNLJSliceEndProxy(void* ptrNljSlice) {
    NES_ASSERT2_FMT(ptrNljSlice != nullptr, "nlj slice pointer should not be null!");
    auto* nljSlice = static_cast<NES::Runtime::Execution::NLJSlice*>(ptrNljSlice);
    TRACE_PROXY_FUNCTION(*nljSlice)
    return nljSlice->getSliceEnd();
}

EXT_C void* getCurrentWindowProxy(void* ptrOpHandler) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<NES::Runtime::Execution::Operators::NLJOperatorHandlerSlicing*>(ptrOpHandler);
    TRACE_PROXY_FUNCTION_NO_ARG;
    return opHandler->getCurrentSliceOrCreate();
}

EXT_C void* getNLJSliceRefProxy(void* ptrOpHandler, uint64_t timestamp) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<NES::Runtime::Execution::Operators::NLJOperatorHandlerSlicing*>(ptrOpHandler);
    TRACE_PROXY_FUNCTION(timestamp)
    return opHandler->getSliceByTimestampOrCreateIt(timestamp).get();
}

EXT_C void* getNLJPagedVectorProxy(void* ptrNljSlice, uint64_t workerId, uint64_t joinBuildSideInt) {
    NES_ASSERT2_FMT(ptrNljSlice != nullptr, "nlj slice pointer should not be null!");
    auto joinBuildSide = magic_enum::enum_cast<NES::QueryCompilation::JoinBuildSideType>(joinBuildSideInt).value();
    auto* nljSlice = static_cast<NES::Runtime::Execution::NLJSlice*>(ptrNljSlice);
    TRACE_PROXY_FUNCTION(workerId, magic_enum::enum_name(joinBuildSide));
    switch (joinBuildSide) {
        case NES::QueryCompilation::JoinBuildSideType::Left: return nljSlice->getPagedVectorRefLeft(workerId);
        case NES::QueryCompilation::JoinBuildSideType::Right: return nljSlice->getPagedVectorRefRight(workerId);
    }
}

EXT_C void allocateNewPageProxy(void* pagedVectorPtr) {
    auto* pagedVector = (NES::Nautilus::Interface::PagedVector*) pagedVectorPtr;
    TRACE_PROXY_FUNCTION(*pagedVector);
    pagedVector->appendPage();
}

EXT_C void* getPagedVectorPageProxy(void* pagedVectorPtr, uint64_t pagePos) {
    auto* pagedVector = (NES::Nautilus::Interface::PagedVector*) pagedVectorPtr;
    TRACE_PROXY_FUNCTION(*pagedVector, pagePos);
    return pagedVector->getPages()[pagePos];
}

EXT_C void checkWindowsTriggerProxy(void* ptrOpHandler,
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

EXT_C void
triggerAllWindowsProxy(void* ptrOpHandler, void* ptrPipelineCtx, uint64_t joinStrategyInt, uint64_t windowingStrategyInt) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    NES_ASSERT2_FMT(ptrPipelineCtx != nullptr, "pipeline context should not be null");

    TRACE_PROXY_FUNCTION(joinStrategyInt, windowingStrategyInt);
    auto* opHandler = getSpecificOperatorHandler(ptrOpHandler, joinStrategyInt, windowingStrategyInt);
    auto* pipelineCtx = static_cast<NES::Unikernel::UnikernelPipelineExecutionContext*>(ptrPipelineCtx);

    opHandler->triggerAllSlices(pipelineCtx);
}

EXT_C void setNumberOfWorkerThreadsProxy(void* ptrOpHandler,
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

EXT_C void* getNLJSliceRefFromIdProxy(void* ptrOpHandler, uint64_t sliceIdentifier) {
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

EXT_C uint64_t getNLJWindowStartProxy(void* ptrNLJWindowTriggerTask) {
    NES_ASSERT2_FMT(ptrNLJWindowTriggerTask != nullptr, "ptrNLJWindowTriggerTask should not be null");
    auto task = static_cast<NES::Runtime::Execution::Operators::EmittedNLJWindowTriggerTask*>(ptrNLJWindowTriggerTask);
    TRACE_PROXY_FUNCTION(*task);
    return task->windowInfo.windowStart;
}

EXT_C uint64_t getNLJWindowEndProxy(void* ptrNLJWindowTriggerTask) {
    NES_ASSERT2_FMT(ptrNLJWindowTriggerTask != nullptr, "ptrNLJWindowTriggerTask should not be null");
    auto task = static_cast<NES::Runtime::Execution::Operators::EmittedNLJWindowTriggerTask*>(ptrNLJWindowTriggerTask);
    TRACE_PROXY_FUNCTION(*task);
    return task->windowInfo.windowEnd;
}

EXT_C uint64_t getSliceIdNLJProxy(void* ptrNLJWindowTriggerTask, uint64_t joinBuildSideInt) {
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
EXT_C void deleteAllSlicesProxy(void* ptrOpHandler,
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