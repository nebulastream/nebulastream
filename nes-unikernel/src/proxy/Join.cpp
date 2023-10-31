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
static OutputClass* getSpecificOperatorHandler(void* ptrOpHandler, uint64_t joinStrategyInt, uint64_t windowingStrategyInt) {

    auto joinStrategy = magic_enum::enum_value<NES::QueryCompilation::StreamJoinStrategy>(joinStrategyInt);
    auto windowingStrategy = magic_enum::enum_value<NES::QueryCompilation::WindowingStrategy>(windowingStrategyInt);
    switch (joinStrategy) {
        case NES::QueryCompilation::StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCKING:
        case NES::QueryCompilation::StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCK_FREE:
        case NES::QueryCompilation::StreamJoinStrategy::HASH_JOIN_LOCAL: {
            NES_THROW_RUNTIME_ERROR("Windowing strategy was used that is not supported with this compiler!");
        }
        case NES::QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN: {
            if (windowingStrategy == NES::QueryCompilation::WindowingStrategy::SLICING) {
                auto* tmpOpHandler = static_cast<NES::Runtime::Execution::Operators::NLJOperatorHandlerSlicing*>(ptrOpHandler);
                return dynamic_cast<OutputClass*>(tmpOpHandler);
            } else {
                NES_THROW_RUNTIME_ERROR("Windowing strategy was used that is not supported with this compiler!");
            }
        }
    }
}

extern "C" uint64_t getNLJSliceStartProxy(void* ptrNljSlice) {
    NES_ASSERT2_FMT(ptrNljSlice != nullptr, "nlj slice pointer should not be null!");
    auto* nljSlice = static_cast<NES::Runtime::Execution::NLJSlice*>(ptrNljSlice);
    return nljSlice->getSliceStart();
}

extern "C" uint64_t getNLJSliceEndProxy(void* ptrNljSlice) {
    NES_ASSERT2_FMT(ptrNljSlice != nullptr, "nlj slice pointer should not be null!");
    auto* nljSlice = static_cast<NES::Runtime::Execution::NLJSlice*>(ptrNljSlice);
    return nljSlice->getSliceEnd();
}

extern "C" void* getCurrentWindowProxy(void* ptrOpHandler) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<NES::Runtime::Execution::Operators::NLJOperatorHandlerSlicing*>(ptrOpHandler);
    return opHandler->getCurrentSliceOrCreate();
}

extern "C" void* getNLJSliceRefProxy(void* ptrOpHandler, uint64_t timestamp) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<NES::Runtime::Execution::Operators::NLJOperatorHandlerSlicing*>(ptrOpHandler);
    return opHandler->getSliceByTimestampOrCreateIt(timestamp).get();
}

extern "C" void* getNLJPagedVectorProxy(void* ptrNljSlice, uint64_t workerId, uint64_t joinBuildSideInt) {
    NES_ASSERT2_FMT(ptrNljSlice != nullptr, "nlj slice pointer should not be null!");
    auto joinBuildSide = magic_enum::enum_cast<NES::QueryCompilation::JoinBuildSideType>(joinBuildSideInt).value();
    auto* nljSlice = static_cast<NES::Runtime::Execution::NLJSlice*>(ptrNljSlice);
    NES_DEBUG("nljSlice:\n{}", nljSlice->toString());
    switch (joinBuildSide) {
        case NES::QueryCompilation::JoinBuildSideType::Left: return nljSlice->getPagedVectorRefLeft(workerId);
        case NES::QueryCompilation::JoinBuildSideType::Right: return nljSlice->getPagedVectorRefRight(workerId);
    }
}

extern "C" void allocateNewPageProxy(void* pagedVectorPtr) {
    auto* pagedVector = (NES::Nautilus::Interface::PagedVector*) pagedVectorPtr;
    pagedVector->appendPage();
}

extern "C" void* getPagedVectorPageProxy(void* pagedVectorPtr, uint64_t pagePos) {
    auto* pagedVector = (NES::Nautilus::Interface::PagedVector*) pagedVectorPtr;
    return pagedVector->getPages()[pagePos];
}

extern "C" void checkWindowsTriggerProxy(void* ptrOpHandler,
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

    auto* opHandler = getSpecificOperatorHandler(ptrOpHandler, joinStrategyInt, windowingStrategyInt);
    auto* pipelineCtx = static_cast<NES::Runtime::Execution::PipelineExecutionContext*>(ptrPipelineCtx);
    auto* workerCtx = static_cast<NES::Runtime::WorkerContext*>(ptrWorkerCtx);

    //update last seen watermark by this worker
    opHandler->updateWatermarkForWorker(watermarkTs, workerCtx->getId());
    auto minWatermark = opHandler->getMinWatermarkForWorker();

    NES::Runtime::Execution::Operators::BufferMetaData bufferMetaData(minWatermark, sequenceNumber, originId);
    opHandler->checkAndTriggerWindows(bufferMetaData, pipelineCtx);
}

extern "C" void
triggerAllWindowsProxy(void* ptrOpHandler, void* ptrPipelineCtx, uint64_t joinStrategyInt, uint64_t windowingStrategyInt) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    NES_ASSERT2_FMT(ptrPipelineCtx != nullptr, "pipeline context should not be null");

    auto* opHandler = getSpecificOperatorHandler(ptrOpHandler, joinStrategyInt, windowingStrategyInt);
    auto* pipelineCtx = static_cast<NES::Runtime::Execution::PipelineExecutionContext*>(ptrPipelineCtx);

    opHandler->triggerAllSlices(pipelineCtx);
}

extern "C" void setNumberOfWorkerThreadsProxy(void* ptrOpHandler,
                                              void* ptrPipelineContext,
                                              uint64_t joinStrategyInt,
                                              uint64_t windowingStrategyInt) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    NES_ASSERT2_FMT(ptrPipelineContext != nullptr, "pipeline context should not be null!");

    auto* opHandler = getSpecificOperatorHandler(ptrOpHandler, joinStrategyInt, windowingStrategyInt);
    auto* pipelineCtx = static_cast<NES::Runtime::Execution::PipelineExecutionContext*>(ptrPipelineContext);

    opHandler->setNumberOfWorkerThreads(pipelineCtx->getNumberOfWorkerThreads());
}

extern "C" void* getNLJSliceRefFromIdProxy(void* ptrOpHandler, uint64_t sliceIdentifier) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");
    const auto opHandler = static_cast<NES::Runtime::Execution::Operators::NLJOperatorHandlerSlicing*>(ptrOpHandler);
    auto slice = opHandler->getSliceBySliceIdentifier(sliceIdentifier);
    if (slice.has_value()) {
        return slice.value().get();
    }
    // For now this is fine. We should handle this as part of issue #4016
    NES_ERROR("Could not find a slice with the id: {}", sliceIdentifier);
    return nullptr;
}

extern "C" uint64_t getNLJWindowStartProxy(void* ptrNLJWindowTriggerTask) {
    NES_ASSERT2_FMT(ptrNLJWindowTriggerTask != nullptr, "ptrNLJWindowTriggerTask should not be null");
    return static_cast<NES::Runtime::Execution::Operators::EmittedNLJWindowTriggerTask*>(ptrNLJWindowTriggerTask)
        ->windowInfo.windowStart;
}

extern "C" uint64_t getNLJWindowEndProxy(void* ptrNLJWindowTriggerTask) {
    NES_ASSERT2_FMT(ptrNLJWindowTriggerTask != nullptr, "ptrNLJWindowTriggerTask should not be null");
    return static_cast<NES::Runtime::Execution::Operators::EmittedNLJWindowTriggerTask*>(ptrNLJWindowTriggerTask)
        ->windowInfo.windowEnd;
}

extern "C" uint64_t getSliceIdNLJProxy(void* ptrNLJWindowTriggerTask, uint64_t joinBuildSideInt) {
    NES_ASSERT2_FMT(ptrNLJWindowTriggerTask != nullptr, "ptrNLJWindowTriggerTask should not be null");
    auto joinBuildSide = magic_enum::enum_cast<NES::QueryCompilation::JoinBuildSideType>(joinBuildSideInt).value();

    if (joinBuildSide == NES::QueryCompilation::JoinBuildSideType::Left) {
        return static_cast<NES::Runtime::Execution::Operators::EmittedNLJWindowTriggerTask*>(ptrNLJWindowTriggerTask)
            ->leftSliceIdentifier;
    } else if (joinBuildSide == NES::QueryCompilation::JoinBuildSideType::Right) {
        return static_cast<NES::Runtime::Execution::Operators::EmittedNLJWindowTriggerTask*>(ptrNLJWindowTriggerTask)
            ->rightSliceIdentifier;
    } else {
        NES_THROW_RUNTIME_ERROR("Not Implemented");
    }
}

/**
 * @brief Deletes all slices that are not valid anymore
 */
extern "C" void deleteAllSlicesProxy(void* ptrOpHandler,
                                     uint64_t watermarkTs,
                                     uint64_t sequenceNumber,
                                     NES::OriginId originId,
                                     uint64_t joinStrategyInt,
                                     uint64_t windowingStrategyInt) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = getSpecificOperatorHandler(ptrOpHandler, joinStrategyInt, windowingStrategyInt);
    NES::Runtime::Execution::Operators::BufferMetaData bufferMetaData(watermarkTs, sequenceNumber, originId);
    opHandler->deleteSlices(bufferMetaData);
}