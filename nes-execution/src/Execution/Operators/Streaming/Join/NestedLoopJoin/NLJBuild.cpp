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

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/SliceStore/Slice.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJBuild.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJSlice.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinBuild.hpp>
#include <Execution/Operators/Watermark/TimeFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Execution.hpp>
#include <Util/SliceCache/SliceCache.hpp>
#include <nautilus/val_enum.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES::Runtime::Execution::Operators
{
SliceStart getNLJSliceStartProxy(const NLJSlice* nljSlice)
{
    PRECONDITION(nljSlice != nullptr, "nlj slice pointer should not be null!");
    return nljSlice->getSliceStart();
}

SliceEnd getNLJSliceEndProxy(const NLJSlice* nljSlice)
{
    PRECONDITION(nljSlice != nullptr, "nlj slice pointer should not be null!");
    return nljSlice->getSliceEnd();
}

NLJSlice* getNLJSliceRefProxy(OperatorHandler* ptrOpHandler, const Timestamp timestamp, const WorkerThreadId workerThreadId)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");
    const auto* opHandler = dynamic_cast<NLJOperatorHandler*>(ptrOpHandler);
    const auto createFunction = opHandler->getCreateNewSlicesFunction();
    auto localSliceCache = opHandler->getSliceCacheForWorker(workerThreadId);
    auto sliceFromCache = localSliceCache->getSliceFromCache(timestamp);
    if (sliceFromCache.has_value())
    {
        return dynamic_cast<NLJSlice*>(sliceFromCache.value().get());
    }
    auto slice = opHandler->getSliceAndWindowStore().getSlicesOrCreate(timestamp, createFunction)[0];
    localSliceCache->passSliceToCache(slice->getSliceEnd(), slice);
    return dynamic_cast<NLJSlice*>(slice.get());
}

void logCacheStatsProxy(OperatorHandler* ptrOpHandler, const WorkerThreadId workerThreadId)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");
    const auto* opHandler = dynamic_cast<NLJOperatorHandler*>(ptrOpHandler);

    auto localSliceCache = opHandler->getSliceCacheForWorker(workerThreadId);

    std::ofstream cacheFile;
    std::filesystem::path folderPath("cache");
    auto fileName = "cache_" + std::to_string(workerThreadId.getRawValue()) + ".txt";
    std::filesystem::path filePath = folderPath / fileName;
    cacheFile.open(filePath, std::ios_base::app);
    if (cacheFile.is_open())
    {
        cacheFile << "Hits: " << localSliceCache->getNumberOfCacheHits() << " Misses: " << localSliceCache->getNumberOfCacheMisses()
                  << std::endl;
        cacheFile.flush();
    }
    localSliceCache->resetCounters();
}

NLJBuild::NLJBuild(
    const uint64_t operatorHandlerIndex,
    const QueryCompilation::JoinBuildSideType joinBuildSide,
    std::unique_ptr<TimeFunction> timeFunction,
    const std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider>& memoryProvider)
    : StreamJoinBuild(operatorHandlerIndex, joinBuildSide, std::move(timeFunction), memoryProvider)
{
}

void NLJBuild::setup(ExecutionContext& executionCtx) const
{
    StreamJoinBuild::setup(executionCtx);
    std::filesystem::path folderPath("cache");
    if (!std::filesystem::exists(folderPath))
    {
        std::filesystem::create_directories(folderPath);
    }
}

void NLJBuild::open(ExecutionContext&, RecordBuffer&) const
{
    nautilus::invoke(+[]() { NES_INFO("executing NLJbuild") });
    // auto opHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);

    // auto sliceReference = invoke(getNLJSliceRefProxy, opHandlerMemRef, recordBuffer.getWatermarkTs(), executionCtx.getWorkerThreadId());
    // auto sliceStart = invoke(getNLJSliceStartProxy, sliceReference);
    // auto sliceEnd = invoke(getNLJSliceEndProxy, sliceReference);
    // const auto pagedVectorReference = invoke(
    //     getNLJPagedVectorProxy,
    //     sliceReference,
    //     executionCtx.getWorkerThreadId(),
    //     nautilus::val<QueryCompilation::JoinBuildSideType>(joinBuildSide));


    // auto localJoinState
    //     = std::make_unique<LocalNestedLoopJoinState>(opHandlerMemRef, sliceReference, sliceStart, sliceEnd, pagedVectorReference);

    // /// Store the local state
    // executionCtx.setLocalOperatorState(this, std::move(localJoinState));
}


void NLJBuild::execute(ExecutionContext& executionCtx, Record& record) const
{
    /// Get the current join state that stores the slice / pagedVector that we have to insert the tuple into
    const auto timestamp = timeFunction->getTs(executionCtx, record);
    // auto* localJoinState = getLocalJoinState(executionCtx, timestamp);

    // Skip directly to the slice cache and store
    const auto operatorHandlerRef = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    const auto sliceReference = invoke(getNLJSliceRefProxy, operatorHandlerRef, timestamp, executionCtx.getWorkerThreadId());
    const auto nljPagedVectorMemRef = invoke(
        getNLJPagedVectorProxy,
        sliceReference,
        executionCtx.getWorkerThreadId(),
        nautilus::val<QueryCompilation::JoinBuildSideType>(joinBuildSide));

    /// Write record to the pagedVector
    const Interface::PagedVectorRef pagedVectorRef(nljPagedVectorMemRef, memoryProvider);
    pagedVectorRef.writeRecord(record);
}

void NLJBuild::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    StreamJoinBuild::close(executionCtx, recordBuffer);
    invoke(logCacheStatsProxy, executionCtx.getGlobalOperatorHandler(operatorHandlerIndex), executionCtx.getWorkerThreadId());
}

NLJBuild::LocalNestedLoopJoinState*
NLJBuild::getLocalJoinState(ExecutionContext& executionCtx, const nautilus::val<Timestamp>& timestamp) const
{
    auto* localJoinState = dynamic_cast<LocalNestedLoopJoinState*>(executionCtx.getLocalState(this));
    const auto operatorHandlerMemRef = localJoinState->joinOperatorHandler;

    if (!(localJoinState->sliceStart <= timestamp && timestamp < localJoinState->sliceEnd))
    {
        /// Get the slice for the current timestamp
        updateLocalJoinState(localJoinState, operatorHandlerMemRef, executionCtx, timestamp);
    }
    return localJoinState;
}

void NLJBuild::updateLocalJoinState(
    LocalNestedLoopJoinState* localJoinState,
    const nautilus::val<NLJOperatorHandler*>& operatorHandlerRef,
    const ExecutionContext& executionCtx,
    const nautilus::val<Timestamp>& timestamp) const
{
    localJoinState->sliceReference = invoke(getNLJSliceRefProxy, operatorHandlerRef, timestamp, executionCtx.getWorkerThreadId());
    localJoinState->sliceStart = invoke(getNLJSliceStartProxy, localJoinState->sliceReference);
    localJoinState->sliceEnd = invoke(getNLJSliceEndProxy, localJoinState->sliceReference);
    localJoinState->nljPagedVectorMemRef = invoke(
        getNLJPagedVectorProxy,
        localJoinState->sliceReference,
        executionCtx.getWorkerThreadId(),
        nautilus::val<QueryCompilation::JoinBuildSideType>(joinBuildSide));
}
}
