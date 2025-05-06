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
#include <Execution/Operators/SliceCache/SliceCacheFIFO.hpp>
#include <Execution/Operators/SliceCache/SliceCacheLRU.hpp>
#include <Execution/Operators/SliceCache/SliceCacheSecondChance.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJBuildCache.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <QueryCompiler/Configurations/QueryCompilerConfiguration.hpp>
#include <nautilus/std/cstdlib.h>
#include <nautilus/val_enum.hpp>

namespace NES::Runtime::Execution::Operators
{
NLJBuildCache::NLJBuildCache(
    const uint64_t operatorHandlerIndex,
    const QueryCompilation::JoinBuildSideType joinBuildSide,
    std::unique_ptr<TimeFunction> timeFunction,
    const std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider>& memoryProvider,
    const QueryCompilation::Configurations::SliceCacheOptions& sliceCacheOptions)
    : StreamJoinBuild(operatorHandlerIndex, joinBuildSide, std::move(timeFunction), memoryProvider), sliceCacheOptions(sliceCacheOptions)
{
}

void NLJBuildCache::setup(ExecutionContext& executionCtx) const
{
    StreamJoinBuild::setup(executionCtx);
    /// I know that we should not set the numberOfWorkerThreads in the build operator, due to a race condition.
    /// However, we need to set it here, as we need to know the number of worker threads to allocate the slice cache entries.
    /// This should/is fine for this PoC
    const auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    nautilus::invoke(
        +[](NLJOperatorHandler* opHandler, const PipelineExecutionContext* pipelineExecutionContext)
        { opHandler->setWorkerThreads(pipelineExecutionContext->getNumberOfWorkerThreads()); },
        globalOperatorHandler,
        executionCtx.pipelineContext);

    /// Allocating the memory for the slice cache, so that we can use it in the execute method
    nautilus::val<uint64_t> sizeOfEntry = 0;
    nautilus::val<uint64_t> numberOfEntries = sliceCacheOptions.numberOfEntries;
    switch (sliceCacheOptions.sliceCacheType)
    {
        case QueryCompilation::Configurations::SliceCacheType::NONE:
            return;
        case QueryCompilation::Configurations::SliceCacheType::FIFO:
            sizeOfEntry = sizeof(SliceCacheEntryFIFO);
            break;
        case QueryCompilation::Configurations::SliceCacheType::LRU:
            sizeOfEntry = sizeof(SliceCacheEntryLRU);
            break;
        case QueryCompilation::Configurations::SliceCacheType::SECOND_CHANCE:
            sizeOfEntry = sizeof(SliceCacheEntrySecondChance);
            break;
    }

    nautilus::invoke(
        +[](NLJOperatorHandler* opHandler,
            Memory::AbstractBufferProvider* bufferProvider,
            const uint64_t sizeOfEntryVal,
            const uint64_t numberOfEntriesVal)
        { opHandler->allocateSliceCacheEntries(sizeOfEntryVal, numberOfEntriesVal, bufferProvider); },
        globalOperatorHandler,
        executionCtx.pipelineMemoryProvider.bufferProvider,
        sizeOfEntry,
        numberOfEntries);
}

void NLJBuildCache::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    StreamJoinBuild::open(executionCtx, recordBuffer);

    /// Creating the local state for the slice cache, so that we can use it in the execute method
    const auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    const auto startOfSliceEntries = nautilus::invoke(
        +[](NLJOperatorHandler* opHandler, const WorkerThreadId workerThreadId, const QueryCompilation::JoinBuildSideType joinBuildSide)
        { return opHandler->getStartOfSliceCacheEntries(workerThreadId, joinBuildSide); },
        globalOperatorHandler,
        executionCtx.getWorkerThreadId(),
        nautilus::val<QueryCompilation::JoinBuildSideType>(joinBuildSide));
    const auto hitsRef = startOfSliceEntries;
    const auto missesRef = hitsRef + nautilus::val<uint64_t>(sizeof(uint64_t));
    const auto sliceCacheEntries = startOfSliceEntries + nautilus::val<uint64_t>(sizeof(HitsAndMisses));
    const auto startOfDataEntry = nautilus::invoke(
        +[](const SliceCacheEntry* sliceCacheEntry)
        {
            const auto startOfDataStructure = &sliceCacheEntry->dataStructure;
            return startOfDataStructure;
        },
        sliceCacheEntries);

    switch (sliceCacheOptions.sliceCacheType)
    {
        case QueryCompilation::Configurations::SliceCacheType::NONE:
            break;
        case QueryCompilation::Configurations::SliceCacheType::FIFO:
            executionCtx.setLocalOperatorState(
                this,
                std::make_unique<SliceCacheFIFO>(
                    sliceCacheOptions.numberOfEntries,
                    sizeof(SliceCacheEntryFIFO),
                    sliceCacheEntries,
                    startOfDataEntry,
                    hitsRef,
                    missesRef));
            break;
        case QueryCompilation::Configurations::SliceCacheType::LRU:
            executionCtx.setLocalOperatorState(
                this,
                std::make_unique<SliceCacheLRU>(
                    sliceCacheOptions.numberOfEntries,
                    sizeof(SliceCacheEntryLRU),
                    sliceCacheEntries,
                    startOfDataEntry,
                    hitsRef,
                    missesRef));
            break;
        case QueryCompilation::Configurations::SliceCacheType::SECOND_CHANCE:
            executionCtx.setLocalOperatorState(
                this,
                std::make_unique<SliceCacheSecondChance>(
                    sliceCacheOptions.numberOfEntries,
                    sizeof(SliceCacheEntrySecondChance),
                    sliceCacheEntries,
                    startOfDataEntry,
                    hitsRef,
                    missesRef));
            break;
    }
}

int8_t* createNewNLJSliceProxy(
    SliceCacheEntry* sliceCacheEntry,
    OperatorHandler* ptrOpHandler,
    const Timestamp timestamp,
    const WorkerThreadId workerThreadId,
    const QueryCompilation::JoinBuildSideType joinBuildSide)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");
    const auto* opHandler = dynamic_cast<NLJOperatorHandler*>(ptrOpHandler);
    const auto createFunction = opHandler->getCreateNewSlicesFunction();
    const auto newNLJSlice
        = dynamic_cast<NLJSlice*>(opHandler->getSliceAndWindowStore().getSlicesOrCreate(timestamp, createFunction)[0].get());

    /// Updating the slice cache entry with the new slice
    sliceCacheEntry->sliceStart = newNLJSlice->getSliceStart();
    sliceCacheEntry->sliceEnd = newNLJSlice->getSliceEnd();
    sliceCacheEntry->dataStructure = reinterpret_cast<int8_t*>(newNLJSlice->getPagedVectorRef(workerThreadId, joinBuildSide));
    return sliceCacheEntry->dataStructure;
}


void NLJBuildCache::execute(ExecutionContext& executionCtx, Record& record) const
{
    /// Getting the slice cache from the local state
    const auto sliceCache = dynamic_cast<SliceCache*>(executionCtx.getLocalState(this));

    /// Getting the current paged vector that we have to insert the tuple into
    const auto timestamp = timeFunction->getTs(executionCtx, record);
    const auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    const auto nljPagedVectorRef = sliceCache->getDataStructureRef(
        timestamp,
        [&](const nautilus::val<SliceCacheEntry*>& sliceCacheEntryToReplace)
        {
            return nautilus::invoke(
                createNewNLJSliceProxy,
                sliceCacheEntryToReplace,
                globalOperatorHandler,
                timestamp,
                executionCtx.getWorkerThreadId(),
                nautilus::val<QueryCompilation::JoinBuildSideType>(joinBuildSide));
        });

    /// Write record to the pagedVector
    const Interface::PagedVectorRef pagedVectorRef(nljPagedVectorRef, memoryProvider, executionCtx.pipelineMemoryProvider.bufferProvider);
    pagedVectorRef.writeRecord(record, executionCtx.pipelineMemoryProvider.bufferProvider);
}
}
