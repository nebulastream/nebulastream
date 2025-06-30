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
#include <Join/NestedLoopJoin/NLJBuildCachePhysicalOperator.hpp>

#include <memory>
#include <utility>
#include <Configurations/Worker/SliceCacheConfiguration.hpp>
#include <Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Join/NestedLoopJoin/NLJSlice.hpp>
#include <Join/StreamJoinBuildPhysicalOperator.hpp>
#include <Join/StreamJoinOperatorHandler.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceCache/SliceCache2Q.hpp>
#include <SliceCache/SliceCacheFIFO.hpp>
#include <SliceCache/SliceCacheLRU.hpp>
#include <SliceCache/SliceCacheSecondChance.hpp>
#include <SliceCache/SliceCacheUtil.hpp>
#include <SliceStore/Slice.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/TimeFunction.hpp>
#include <nautilus/val_enum.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <WindowBuildPhysicalOperator.hpp>
#include <function.hpp>

namespace NES
{

NLJBuildCachePhysicalOperator::NLJBuildCachePhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    const JoinBuildSideType joinBuildSide,
    std::unique_ptr<TimeFunction> timeFunction,
    std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider,
    const NES::Configurations::SliceCacheOptions sliceCacheOptions)
    : NLJBuildPhysicalOperator(operatorHandlerId, joinBuildSide, std::move(timeFunction), std::move(memoryProvider))
    , sliceCacheOptions(sliceCacheOptions)
{
}

void NLJBuildCachePhysicalOperator::setup(ExecutionContext& executionCtx, const nautilus::engine::NautilusEngine& engine) const
{
    NLJBuildPhysicalOperator::setup(executionCtx, engine);

    /// I know that we should not set the numberOfWorkerThreads in the build operator, due to a race condition.
    /// However, we need to set it here, as we need to know the number of worker threads to allocate the slice cache entries.
    /// This should/is fine for this PoC
    const auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
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
        case NES::Configurations::SliceCacheType::NONE:
            return;
        case NES::Configurations::SliceCacheType::FIFO:
            sizeOfEntry = sizeof(SliceCacheEntryFIFO);
            break;
        case NES::Configurations::SliceCacheType::LRU:
            sizeOfEntry = sizeof(SliceCacheEntryLRU);
            break;
        case NES::Configurations::SliceCacheType::SECOND_CHANCE:
            sizeOfEntry = sizeof(SliceCacheEntrySecondChance);
            break;
        case Configurations::SliceCacheType::TWO_QUEUES:
            sizeOfEntry = sizeof(SliceCacheEntry2Q);
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

void NLJBuildCachePhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    NLJBuildPhysicalOperator::open(executionCtx, recordBuffer);

    /// Creating the local state for the slice cache, so that we can use it in the execute method
    const auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto startOfSliceEntries = nautilus::invoke(
        +[](const NLJOperatorHandler* opHandler, const WorkerThreadId workerThreadId, const JoinBuildSideType joinBuildSide)
        {
            return opHandler->getStartOfSliceCacheEntries(
                StreamJoinOperatorHandler::StartSliceCacheEntriesStreamJoin{workerThreadId, joinBuildSide});
        },
        globalOperatorHandler,
        executionCtx.workerThreadId,
        nautilus::val<JoinBuildSideType>(joinBuildSide));
    auto sliceCache = NES::Util::createSliceCache(sliceCacheOptions, globalOperatorHandler, startOfSliceEntries);
    executionCtx.setLocalOperatorState(id, std::move(sliceCache));
}

void NLJBuildCachePhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    /// Getting the operator handler from the slice cache
    auto* sliceCache = dynamic_cast<SliceCache*>(executionCtx.getLocalState(id));
    auto operatorHandler = sliceCache->getOperatorHandler();

    /// Get the current slice / pagedVector that we have to insert the tuple into
    const auto timestamp = timeFunction->getTs(executionCtx, record);
    const auto nljPagedVectorMemRef = sliceCache->getDataStructureRef(
        timestamp,
        [&](const nautilus::val<SliceCacheEntry*>& sliceCacheEntryToReplace, const nautilus::val<uint64_t>&)
        {
            return nautilus::invoke(
                +[](SliceCacheEntry* sliceCacheEntry,
                    const NLJOperatorHandler* opHandler,
                    const Timestamp timestampVal,
                    const WorkerThreadId workerThreadId,
                    const JoinBuildSideType joinBuildSide)
                {
                    PRECONDITION(opHandler != nullptr, "opHandler context should not be null!");
                    const auto createFunction = opHandler->getCreateNewSlicesFunction({});
                    const auto newSlices = opHandler->getSliceAndWindowStore().getSlicesOrCreate(timestampVal, createFunction);
                    INVARIANT(newSlices.size() == 1, "We expect exactly one slice for the NLJ");
                    const auto newNLJSlice = std::dynamic_pointer_cast<NLJSlice>(newSlices[0]);

                    /// Updating the slice cache entry with the new slice
                    sliceCacheEntry->sliceStart = newNLJSlice->getSliceStart();
                    sliceCacheEntry->sliceEnd = newNLJSlice->getSliceEnd();
                    sliceCacheEntry->dataStructure
                        = reinterpret_cast<int8_t*>(newNLJSlice->getPagedVectorRef(workerThreadId, joinBuildSide));
                    return sliceCacheEntry->dataStructure;
                },
                sliceCacheEntryToReplace,
                operatorHandler,
                timestamp,
                executionCtx.workerThreadId,
                nautilus::val<JoinBuildSideType>(joinBuildSide));
        });

    /// Write record to the pagedVector
    const Interface::PagedVectorRef pagedVectorRef(nljPagedVectorMemRef, memoryProvider);
    pagedVectorRef.writeRecord(record, executionCtx.pipelineMemoryProvider.bufferProvider);
}
}
