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

#include <Join/NestedLoopJoin/NLJSlice.hpp>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <numeric>
#include <string>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Spill/ArenaMemoryResource.hpp>
#include <Runtime/Spill/SpillManager.hpp>
#include <SliceStore/Slice.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
std::atomic<uint64_t> nljSpillSliceCounter{0};
/// The per-slice BufferManager's pooled area is unused (PagedVector main buffers and pages are allocated via
/// getUnpooledBuffer, which routes through the arena), so it is kept tiny; this page size only sizes that unused
/// pooled area.
constexpr uint32_t NLJ_SLICE_POOL_PAGE_SIZE = 4096;
constexpr uint32_t NLJ_SLICE_POOL_BUFFERS = 2;
}

NLJSlice::NLJSlice(
    AbstractBufferProvider& bufferProvider,
    const SliceStart sliceStart,
    const SliceEnd sliceEnd,
    const uint64_t numberOfWorkerThreads,
    const uint64_t tupleSizeLeft,
    const uint64_t tupleSizeRight,
    std::shared_ptr<SpillManager> spillManager)
    : Slice(sliceStart, sliceEnd)
    , spillManager(std::move(spillManager))
    , spillEnabled(this->spillManager != nullptr && this->spillManager->configuration().enabled)
{
    /// When spilling is enabled, create the slice's arena up front and allocate ALL the PagedVector main buffers (and,
    /// via the build operator routing getSpillBufferProvider() into pushBack, all pages) from the per-slice
    /// arena-backed BufferManager. This makes the whole slice (both sides, all workers) a single spill unit.
    if (spillEnabled)
    {
        const auto& config = this->spillManager->configuration();
        const auto backingFile = config.spillDirectory + "/nes-nljslice-" + std::to_string(nljSpillSliceCounter.fetch_add(1)) + ".spill";
        spillArena = std::make_shared<ArenaMemoryResource>(config.arenaMode, backingFile);
        spillBufferManager = BufferManager::create(NLJ_SLICE_POOL_PAGE_SIZE, NLJ_SLICE_POOL_BUFFERS, spillArena);
    }
    AbstractBufferProvider& effectiveProvider = spillEnabled ? *spillBufferManager : bufferProvider;

    const uint64_t pvMainBufferSize = PagedVector::getMainBufferSize();
    const uint64_t pvPageBufferSize = effectiveProvider.getBufferSize();
    for (uint64_t i = 0; i < numberOfWorkerThreads; ++i)
    {
        if (auto pagedVectorBuffer = effectiveProvider.getUnpooledBuffer(pvMainBufferSize))
        {
            /// initialize the paged vector tuple buffer
            PagedVector::init(pagedVectorBuffer.value(), pvPageBufferSize, tupleSizeLeft);
            leftPagedVectorBuffers.emplace_back(pagedVectorBuffer.value());
        }
        else
        {
            throw BufferAllocationFailure("No unpooled TupleBuffer available for NLJ left paged vector main buffer");
        }
    }

    for (uint64_t i = 0; i < numberOfWorkerThreads; ++i)
    {
        if (auto pagedVectorBuffer = effectiveProvider.getUnpooledBuffer(pvMainBufferSize))
        {
            /// initialize the paged vector tuple buffer
            PagedVector::init(pagedVectorBuffer.value(), pvPageBufferSize, tupleSizeRight);
            rightPagedVectorBuffers.emplace_back(pagedVectorBuffer.value());
        }
        else
        {
            throw BufferAllocationFailure("No unpooled TupleBuffer available for NLJ right paged vector main buffer");
        }
    }
}

NLJSlice::~NLJSlice()
{
    if (spillManager != nullptr)
    {
        spillManager->unregisterState(this);
    }
    /// Releasing the PagedVectors' TupleBuffers touches buffer control blocks that live in the arena; reload first so
    /// that memory is valid rather than discarded (zero-fill) pages.
    if (spillEvicted.load() && spillArena != nullptr)
    {
        spillArena->reload();
        spillEvicted.store(false);
    }
}

size_t NLJSlice::residentBytes() const
{
    if (spillEvicted.load() || spillArena == nullptr)
    {
        return 0;
    }
    return spillArena->liveBytes();
}

bool NLJSlice::isEvicted() const
{
    return spillEvicted.load();
}

void NLJSlice::evictState()
{
    if (spillArena != nullptr && !spillEvicted.load())
    {
        spillArena->evict();
        spillEvicted.store(true);
    }
}

void NLJSlice::reloadState()
{
    if (spillArena != nullptr && spillEvicted.load())
    {
        spillArena->reload();
        spillEvicted.store(false);
    }
}

uint64_t NLJSlice::coldnessKey() const
{
    return sliceEnd.getRawValue();
}

uint64_t NLJSlice::getNumberOfTuplesLeft() const
{
    return std::accumulate(
        leftPagedVectorBuffers.begin(),
        leftPagedVectorBuffers.end(),
        0,
        [](uint64_t sum, const TupleBuffer& buf)
        {
            auto pagedVector = PagedVector::load(buf);
            return sum + pagedVector.getTotalNumberOfRecords();
        });
}

uint64_t NLJSlice::getNumberOfTuplesRight() const
{
    return std::accumulate(
        rightPagedVectorBuffers.begin(),
        rightPagedVectorBuffers.end(),
        0,
        [](uint64_t sum, const TupleBuffer& buf)
        {
            auto pagedVector = PagedVector::load(buf);
            return sum + pagedVector.getTotalNumberOfRecords();
        });
}

const TupleBuffer* NLJSlice::getPagedVectorRefLeft(const WorkerThreadId workerThreadId) const
{
    const auto pos = workerThreadId % leftPagedVectorBuffers.size();
    return &leftPagedVectorBuffers[pos];
}

const TupleBuffer* NLJSlice::getPagedVectorRefRight(const WorkerThreadId workerThreadId) const
{
    const auto pos = workerThreadId % rightPagedVectorBuffers.size();
    return &rightPagedVectorBuffers[pos];
}

const TupleBuffer* NLJSlice::getPagedVectorTupleBufferRef(const WorkerThreadId workerThreadId, const JoinBuildSideType joinBuildSide) const
{
    switch (joinBuildSide)
    {
        case JoinBuildSideType::Right:
            return getPagedVectorRefRight(workerThreadId);
        case JoinBuildSideType::Left:
            return getPagedVectorRefLeft(workerThreadId);
    }
    std::unreachable();
}

void NLJSlice::combinePagedVectors()
{
    /// Due to the out-of-order nature of our execution engine, it might happen that we call this code here from multiple worker threads.
    /// For example, if different worker threads are emitting the same slice for different windows.
    /// To ensure correctness, we use a lock here
    const std::scoped_lock lock(combinePagedVectorsMutex);

    /// Append all PagedVectors on the left join side and erase all items except for the first one
    /// We do this to ensure that we have only one PagedVector for each side during the probing phase
    if (leftPagedVectorBuffers.size() > 1)
    {
        for (uint64_t i = 1; i < leftPagedVectorBuffers.size(); ++i)
        {
            auto firstLeftPagedVector = PagedVector::load(leftPagedVectorBuffers[0]);
            auto currLeftPagedVector = PagedVector::load(leftPagedVectorBuffers[i]);
            firstLeftPagedVector.movePagesFrom(currLeftPagedVector);
        }
        leftPagedVectorBuffers.erase(leftPagedVectorBuffers.begin() + 1, leftPagedVectorBuffers.end());
    }

    /// Append all PagedVectors on the right join side and remove all items except for the first one
    if (rightPagedVectorBuffers.size() > 1)
    {
        for (uint64_t i = 1; i < rightPagedVectorBuffers.size(); ++i)
        {
            auto firstRightPagedVector = PagedVector::load(rightPagedVectorBuffers[0]);
            auto currRightPagedVector = PagedVector::load(rightPagedVectorBuffers[i]);
            firstRightPagedVector.movePagesFrom(currRightPagedVector);
        }
        rightPagedVectorBuffers.erase(rightPagedVectorBuffers.begin() + 1, rightPagedVectorBuffers.end());
    }
}

AbstractBufferProvider* NLJSlice::getSpillBufferProvider() const
{
    return spillEnabled ? spillBufferManager.get() : nullptr;
}
}
