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

#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/Slice.hpp>

namespace NES
{

struct CreateNewIntervalJoinSliceArgs final : CreateNewSlicesArguments
{
    CreateNewIntervalJoinSliceArgs(AbstractBufferProvider& bufferProvider, uint64_t tupleSize)
        : bufferProvider(&bufferProvider), tupleSize(tupleSize) /// NOLINT(clang-analyzer-optin.cplusplus.UninitializedObject)
    {
    }

    ~CreateNewIntervalJoinSliceArgs() override = default;

    AbstractBufferProvider* bufferProvider;
    uint64_t tupleSize;
};

/// Single-sided slice for the streaming interval join: lives in one per-side store, so it holds one
/// per-worker PagedVector array (views over TupleBuffers). The side is implicit in the store.
///
/// The per-worker merge must stay idempotent (atomic CAS gate): a single anchor slice can be
/// referenced by multiple partner emissions, and re-merging would corrupt the merged vector.
class IntervalJoinSlice final : public Slice
{
public:
    IntervalJoinSlice(
        AbstractBufferProvider& bufferProvider,
        SliceStart sliceStart,
        SliceEnd sliceEnd,
        uint64_t numberOfWorkerThreads,
        uint64_t tupleSize);

    /// Total tuple count. Safe before or after the per-worker merge (sums whichever vectors are live).
    [[nodiscard]] uint64_t getNumberOfTuples() const;

    /// Per-worker PagedVector TupleBuffer. Used by the build operator on the hot path.
    [[nodiscard]] const TupleBuffer* getPagedVectorRef(WorkerThreadId workerThreadId) const;

    /// Merged PagedVector TupleBuffer (slot 0). Read by the probe after the per-worker merge.
    [[nodiscard]] const TupleBuffer* getMergedPagedVector() const;

    /// Idempotent merge of per-worker PagedVectors into slot 0 (CAS-gated).
    void combinePagedVectors();

    /// Atomically claim this slice for trigger emission (anchor store only). True iff this call claimed it.
    bool claimTriggered() noexcept;

private:
    std::vector<TupleBuffer> pagedVectorBuffers;
    /// combined gates the per-worker merge, triggered claims the slice for emission once.
    /// Independent flags, so not a single WindowInfoState.
    std::atomic<bool> combined{false};
    std::atomic<bool> triggered{false};
    /// Guards pagedVectorBuffers mutation during the per-worker merge. Also held while counting tuples (hence mutable)
    /// so a shared partner slice's tuple count serializes against a concurrent merge.
    mutable std::mutex combineMutex;
};

}
