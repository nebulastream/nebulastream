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
#include <memory>
#include <mutex>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <SliceStore/Slice.hpp>

namespace NES
{

/// Single-sided slice for the streaming interval join.
///
/// Unlike NLJSlice (which carries left + right PagedVectors in one object), an
/// interval-join slice lives in exactly one of the per-side stores held by
/// IntervalJoinOperatorHandler. The side is implicit in the store; the slice
/// only needs one PagedVector array.
///
/// combinePagedVectors() is idempotent under an explicit atomic CAS gate. This
/// matters here in a way it does not for NLJ: a single anchor slice can be
/// referenced by multiple partner emissions; making the merge a no-op on
/// repeated calls avoids quadratic corruption on the merged vector.
class IntervalJoinSlice final : public Slice
{
public:
    IntervalJoinSlice(SliceStart sliceStart, SliceEnd sliceEnd, uint64_t numberOfWorkerThreads);

    /// Total tuple count across all per-worker PagedVectors. Safe to call before
    /// or after combinePagedVectors() — sums whichever vectors are still live.
    [[nodiscard]] uint64_t getNumberOfTuples() const;

    /// Returns the per-worker PagedVector for the given worker. Used by the
    /// build operator on the hot path.
    [[nodiscard]] PagedVector* getPagedVectorRef(WorkerThreadId workerThreadId) const;

    /// Returns the merged PagedVector (slot 0). The handler calls
    /// combinePagedVectors() before emit, so the probe operator reads from this.
    [[nodiscard]] PagedVector* getMergedPagedVector() const;

    /// Idempotent merge of per-worker PagedVectors into slot 0. The first call
    /// drains slots 1..N into slot 0 under combineMutex; subsequent calls return
    /// immediately via the CAS gate.
    void combinePagedVectors();

    /// True once combinePagedVectors() has been called.
    [[nodiscard]] bool isCombined() const noexcept;

    /// LEFT-store usage: atomically claim this slice for trigger emission.
    /// Returns true iff this call performed the claim (i.e. the slice was not
    /// previously claimed). Unused on the right store.
    bool claimTriggered() noexcept;

    /// True iff the slice has been claimed for trigger.
    [[nodiscard]] bool isTriggered() const noexcept;

private:
    std::vector<std::unique_ptr<PagedVector>> pagedVectors;
    std::atomic<bool> combined{false};
    std::atomic<bool> triggered{false};
    std::mutex combineMutex;
};

}
