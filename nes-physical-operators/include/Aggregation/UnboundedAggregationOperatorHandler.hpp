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

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/SlicedWindowStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <folly/Synchronized.h>

namespace NES
{

/// Handler of an aggregation with an UNBOUNDED window. Its slices only batch the input: each triggered slice is merged into a delta,
/// and the deltas are applied to a table that holds the state of every key seen so far, emitting a retract changelog of the keys.
///
/// The probe tasks of consecutive slices run concurrently, but the deltas must be applied in slice order: otherwise an UPDATE_BEFORE
/// could withdraw a value that was never emitted. Every buffer the handler emits to the probe, a triggered slice or a watermark,
/// carries the next sequence number, so the probe sees every sequence number exactly once. A probe parks the delta of its slice
/// under its sequence number and marks the number as arrived. Whichever probe becomes the applier applies the arrived numbers in
/// order until the next one has not arrived yet; parking, arriving and claiming share one lock, so no delta is left behind.
class UnboundedAggregationOperatorHandler : public AggregationOperatorHandler
{
public:
    /// What claimNext found for the next sequence number.
    enum class Claimed : uint8_t
    {
        /// The next sequence number has not arrived yet, and the caller is no longer the applier.
        NOTHING = 0,
        /// The next sequence number carried a watermark only.
        NO_DELTA = 1,
        /// The next sequence number carried a slice, whose delta claimedDelta returns.
        DELTA = 2,
    };

    UnboundedAggregationOperatorHandler(
        SourcesOfInputOrigins sourcesOfInputOrigins, OriginId outputOriginId, std::unique_ptr<SlicedWindowStoreInterface> sliceAndWindowStore);

    /// Keeps the merged delta of the slice that was emitted with the given sequence number until it is applied.
    void parkDelta(SequenceNumber sequenceNumber, const TupleBuffer& delta, Timestamp sliceEnd);

    /// Marks the sequence number as arrived at the probe, with the watermark of the buffer that carried it, and returns true if the
    /// caller became the applier.
    bool arriveAndTryToApply(SequenceNumber sequenceNumber, Timestamp watermark);

    /// Only for the applier: advances to the next sequence number if it has arrived. Returns NOTHING and ends the role of the
    /// applier otherwise.
    Claimed claimNext();

    /// Only for the applier: the delta of the last DELTA that claimNext returned, and the end of its slice.
    [[nodiscard]] const TupleBuffer& claimedDelta() const;
    [[nodiscard]] Timestamp claimedSliceEnd() const;
    /// Only for the applier: the watermark of the buffer that carried the sequence number claimNext advanced to last.
    [[nodiscard]] Timestamp claimedWatermark() const;

    /// Only for the applier: the table, a chained hash map that holds the aggregation states of all keys seen so far. The first
    /// applier creates it.
    [[nodiscard]] bool hasTable() const;
    void setTable(const TupleBuffer& newTable);
    [[nodiscard]] const TupleBuffer& getTable() const;

protected:
    struct ParkedDelta
    {
        TupleBuffer delta;
        Timestamp sliceEnd;
    };

    struct ApplyState
    {
        std::map<SequenceNumber, ParkedDelta> parkedDeltas;
        /// Arrived sequence numbers and the watermarks of their buffers.
        std::map<SequenceNumber, Timestamp> arrived;
        SequenceNumber nextToApply{SequenceNumber::INITIAL};
        bool applierActive = false;
    };

    folly::Synchronized<ApplyState> applyState;
    /// Owned by the applier, which is a single thread at a time.
    std::optional<ParkedDelta> claimed;
    Timestamp claimedBufferWatermark{Timestamp::INITIAL_VALUE};
    std::optional<TupleBuffer> table;
};

}
