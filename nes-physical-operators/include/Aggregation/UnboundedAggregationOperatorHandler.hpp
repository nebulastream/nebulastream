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
#include <vector>
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
/// The keys are split into partitions by their hash, i.e. by ranges of chains of the hash maps. Each partition has its own table and
/// its own applier, so the partitions of a slice are merged and applied in parallel. The handler emits every buffer of the slice
/// store (a triggered slice or a watermark) once per partition: the store's sequence number n becomes the sequence numbers
/// (n - 1) * P + p + 1 of the P partitions p. The sequence numbers stay unique and dense, so downstream operators only see the output
/// of a slice as complete once all partitions emitted it, and the partition and the position of a buffer follow from its number.
///
/// The probe tasks of consecutive slices run concurrently, but within a partition the deltas must be applied in slice order:
/// otherwise an UPDATE_BEFORE could withdraw a value that was never emitted. The order only matters per key, so different
/// partitions do not wait for each other. A probe parks the delta of its partition under its position and marks the position as
/// arrived. Whichever probe of the partition becomes its applier applies the arrived positions in order until the next one has not
/// arrived yet; parking, arriving and claiming share one lock per partition, so no delta is left behind.
class UnboundedAggregationOperatorHandler : public AggregationOperatorHandler
{
public:
    /// What claimNext found for the next position.
    enum class Claimed : uint8_t
    {
        /// The next position has not arrived yet, and the caller is no longer the applier.
        NOTHING = 0,
        /// The next position carried a watermark only.
        NO_DELTA = 1,
        /// The next position carried a slice, whose delta claimedDelta returns.
        DELTA = 2,
    };

    UnboundedAggregationOperatorHandler(
        SourcesOfInputOrigins sourcesOfInputOrigins,
        OriginId outputOriginId,
        std::unique_ptr<SlicedWindowStoreInterface> sliceAndWindowStore,
        uint64_t numberOfPartitions = 1);

    [[nodiscard]] uint64_t getNumberOfPartitions() const { return partitions.size(); }
    /// The partition of the buffer with the given sequence number.
    [[nodiscard]] uint64_t partitionOf(SequenceNumber sequenceNumber) const;

    /// Keeps the merged delta of the partition and slice that was emitted with the given sequence number until it is applied.
    void parkDelta(SequenceNumber sequenceNumber, const TupleBuffer& delta, Timestamp sliceEnd);

    /// Marks the sequence number as arrived at the probe, with the watermark of the buffer that carried it, and returns true if the
    /// caller became the applier of its partition.
    bool arriveAndTryToApply(SequenceNumber sequenceNumber, Timestamp watermark);

    /// Only for the applier of the partition: advances to its next position if it has arrived. Returns NOTHING and ends the role of
    /// the applier otherwise.
    Claimed claimNext(uint64_t partition = 0);

    /// Only for the applier of the partition: the delta of the last DELTA that claimNext returned, and the end of its slice.
    [[nodiscard]] const TupleBuffer& claimedDelta(uint64_t partition = 0) const;
    [[nodiscard]] Timestamp claimedSliceEnd(uint64_t partition = 0) const;
    /// Only for the applier of the partition: the watermark of the buffer that carried the position claimNext advanced to last.
    [[nodiscard]] Timestamp claimedWatermark(uint64_t partition = 0) const;

    /// Only for the applier of the partition: its table, a chained hash map that holds the aggregation states of all keys of the
    /// partition seen so far. The first applier creates it.
    [[nodiscard]] bool hasTable(uint64_t partition = 0) const;
    void setTable(const TupleBuffer& newTable, uint64_t partition = 0);
    [[nodiscard]] const TupleBuffer& getTable(uint64_t partition = 0) const;

protected:
    void triggerSlices(
        const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
        PipelineExecutionContext* pipelineCtx) override;
    void emitWatermark(Timestamp watermark, SequenceNumber sequenceNumber, PipelineExecutionContext* pipelineCtx) override;

    struct ParkedDelta
    {
        TupleBuffer delta;
        Timestamp sliceEnd;
    };

    /// Positions start at SequenceNumber::INITIAL, like the sequence numbers of the slice store.
    struct ApplyState
    {
        std::map<SequenceNumber, ParkedDelta> parkedDeltas;
        /// Arrived positions and the watermarks of their buffers.
        std::map<SequenceNumber, Timestamp> arrived;
        SequenceNumber nextToApply{SequenceNumber::INITIAL};
        bool applierActive = false;
    };

    struct Partition
    {
        folly::Synchronized<ApplyState> applyState;
        /// Owned by the applier of the partition, which is a single thread at a time.
        std::optional<ParkedDelta> claimed;
        Timestamp claimedBufferWatermark{Timestamp::INITIAL_VALUE};
        std::optional<TupleBuffer> table;
    };

    /// The sequence number of partition p for the sequence number the slice store assigned, and the position of a sequence number
    /// within its partition.
    [[nodiscard]] SequenceNumber partitionSequenceNumber(SequenceNumber storeSequenceNumber, uint64_t partition) const;
    [[nodiscard]] SequenceNumber positionOf(SequenceNumber sequenceNumber) const;

    std::vector<std::unique_ptr<Partition>> partitions;
};

}
