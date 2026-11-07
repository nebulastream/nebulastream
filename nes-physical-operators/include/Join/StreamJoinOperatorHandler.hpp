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
#include <mutex>
#include <optional>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Join/JoinTriggerStrategy.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/SequenceData.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SlicedWindowStoreInterface.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

/// Probe task whose join state is passed as child buffers of the task buffer: the left buffers are the children
/// [0, leftNumberOfBuffers), the right ones follow at [leftNumberOfBuffers, leftNumberOfBuffers + rightNumberOfBuffers).
/// What a buffer holds depends on the join: a hash map, a paged vector or a B-tree.
struct EmittedJoinStateTrigger
{
    WindowInfo windowInfo;
    uint64_t leftNumberOfBuffers;
    uint64_t rightNumberOfBuffers;
    ProbeTaskType probeTaskType;
    /// The unbounded-join segment a COMPACT task merges; unused by the other task types
    uint64_t segmentId;
};

/// The join state of an UNBOUNDED window: every triggered slice must be joined with every slice triggered before it, so the
/// state of all past slices is kept. It is kept as segments that each cover a contiguous run of slices and form a binary counter:
/// appending a slice merges neighbouring segments that cover the same number of slices. A segment's buffers are immutable, so
/// probe tasks may read them while a COMPACT task merges them.
///
/// With compaction, the join merges each newly formed segment into one buffer per side, so the state consists of O(log #slices)
/// buffers per side and every tuple is copied O(log #slices) times. Without compaction, a segment just concatenates the buffer
/// lists of the segments it merged.
class UnboundedJoinSegments
{
public:
    explicit UnboundedJoinSegments(bool compactSegments);

    struct Compaction
    {
        uint64_t segmentId;
        /// Either side is empty if it already consists of at most one buffer
        std::vector<TupleBuffer> leftBuffers;
        std::vector<TupleBuffer> rightBuffers;
    };

    struct AppendedSlice
    {
        /// The state of all slices appended before this one
        std::vector<TupleBuffer> oldLeftBuffers;
        std::vector<TupleBuffer> oldRightBuffers;
        std::optional<Compaction> compaction;
    };

    /// Appends the state of a newly triggered slice. Appends are serialized, so each pair of slices is joined exactly once by
    /// whichever of the two is appended last, even if concurrent triggers append slices out of event-time order.
    AppendedSlice append(std::vector<TupleBuffer> newLeftBuffers, std::vector<TupleBuffer> newRightBuffers);

    /// Replaces the buffers of a still existing segment with the result of its COMPACT task. A segment that has been merged into
    /// a larger one since the task was emitted no longer exists; the larger segment's own COMPACT task covers its buffers.
    void publishCompacted(uint64_t segmentId, std::optional<TupleBuffer> leftBuffer, std::optional<TupleBuffer> rightBuffer);

private:
    struct Segment
    {
        uint64_t id;
        uint64_t numberOfSlices;
        std::vector<TupleBuffer> leftBuffers;
        std::vector<TupleBuffer> rightBuffers;
    };

    bool compactSegments;
    std::mutex mutex;
    std::vector<Segment> segments;
    uint64_t nextSegmentId = 0;
};

/// This operator is the general join operator handler. It is expected that all StreamJoinOperatorHandlers inherit from this class.
/// It delegates window triggering to a JoinTriggerStrategy configured at lowering time and delegates the actual probe
/// emission to the specific join implementation via emitSlicesToProbe().
/// For an UNBOUNDED window, it instead joins every triggered slice with all previously triggered slices, see UnboundedJoinSegments.
class StreamJoinOperatorHandler : public WindowBasedOperatorHandler
{
public:
    /// unboundedSegments is set if and only if the join has an UNBOUNDED window
    StreamJoinOperatorHandler(
        SourcesOfInputOrigins sourcesOfInputOrigins,
        OriginId outputOriginId,
        std::unique_ptr<SlicedWindowStoreInterface> sliceAndWindowStore,
        JoinTriggerStrategy triggerStrategy,
        std::unique_ptr<UnboundedJoinSegments> unboundedSegments = nullptr);

    /// Publishes the result of a COMPACT task of an unbounded join
    void publishCompactedSegment(uint64_t segmentId, std::optional<TupleBuffer> leftBuffer, std::optional<TupleBuffer> rightBuffer);

protected:
    /// Delegates to the configured JoinTriggerStrategy for each triggered window, or for an unbounded window to
    /// triggerUnboundedSlices().
    void triggerSlices(
        const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
        PipelineExecutionContext* pipelineCtx) override;

    /// Emits probe buffers for a given set of left and right slices with the specified task type.
    /// Each join implementation (HJ, NLJ, ...) packs its own data format into the probe buffer.
    virtual void emitSlicesToProbe(
        const std::vector<std::shared_ptr<Slice>>& leftSlices,
        const std::vector<std::shared_ptr<Slice>>& rightSlices,
        ProbeTaskType probeTaskType,
        const WindowInfo& windowInfo,
        const SequenceData& sequenceData,
        PipelineExecutionContext* pipelineCtx) = 0;

    /// Returns the join state of one side of the given triggered slices as buffers that stay valid after the slices are deleted,
    /// leaving out empty ones. Only called for an unbounded window, once per slice.
    [[nodiscard]] virtual std::vector<TupleBuffer>
    getJoinStateOfSlices(const std::vector<std::shared_ptr<Slice>>& slices, JoinBuildSideType side) = 0;

    /// Returns the number of tuples in a buffer returned by getJoinStateOfSlices()
    [[nodiscard]] virtual uint64_t getNumberOfTuples(const TupleBuffer& joinState) const = 0;

    /// Emits a probe task that carries the given state buffers as children, see EmittedJoinStateTrigger
    void emitJoinStateToProbe(
        const std::vector<TupleBuffer>& leftBuffers,
        const std::vector<TupleBuffer>& rightBuffers,
        ProbeTaskType probeTaskType,
        const WindowInfo& windowInfo,
        const SequenceData& sequenceData,
        uint64_t segmentId,
        PipelineExecutionContext* pipelineCtx) const;

    [[nodiscard]] bool isUnbounded() const { return unboundedSegments != nullptr; }

    JoinTriggerStrategy triggerStrategy;

private:
    /// For each triggered slice, joins its new left tuples with all right tuples and all older left tuples with its new right
    /// tuples, as two probe tasks that both iterate the small new state and look up in the large old one. A third task compacts
    /// the newly formed segment, if the join compacts its state.
    void triggerUnboundedSlices(
        const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
        PipelineExecutionContext* pipelineCtx);

    std::unique_ptr<UnboundedJoinSegments> unboundedSegments;
};

/// Proxy for the COMPACT task of a probe operator: publishes the merged buffers of a segment to the operator handler
void publishCompactedJoinStateProxy(
    OperatorHandler* operatorHandler, uint64_t segmentId, TupleBuffer* leftBuffer, bool hasLeft, TupleBuffer* rightBuffer, bool hasRight);
}
