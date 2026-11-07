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
#include <algorithm>
#include <bit>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Join/StreamJoinOperatorHandler.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/SequenceData.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SlicedWindowStoreInterface.hpp>
#include <HashMapSlice.hpp>

namespace NES
{

/// This task models the information for a hash join based window trigger
struct EmittedHJWindowTrigger
{
    EmittedHJWindowTrigger(
        const WindowInfo windowInfo,
        const uint64_t leftNumberOfHashMaps,
        const uint64_t rightNumberOfHashMaps,
        ProbeTaskType probeTaskType,
        const uint64_t segmentId)
        : windowInfo(windowInfo)
        , leftNumberOfHashMaps(leftNumberOfHashMaps)
        , rightNumberOfHashMaps(rightNumberOfHashMaps)
        , probeTaskType(probeTaskType)
        , segmentId(segmentId)
    {
    }

    WindowInfo windowInfo;
    uint64_t leftNumberOfHashMaps;
    uint64_t rightNumberOfHashMaps;
    ProbeTaskType probeTaskType;
    /// The unbounded-join segment a COMPACT task merges; unused by the other task types
    uint64_t segmentId;
};

/// Join state of an unbounded window that covers a contiguous run of triggered slices. All its hash maps are immutable, so probe
/// tasks may read them while a COMPACT task merges them into one map per side.
struct UnboundedJoinSegment
{
    uint64_t id;
    uint64_t numberOfSlices;
    std::vector<TupleBuffer> leftHashMaps;
    std::vector<TupleBuffer> rightHashMaps;
};

class HJOperatorHandler final : public StreamJoinOperatorHandler
{
public:
    HJOperatorHandler(
        SourcesOfInputOrigins sourcesOfInputOrigins,
        OriginId outputOriginId,
        std::unique_ptr<SlicedWindowStoreInterface> sliceAndWindowStore,
        JoinTriggerStrategy triggerStrategy,
        bool unboundedWindow = false);

    /// Replaces the hash maps of a still existing segment with the result of its COMPACT task. A segment that has been merged into
    /// a larger one since the task was emitted no longer exists; the larger segment's own COMPACT task covers its maps.
    void publishCompactedSegment(uint64_t segmentId, std::optional<TupleBuffer> leftHashMap, std::optional<TupleBuffer> rightHashMap);

    [[nodiscard]] std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
    getCreateNewSlicesFunction(const CreateNewSlicesArguments& newSlicesArguments) const override;

    bool wasSetupCalled(const JoinBuildSideType& buildSide);

private:
    /// Is required to not perform the setup again and resolving a race condition to the cleanup state function
    std::atomic<bool> setupAlreadyCalledLeft;
    std::atomic<bool> setupAlreadyCalledRight;

    /// An unbounded window joins every new slice with all previously triggered slices. The previous slices are kept as segments
    /// forming a binary counter: neighbouring segments that cover the same number of slices are merged, so a probe looks up in
    /// O(log #slices) hash maps per side and every tuple is copied O(log #slices) times by compaction.
    bool unboundedWindow;
    std::mutex unboundedSegmentsMutex;
    std::vector<UnboundedJoinSegment> unboundedSegments;
    uint64_t nextSegmentId = 0;

    void triggerUnboundedSlices(
        const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
        PipelineExecutionContext* pipelineCtx);

    void emitHashMapsToProbe(
        const std::vector<TupleBuffer>& leftHashMapBuffers,
        const std::vector<TupleBuffer>& rightHashMapBuffers,
        ProbeTaskType probeTaskType,
        const WindowInfo& windowInfo,
        const SequenceData& sequenceData,
        uint64_t segmentId,
        PipelineExecutionContext* pipelineCtx) const;

protected:
    void triggerSlices(
        const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
        PipelineExecutionContext* pipelineCtx) override;

    void emitSlicesToProbe(
        const std::vector<std::shared_ptr<Slice>>& leftSlices,
        const std::vector<std::shared_ptr<Slice>>& rightSlices,
        ProbeTaskType probeTaskType,
        const WindowInfo& windowInfo,
        const SequenceData& sequenceData,
        PipelineExecutionContext* pipelineCtx) override;
};

}
