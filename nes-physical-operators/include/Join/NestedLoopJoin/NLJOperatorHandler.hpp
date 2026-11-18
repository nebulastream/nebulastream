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
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Join/StreamJoinOperatorHandler.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Reconfiguration/ReconfigurationMarker.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>

namespace NES
{

/// Variable-sized trigger buffer for NLJ. Stores arrays of slice ends for both sides,
/// a ProbeTaskType, and window info. The slice end arrays are packed after the struct.
struct EmittedNLJWindowTrigger
{
    EmittedNLJWindowTrigger(
        const WindowInfo& windowInfo,
        const std::vector<SliceEnd>& leftSliceEnds,
        const std::vector<SliceEnd>& rightSliceEnds,
        ProbeTaskType probeTaskType)
        : windowInfo(windowInfo)
        , leftNumberOfSliceEnds(leftSliceEnds.size())
        , rightNumberOfSliceEnds(rightSliceEnds.size())
        , probeTaskType(probeTaskType)
    {
        auto* base = std::bit_cast<int8_t*>(this + 1);
        this->leftSliceEnds = std::bit_cast<SliceEnd*>(base);
        this->rightSliceEnds = std::bit_cast<SliceEnd*>(base + (leftSliceEnds.size() * sizeof(SliceEnd)));
        std::ranges::copy(leftSliceEnds, this->leftSliceEnds);
        std::ranges::copy(rightSliceEnds, this->rightSliceEnds);
    }

    WindowInfo windowInfo;
    uint64_t leftNumberOfSliceEnds;
    uint64_t rightNumberOfSliceEnds;
    ProbeTaskType probeTaskType;
    SliceEnd* leftSliceEnds;
    SliceEnd* rightSliceEnds;
};

class NLJOperatorHandler final : public StreamJoinOperatorHandler
{
public:
    NLJOperatorHandler(
        const std::vector<OriginId>& inputOrigins,
        OriginId outputOriginId,
        std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
        JoinTriggerStrategy triggerStrategy);

    [[nodiscard]] std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
    getCreateNewSlicesFunction(const CreateNewSlicesArguments&) const override;

    /// Serializes the state that is migrated with the join (ISQD): the progress of the watermark processors, the counters of the slice
    /// store, and the tuples of every slice of a window that has not been triggered yet. The state is a sequence of flat tuple buffers
    /// without child buffers, so it can be sent over network channels; every buffer stores its payload size as its number of tuples.
    /// Shortcut: only tumbling windows are supported, as a slice of a sliding window may belong to triggered and pending windows.
    [[nodiscard]] std::vector<TupleBuffer> serializeState(AbstractBufferProvider& bufferProvider) const;

    /// Restores a migrated state into this handler before it processes any data. `start` must have been called.
    void restoreState(const std::vector<TupleBuffer>& state, AbstractBufferProvider& bufferProvider);

    /// The join is the new instance of a join that moved to this worker: it receives the state of the old instance over the channel.
    void setIncomingStateMigration(StateMigrationChannel channel) { incomingStateMigration = std::move(channel); }

    /// Receives and restores the migrated state, if any. The query engine starts the sources of a query only once all of its pipelines
    /// started, so the join processes no data before the state was restored (like the original, which reads the state file at start).
    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    /// A join that is updated and drained sends its state to its new instance instead of discarding it.
    void stop(QueryTerminationType queryTerminationType, PipelineExecutionContext& pipelineExecutionContext) override;

    /// How long the old and the new instance wait for each other during a state migration.
    static constexpr std::chrono::seconds STATE_MIGRATION_TIMEOUT{120};

private:
    std::optional<StateMigrationChannel> incomingStateMigration;

    void emitSlicesToProbe(
        const std::vector<std::shared_ptr<Slice>>& leftSlices,
        const std::vector<std::shared_ptr<Slice>>& rightSlices,
        ProbeTaskType probeTaskType,
        const WindowInfo& windowInfo,
        const SequenceData& sequenceData,
        PipelineExecutionContext* pipelineCtx) override;
};
}
