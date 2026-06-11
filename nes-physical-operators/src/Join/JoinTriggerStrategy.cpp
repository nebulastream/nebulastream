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

#include <Join/JoinTriggerStrategy.hpp>

#include <memory>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Sequencing/SequenceData.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>

namespace NES
{

void InnerJoinTriggerStrategy::triggerWindow(
    const std::vector<std::shared_ptr<Slice>>& allSlices,
    const WindowInfoAndSequenceNumber& windowInfo,
    const EmitSlicesFn& emitFn,
    PipelineExecutionContext* pipelineCtx)
{
    const auto totalChunks = allSlices.size() * allSlices.size();
    ChunkNumber::Underlying chunkNumber = ChunkNumber::INITIAL;

    for (const auto& sliceLeft : allSlices)
    {
        for (const auto& sliceRight : allSlices)
        {
            const bool isLastChunk = chunkNumber == totalChunks;
            const SequenceData sequenceData{windowInfo.sequenceNumber, ChunkNumber(chunkNumber), isLastChunk};
            emitFn({sliceLeft}, {sliceRight}, ProbeTaskType::MATCH_PAIRS, windowInfo.windowInfo, sequenceData, pipelineCtx);
            ++chunkNumber;
        }
    }
}

template <bool EmitLeftNullFill, bool EmitRightNullFill>
void OuterJoinTriggerStrategy<EmitLeftNullFill, EmitRightNullFill>::triggerWindow(
    const std::vector<std::shared_ptr<Slice>>& allSlices,
    const WindowInfoAndSequenceNumber& windowInfo,
    const EmitSlicesFn& emitFn,
    PipelineExecutionContext* pipelineCtx)
{
    const auto numSlices = allSlices.size();
    auto totalChunks = numSlices * numSlices;
    if constexpr (EmitLeftNullFill)
    {
        totalChunks += numSlices;
    }
    if constexpr (EmitRightNullFill)
    {
        totalChunks += numSlices;
    }

    ChunkNumber::Underlying chunkNumber = ChunkNumber::INITIAL;

    /// 1) NxN MATCH_PAIRS — same as inner join
    for (const auto& sliceLeft : allSlices)
    {
        for (const auto& sliceRight : allSlices)
        {
            const bool isLastChunk = chunkNumber == totalChunks;
            const SequenceData sequenceData{windowInfo.sequenceNumber, ChunkNumber(chunkNumber), isLastChunk};
            emitFn({sliceLeft}, {sliceRight}, ProbeTaskType::MATCH_PAIRS, windowInfo.windowInfo, sequenceData, pipelineCtx);
            ++chunkNumber;
        }
    }

    /// 2) LEFT_NULL_FILL: each left slice vs ALL right slices
    if constexpr (EmitLeftNullFill)
    {
        for (const auto& slice : allSlices)
        {
            const bool isLastChunk = chunkNumber == totalChunks;
            const SequenceData sequenceData{windowInfo.sequenceNumber, ChunkNumber(chunkNumber), isLastChunk};
            emitFn({slice}, allSlices, ProbeTaskType::LEFT_NULL_FILL, windowInfo.windowInfo, sequenceData, pipelineCtx);
            ++chunkNumber;
        }
    }

    /// 3) RIGHT_NULL_FILL: ALL left slices vs each right slice
    if constexpr (EmitRightNullFill)
    {
        for (const auto& slice : allSlices)
        {
            const bool isLastChunk = chunkNumber == totalChunks;
            const SequenceData sequenceData{windowInfo.sequenceNumber, ChunkNumber(chunkNumber), isLastChunk};
            emitFn(allSlices, {slice}, ProbeTaskType::RIGHT_NULL_FILL, windowInfo.windowInfo, sequenceData, pipelineCtx);
            ++chunkNumber;
        }
    }
}

/// Explicit instantiations for all used combinations
template struct OuterJoinTriggerStrategy<true, false>;
template struct OuterJoinTriggerStrategy<false, true>;
template struct OuterJoinTriggerStrategy<true, true>;

}
