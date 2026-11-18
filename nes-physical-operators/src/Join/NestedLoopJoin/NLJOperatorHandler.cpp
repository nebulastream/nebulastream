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

#include <Join/NestedLoopJoin/NLJOperatorHandler.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <span>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Join/NestedLoopJoin/NLJSlice.hpp>
#include <Join/StreamJoinOperatorHandler.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/SequenceData.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <Watermark/MultiOriginWatermarkProcessor.hpp>
#include <nautilus/inline.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{
NLJOperatorHandler::NLJOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    const OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
    JoinTriggerStrategy triggerStrategy)
    : StreamJoinOperatorHandler(inputOrigins, outputOriginId, std::move(sliceAndWindowStore), std::move(triggerStrategy))
{
}

NAUTILUS_INLINE std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
NLJOperatorHandler::getCreateNewSlicesFunction(const CreateNewSlicesArguments& args) const
{
    PRECONDITION(
        numberOfWorkerThreads > 0, "Number of worker threads not set for window based operator. Was setWorkerThreads() being called?");
    const auto& nljArgs = dynamic_cast<const CreateNewNLJSliceArgs&>(args);
    return std::function(
        [numberOfWorkerThreads = numberOfWorkerThreads,
         bufferProvider = nljArgs.bufferProvider,
         tupleSizeLeft = nljArgs.tupleSizeLeft,
         tupleSizeRight = nljArgs.tupleSizeRight](SliceStart start, SliceEnd end) -> std::vector<std::shared_ptr<Slice>>
        { return {std::make_shared<NLJSlice>(*bufferProvider, start, end, numberOfWorkerThreads, tupleSizeLeft, tupleSizeRight)}; });
}

void NLJOperatorHandler::emitSlicesToProbe(
    const std::vector<std::shared_ptr<Slice>>& leftSlices,
    const std::vector<std::shared_ptr<Slice>>& rightSlices,
    ProbeTaskType probeTaskType,
    const WindowInfo& windowInfo,
    const SequenceData& sequenceData,
    PipelineExecutionContext* pipelineCtx)
{
    /// Combine paged vectors for all slices on both sides
    uint64_t totalNumberOfTuples = 0;
    for (const auto& slice : leftSlices)
    {
        auto& nljSlice = dynamic_cast<NLJSlice&>(*slice);
        nljSlice.combinePagedVectors();
        totalNumberOfTuples += nljSlice.getNumberOfTuplesLeft();
    }
    for (const auto& slice : rightSlices)
    {
        auto& nljSlice = dynamic_cast<NLJSlice&>(*slice);
        nljSlice.combinePagedVectors();
        totalNumberOfTuples += nljSlice.getNumberOfTuplesRight();
    }

    /// Collect slice ends
    std::vector<SliceEnd> leftSliceEnds;
    leftSliceEnds.reserve(leftSlices.size());
    for (const auto& slice : leftSlices)
    {
        leftSliceEnds.emplace_back(slice->getSliceEnd());
    }
    std::vector<SliceEnd> rightSliceEnds;
    rightSliceEnds.reserve(rightSlices.size());
    for (const auto& slice : rightSlices)
    {
        rightSliceEnds.emplace_back(slice->getSliceEnd());
    }

    /// Allocate variable-sized trigger buffer
    const auto neededBufferSize = sizeof(EmittedNLJWindowTrigger) + ((leftSliceEnds.size() + rightSliceEnds.size()) * sizeof(SliceEnd));
    const auto tupleBufferVal = pipelineCtx->getBufferManager()->getUnpooledBuffer(neededBufferSize);
    if (not tupleBufferVal.has_value())
    {
        throw CannotAllocateBuffer("{}B for the NLJ window trigger were requested", neededBufferSize);
    }

    auto tupleBuffer = tupleBufferVal.value();
    tupleBuffer.setOriginId(outputOriginId);
    tupleBuffer.setSequenceNumber(SequenceNumber(sequenceData.sequenceNumber));
    tupleBuffer.setChunkNumber(ChunkNumber(sequenceData.chunkNumber));
    tupleBuffer.setLastChunk(sequenceData.lastChunk);
    tupleBuffer.setWatermark(windowInfo.windowStart);
    tupleBuffer.setNumberOfTuples(totalNumberOfTuples);
    tupleBuffer.setCreationTimestampInMS(Timestamp(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count()));
    new (tupleBuffer.getAvailableMemoryArea().data()) EmittedNLJWindowTrigger{windowInfo, leftSliceEnds, rightSliceEnds, probeTaskType};

    pipelineCtx->emitBuffer(tupleBuffer);
}

namespace
{
constexpr uint64_t NLJ_STATE_MAGIC = 0x4e4c4a5354415445; /// "NLJSTATE"

/// Writes 64-bit values into flat buffers, appending a new buffer whenever one is full.
class FlatStateWriter
{
public:
    explicit FlatStateWriter(AbstractBufferProvider& bufferProvider) : bufferProvider(bufferProvider) { }

    void write(const uint64_t value) { writeBytes(std::as_bytes(std::span{&value, 1})); }

    void writeBytes(std::span<const std::byte> bytes)
    {
        while (not bytes.empty())
        {
            if (buffers.empty() or buffers.back().getNumberOfTuples() == buffers.back().getBufferSize())
            {
                buffers.emplace_back(bufferProvider.getBufferBlocking());
                buffers.back().setNumberOfTuples(0);
            }
            auto& buffer = buffers.back();
            const auto used = buffer.getNumberOfTuples();
            const auto chunk = std::min<size_t>(bytes.size(), buffer.getBufferSize() - used);
            std::memcpy(buffer.getAvailableMemoryArea<std::byte>().data() + used, bytes.data(), chunk);
            buffer.setNumberOfTuples(used + chunk);
            bytes = bytes.subspan(chunk);
        }
    }

    /// Writes a buffer with all of its child buffers, depth first.
    void writeTree(const TupleBuffer& buffer)
    {
        write(buffer.getBufferSize());
        write(buffer.getNumberOfTuples());
        write(buffer.getNumberOfChildBuffers());
        writeBytes(buffer.getAvailableMemoryArea<const std::byte>());
        for (uint32_t child = 0; child < buffer.getNumberOfChildBuffers(); ++child)
        {
            writeTree(buffer.loadChildBuffer(ChildBufferIndex(child)));
        }
    }

    std::vector<TupleBuffer> buffers;

private:
    AbstractBufferProvider& bufferProvider; ///NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
};

class FlatStateReader
{
public:
    FlatStateReader(const std::vector<TupleBuffer>& buffers, AbstractBufferProvider& bufferProvider)
        : buffers(buffers), bufferProvider(bufferProvider)
    {
    }

    uint64_t read()
    {
        uint64_t value = 0;
        readBytes(std::as_writable_bytes(std::span{&value, 1}));
        return value;
    }

    void readBytes(std::span<std::byte> bytes)
    {
        while (not bytes.empty())
        {
            if (bufferIndex >= buffers.size())
            {
                throw CannotDeserialize("The migrated join state ends unexpectedly");
            }
            const auto& buffer = buffers.at(bufferIndex);
            const auto available = buffer.getNumberOfTuples() - offset;
            const auto chunk = std::min<size_t>(bytes.size(), available);
            std::memcpy(bytes.data(), buffer.getAvailableMemoryArea<const std::byte>().data() + offset, chunk);
            offset += chunk;
            bytes = bytes.subspan(chunk);
            if (offset == buffer.getNumberOfTuples())
            {
                ++bufferIndex;
                offset = 0;
            }
        }
    }

    TupleBuffer readTree()
    {
        const auto bufferSize = read();
        const auto numberOfTuples = read();
        const auto numberOfChildren = read();
        auto buffer = bufferProvider.getUnpooledBuffer(bufferSize);
        if (not buffer.has_value())
        {
            throw CannotAllocateBuffer("{}B for the migrated join state were requested", bufferSize);
        }
        readBytes(buffer->getAvailableMemoryArea<std::byte>().subspan(0, bufferSize));
        buffer->setNumberOfTuples(numberOfTuples);
        for (uint64_t child = 0; child < numberOfChildren; ++child)
        {
            auto childBuffer = readTree();
            const auto index = buffer->storeChildBuffer(childBuffer);
            INVARIANT(index == ChildBufferIndex(child), "Child buffers of the migrated join state must keep their order");
        }
        return std::move(*buffer);
    }

private:
    const std::vector<TupleBuffer>& buffers; ///NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    AbstractBufferProvider& bufferProvider; ///NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    size_t bufferIndex = 0;
    size_t offset = 0;
};

void writeProcessorState(FlatStateWriter& writer, const std::vector<MultiOriginWatermarkProcessor::OriginState>& state)
{
    writer.write(state.size());
    for (const auto& [origin, sequenceNumber, watermark] : state)
    {
        writer.write(origin.getRawValue());
        writer.write(sequenceNumber);
        writer.write(watermark);
    }
}

std::vector<MultiOriginWatermarkProcessor::OriginState> readProcessorState(FlatStateReader& reader)
{
    std::vector<MultiOriginWatermarkProcessor::OriginState> state(reader.read());
    for (auto& [origin, sequenceNumber, watermark] : state)
    {
        origin = OriginId(reader.read());
        sequenceNumber = reader.read();
        watermark = reader.read();
    }
    return state;
}
}

std::vector<TupleBuffer> NLJOperatorHandler::serializeState(AbstractBufferProvider& bufferProvider) const
{
    FlatStateWriter writer(bufferProvider);
    writer.write(NLJ_STATE_MAGIC);
    writeProcessorState(writer, watermarkProcessorBuild->getState());
    writeProcessorState(writer, watermarkProcessorProbe->getState());

    const auto storeState = sliceAndWindowStore->getMigratedState();
    const auto triggeredUpTo = highestTriggeredWatermark.load();
    writer.write(storeState.nextSequenceNumber);
    writer.write(storeState.lastForwardedWatermark.getRawValue());
    writer.write(triggeredUpTo);

    /// Slices of windows that were already triggered are not migrated, the new join would trigger them again.
    std::vector<std::shared_ptr<NLJSlice>> pendingSlices;
    for (const auto& slice : storeState.slices)
    {
        if (slice->getSliceEnd().getRawValue() > triggeredUpTo)
        {
            pendingSlices.emplace_back(std::dynamic_pointer_cast<NLJSlice>(slice));
        }
    }
    writer.write(pendingSlices.size());
    for (const auto& slice : pendingSlices)
    {
        slice->combinePagedVectors();
        writer.write(slice->getSliceStart().getRawValue());
        writer.write(slice->getSliceEnd().getRawValue());
        for (const auto side : {JoinBuildSideType::Left, JoinBuildSideType::Right})
        {
            const auto& pagedVector = slice->getCombinedPagedVector(side);
            writer.write(PagedVector::load(pagedVector).getTupleSize());
            writer.writeTree(pagedVector);
        }
    }
    NES_DEBUG(
        "Serialized the state of the join with output origin {}: {} slices in {} buffers",
        outputOriginId,
        pendingSlices.size(),
        writer.buffers.size());
    return std::move(writer.buffers);
}

void NLJOperatorHandler::restoreState(const std::vector<TupleBuffer>& state, AbstractBufferProvider& bufferProvider)
{
    PRECONDITION(numberOfWorkerThreads > 0, "The join must be started before its state is restored");
    FlatStateReader reader(state, bufferProvider);
    if (reader.read() != NLJ_STATE_MAGIC)
    {
        throw CannotDeserialize("The migrated state is not the state of a nested loop join");
    }
    watermarkProcessorBuild->restoreState(readProcessorState(reader));
    watermarkProcessorProbe->restoreState(readProcessorState(reader));
    const auto nextSequenceNumber = reader.read();
    const auto lastForwardedWatermark = Timestamp(reader.read());
    highestTriggeredWatermark = reader.read();
    sliceAndWindowStore->restoreCounters(nextSequenceNumber, lastForwardedWatermark);

    const auto numberOfSlices = reader.read();
    for (uint64_t index = 0; index < numberOfSlices; ++index)
    {
        const auto sliceStart = SliceStart(reader.read());
        const auto sliceEnd = SliceEnd(reader.read());
        const auto tupleSizeLeft = reader.read();
        auto left = reader.readTree();
        const auto tupleSizeRight = reader.read();
        auto right = reader.readTree();
        auto slice = std::make_shared<NLJSlice>(bufferProvider, sliceStart, sliceEnd, numberOfWorkerThreads, tupleSizeLeft, tupleSizeRight);
        slice->setCombinedPagedVector(JoinBuildSideType::Left, std::move(left));
        slice->setCombinedPagedVector(JoinBuildSideType::Right, std::move(right));
        const auto restored = sliceAndWindowStore->getSlicesOrCreate(
            sliceStart, [&slice](SliceStart, SliceEnd) -> std::vector<std::shared_ptr<Slice>> { return {slice}; });
        INVARIANT(restored.size() == 1 and restored.front() == slice, "A migrated slice must not exist in the new join yet");
    }
    NES_DEBUG("Restored the state of the join with output origin {}: {} slices", outputOriginId, numberOfSlices);
}

}
