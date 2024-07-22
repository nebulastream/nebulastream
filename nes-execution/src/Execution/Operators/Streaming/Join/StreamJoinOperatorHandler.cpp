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

#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES::Runtime::Execution::Operators
{
void StreamJoinOperatorHandler::start(PipelineExecutionContextPtr pipelineCtx, uint32_t)
{
    NES_INFO("Started StreamJoinOperatorHandler!");
    setNumberOfWorkerThreads(pipelineCtx->getNumberOfWorkerThreads());
    setBufferManager(pipelineCtx->getBufferManager());
}

void StreamJoinOperatorHandler::stop(QueryTerminationType queryTerminationType, PipelineExecutionContextPtr pipelineCtx)
{
    NES_INFO("Stopped StreamJoinOperatorHandler with {}!", magic_enum::enum_name(queryTerminationType));
    if (queryTerminationType == QueryTerminationType::Graceful)
    {
        triggerAllSlices(pipelineCtx.get());
    }
}

std::vector<Runtime::TupleBuffer> StreamJoinOperatorHandler::getStateToMigrate(uint64_t startTS, uint64_t stopTS)
{
    auto slicesLocked = slices.rlock();

    std::list<StreamSlicePtr> filteredSlices;
    /// filtering slices, which start is in [startTS, stopTS) or end is in (startTS, stopTS]
    /// (records are in range [start, end) in slice)
    std::copy_if(
        slicesLocked->begin(),
        slicesLocked->end(),
        std::back_inserter(filteredSlices),
        [&startTS, &stopTS](const StreamSlicePtr& slice)
        {
            uint64_t sliceStartTS = slice->getSliceStart();
            uint64_t sliceEndTS = slice->getSliceEnd();
            return (sliceStartTS >= startTS && sliceStartTS < stopTS) || (sliceEndTS > startTS && sliceEndTS < stopTS);
        });

    auto buffersToTransfer = std::vector<Runtime::TupleBuffer>();

    /// metadata buffer
    auto mainMetadata = bufferManager->getBufferBlocking();
    buffersToTransfer.insert(buffersToTransfer.begin(), mainMetadata);
    auto metadataBuffersCount = 1;

    /// check that tuple buffer size is more than uint64_t to write number of metadata buffers
    if (!mainMetadata.hasSpaceLeft(0, sizeof(uint64_t)))
    {
        NES_THROW_RUNTIME_ERROR("Buffer is too small");
    }

    /// metadata pointer
    auto metadataPtr = mainMetadata.getBuffer<uint64_t>();
    auto metadataIdx = 1ULL;

    /** @brief Lambda to write to metadata buffers
     * captured variables:
     * mainMetadata - generic metadata buffer, used for checking space left
     * metadataPtr - pointer to the start of current metadata buffer
     * metadataIdx - index of size64_t data inside metadata buffer
     * metadataBuffersCount - number of metadata buffers
     * buffers - vector of buffers
     * @param dataToWrite - value to write to the buffer
    */
    auto writeToMetadata
        = [&mainMetadata, &metadataPtr, &metadataIdx, this, &metadataBuffersCount, &buffersToTransfer](uint64_t dataToWrite)
    {
        /// check that current metadata buffer has enough space, by sending used space and space needed
        if (!mainMetadata.hasSpaceLeft(metadataIdx * sizeof(uint64_t), sizeof(uint64_t)))
        {
            /// if current buffer does not contain enough space then
            /// get new buffer and insert to vector of buffers
            auto newBuffer = bufferManager->getBufferBlocking();
            buffersToTransfer.emplace(buffersToTransfer.begin() + metadataBuffersCount++, newBuffer);
            /// update pointer and index
            metadataPtr = newBuffer.getBuffer<uint64_t>();
            metadataIdx = 0;
        }
        metadataPtr[metadataIdx++] = dataToWrite;
    };

    /// NOTE: Do not change the order of writes to metadata (order is documented in function declaration)
    /// 1. Insert number of slices to metadata buffer
    writeToMetadata(filteredSlices.size());

    for (const auto& slice : filteredSlices)
    {
        /// get buffers with records and store
        auto sliceBuffers = slice->serialize(bufferManager);
        buffersToTransfer.insert(buffersToTransfer.end(), sliceBuffers.begin(), sliceBuffers.end());

        /// 2. Insert number of buffers in i-th slice to metadata buffer
        writeToMetadata(sliceBuffers.size());
    }

    /// set number of metadata buffers to the first metadata buffer
    mainMetadata.getBuffer<uint64_t>()[0] = metadataBuffersCount;

    return buffersToTransfer;
}

void StreamJoinOperatorHandler::restoreState(std::vector<Runtime::TupleBuffer>& buffers)
{
    /// get main metadata buffer
    auto metadataBuffersIdx = 0;
    auto metadataPtr = buffers[metadataBuffersIdx++].getBuffer<uint64_t>();
    auto metadataIdx = 0;

    /// read number of metadata buffers
    auto numberOfMetadataBuffers = metadataPtr[metadataIdx++];

    /** @brief Lambda to read from metadata buffers
     * captured variables:
     * metadataPtr - pointer to the start of current metadata buffer
     * metadataIdx - index of size64_t data inside metadata buffer
     * metadataBuffersIdx - index of current buffer to read
     * buffers - vector of buffers
    */
    auto readFromMetadata = [&metadataPtr, &metadataIdx, &metadataBuffersIdx, &buffers]() -> uint64_t
    {
        /// check left space in metadata buffer
        if (!buffers[metadataBuffersIdx].hasSpaceLeft(metadataIdx * sizeof(uint64_t), sizeof(uint64_t)))
        {
            /// update metadata pointer and index
            metadataPtr = buffers[metadataBuffersIdx++].getBuffer<uint64_t>();
            metadataIdx = 0;
        }
        return metadataPtr[metadataIdx++];
    };
    /// NOTE: Do not change the order of reads from metadata (order is documented in function declaration)
    /// 1. Retrieve number of slices from metadata buffer
    auto numberOfSlices = readFromMetadata();

    auto buffIdx = 0UL;
    auto slicesLocked = this->slices.wlock();

    /// recreate slices from buffers
    for (auto sliceIdx = 0UL; sliceIdx < numberOfSlices; ++sliceIdx)
    {
        /// 2. Retrieve number of buffers in i-th slice
        auto numberOfBuffers = readFromMetadata();

        const auto spanStart = buffers.data() + numberOfMetadataBuffers + buffIdx;
        auto recreatedSlice = deserializeSlice(std::span<const Runtime::TupleBuffer>(spanStart, numberOfBuffers));

        /// insert recreated slice
        auto indexToInsert = std::find_if(
            slicesLocked->begin(),
            slicesLocked->end(),
            [&recreatedSlice](const std::shared_ptr<StreamSlice>& currSlice)
            { return recreatedSlice->getSliceStart() > currSlice->getSliceEnd(); });
        slicesLocked->emplace(indexToInsert, recreatedSlice);
        buffIdx += numberOfBuffers;
    }
}

std::optional<StreamSlicePtr> StreamJoinOperatorHandler::getSliceBySliceIdentifier(uint64_t sliceIdentifier)
{
    auto slicesLocked = slices.rlock();
    return getSliceBySliceIdentifier(slicesLocked, sliceIdentifier);
}

std::optional<StreamSlicePtr>
StreamJoinOperatorHandler::getSliceBySliceIdentifier(const RLockedSlices& slicesLocked, uint64_t sliceIdentifier)
{
    {
        for (auto& curSlice : *slicesLocked)
        {
            if (curSlice->getSliceIdentifier() == sliceIdentifier)
            {
                return curSlice;
            }
        }
    }
    return std::nullopt;
}

std::optional<StreamSlicePtr>
StreamJoinOperatorHandler::getSliceBySliceIdentifier(const WLockedSlices& slicesLocked, uint64_t sliceIdentifier)
{
    {
        for (auto& curSlice : *slicesLocked)
        {
            if (curSlice->getSliceIdentifier() == sliceIdentifier)
            {
                return curSlice;
            }
        }
    }
    return std::nullopt;
}

void StreamJoinOperatorHandler::triggerAllSlices(PipelineExecutionContext* pipelineCtx)
{
    {
        auto [slicesLocked, windowToSlicesLocked] = folly::acquireLocked(slices, windowToSlices);
        for (auto& [windowInfo, slicesAndStateForWindow] : *windowToSlicesLocked)
        {
            switch (slicesAndStateForWindow.windowState)
            {
                case WindowInfoState::BOTH_SIDES_FILLING:
                    slicesAndStateForWindow.windowState = WindowInfoState::ONCE_SEEN_DURING_TERMINATION;
                case WindowInfoState::EMITTED_TO_PROBE:
                    continue;
                case WindowInfoState::ONCE_SEEN_DURING_TERMINATION: {
                    slicesAndStateForWindow.windowState = WindowInfoState::EMITTED_TO_PROBE;

                    /// Performing a cross product of all slices to make sure that each slice gets probe with each other slice
                    /// For bucketing, this should be only done once
                    for (auto& sliceLeft : slicesAndStateForWindow.slices)
                    {
                        for (auto& sliceRight : slicesAndStateForWindow.slices)
                        {
                            emitSliceIdsToProbe(*sliceLeft, *sliceRight, windowInfo, pipelineCtx);
                        }
                    }
                }
            }
        }
    }
}

void StreamJoinOperatorHandler::deleteAllSlices()
{
    {
        auto [slicesLocked, windowToSlicesLocked] = folly::acquireLocked(slices, windowToSlices);
        slicesLocked->clear();
        windowToSlicesLocked->clear();
    }
}

void StreamJoinOperatorHandler::checkAndTriggerWindows(const BufferMetaData& bufferMetaData, PipelineExecutionContext* pipelineCtx)
{
    /// The watermark processor handles the minimal watermark across both streams
    uint64_t newGlobalWatermark
        = watermarkProcessorBuild->updateWatermark(bufferMetaData.watermarkTs, bufferMetaData.seqNumber, bufferMetaData.originId);
    NES_DEBUG("newGlobalWatermark {} bufferMetaData {} ", newGlobalWatermark, bufferMetaData.toString());

    {
        auto [slicesLocked, windowToSlicesLocked] = folly::acquireLocked(slices, windowToSlices);
        for (auto& [windowInfo, slicesAndStateForWindow] : *windowToSlicesLocked)
        {
            if (windowInfo.windowEnd > newGlobalWatermark || slicesAndStateForWindow.windowState == WindowInfoState::EMITTED_TO_PROBE)
            {
                /// This window can not be triggered yet or has already been triggered
                continue;
            }
            slicesAndStateForWindow.windowState = WindowInfoState::EMITTED_TO_PROBE;
            NES_INFO("Emitting all slices for window {}", windowInfo.toString());

            /// Performing a cross product of all slices to make sure that each slice gets probe with each other slice
            /// For bucketing, this should be only done once
            for (auto& sliceLeft : slicesAndStateForWindow.slices)
            {
                for (auto& sliceRight : slicesAndStateForWindow.slices)
                {
                    emitSliceIdsToProbe(*sliceLeft, *sliceRight, windowInfo, pipelineCtx);
                }
            }
        }
    }
}

void StreamJoinOperatorHandler::deleteSlices(const BufferMetaData& bufferMetaData)
{
    uint64_t newGlobalWaterMarkProbe
        = watermarkProcessorProbe->updateWatermark(bufferMetaData.watermarkTs, bufferMetaData.seqNumber, bufferMetaData.originId);
    NES_DEBUG("newGlobalWaterMarkProbe {} bufferMetaData {}", newGlobalWaterMarkProbe, bufferMetaData.toString());

    auto slicesLocked = slices.wlock();
    for (auto it = slicesLocked->begin(); it != slicesLocked->end(); ++it)
    {
        auto& curSlice = *it;
        if (curSlice->getSliceStart() + windowSize < newGlobalWaterMarkProbe)
        {
            /// We can delete this slice/window
            NES_DEBUG(
                "Deleting slice: {} as sliceStart+windowSize {} is smaller then watermark {}",
                curSlice->toString(),
                curSlice->getSliceStart() + windowSize,
                newGlobalWaterMarkProbe);
            it = slicesLocked->erase(it);
        }
    }
}

uint64_t StreamJoinOperatorHandler::getNumberOfSlices()
{
    return slices.rlock()->size();
}

uint64_t StreamJoinOperatorHandler::getNumberOfTuplesInSlice(uint64_t sliceIdentifier, QueryCompilation::JoinBuildSideType buildSide)
{
    auto slice = getSliceBySliceIdentifier(sliceIdentifier);
    if (slice.has_value())
    {
        auto& sliceVal = slice.value();
        switch (buildSide)
        {
            case QueryCompilation::JoinBuildSideType::Left:
                return sliceVal->getNumberOfTuplesLeft();
            case QueryCompilation::JoinBuildSideType::Right:
                return sliceVal->getNumberOfTuplesRight();
        }
    }
    return -1;
}

OriginId StreamJoinOperatorHandler::getOutputOriginId() const
{
    return outputOriginId;
}

uint64_t StreamJoinOperatorHandler::getNextSequenceNumber()
{
    return sequenceNumber++;
}

void StreamJoinOperatorHandler::setNumberOfWorkerThreads(uint64_t numberOfWorkerThreads)
{
    if (StreamJoinOperatorHandler::alreadySetup)
    {
        NES_DEBUG("StreamJoinOperatorHandler::setup was called already!");
        return;
    }
    StreamJoinOperatorHandler::alreadySetup = true;

    NES_DEBUG("StreamJoinOperatorHandler::setup was called!");
    StreamJoinOperatorHandler::numberOfWorkerThreads = numberOfWorkerThreads;
}

void StreamJoinOperatorHandler::updateWatermarkForWorker(uint64_t watermark, WorkerThreadId workerThreadId)
{
    workerThreadIdToWatermarkMap[workerThreadId] = watermark;
}

uint64_t StreamJoinOperatorHandler::getMinWatermarkForWorker()
{
    auto minVal = std::min_element(
        std::begin(workerThreadIdToWatermarkMap),
        std::end(workerThreadIdToWatermarkMap),
        [](const auto& l, const auto& r) { return l.second < r.second; });
    return minVal == workerThreadIdToWatermarkMap.end() ? -1 : minVal->second;
}

uint64_t StreamJoinOperatorHandler::getWindowSlide() const
{
    return sliceAssigner.getWindowSlide();
}

uint64_t StreamJoinOperatorHandler::getWindowSize() const
{
    return sliceAssigner.getWindowSize();
}

void StreamJoinOperatorHandler::setBufferManager(const NES::std::shared_ptr<Runtime::AbstractBufferProvider>& bufManager)
{
    this->bufferManager = bufManager;
}

StreamJoinOperatorHandler::StreamJoinOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    const OriginId outputOriginId,
    const uint64_t windowSize,
    const uint64_t windowSlide,
    const SchemaPtr& leftSchema,
    const SchemaPtr& rightSchema)
    : numberOfWorkerThreads(1)
    , sliceAssigner(windowSize, windowSlide)
    , windowSize(windowSize)
    , windowSlide(windowSlide)
    , watermarkProcessorBuild(std::make_unique<MultiOriginWatermarkProcessor>(inputOrigins))
    , watermarkProcessorProbe(std::make_unique<MultiOriginWatermarkProcessor>(std::vector<OriginId>(1, outputOriginId)))
    , outputOriginId(outputOriginId)
    , sequenceNumber(1)
    , sizeOfRecordLeft(leftSchema->getSchemaSizeInBytes())
    , sizeOfRecordRight(rightSchema->getSchemaSizeInBytes())
    , leftSchema(leftSchema)
    , rightSchema(rightSchema)
{
}

} /// namespace NES::Runtime::Execution::Operators
