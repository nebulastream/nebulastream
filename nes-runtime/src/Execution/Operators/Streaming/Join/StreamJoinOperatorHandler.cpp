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

namespace NES::Runtime::Execution::Operators {
void StreamJoinOperatorHandler::start(PipelineExecutionContextPtr, StateManagerPtr, uint32_t) {
    NES_INFO("Started StreamJoinOperatorHandler!");
}
void StreamJoinOperatorHandler::stop(QueryTerminationType , PipelineExecutionContextPtr) {
    NES_INFO("Stopped StreamJoinOperatorHandler!");
}

std::optional<StreamSlicePtr> StreamJoinOperatorHandler::getSliceBySliceIdentifier(uint64_t sliceIdentifier) {
    auto slicesLocked = slices.rlock();
    return getSliceBySliceIdentifier(slicesLocked, sliceIdentifier);
}

std::optional<StreamSlicePtr> StreamJoinOperatorHandler::getSliceBySliceIdentifier(const RLockedSlices& slicesLocked,
                                                                                   uint64_t sliceIdentifier) {
    {
        for (auto& curSlice : *slicesLocked) {
            if (curSlice->getSliceIdentifier() == sliceIdentifier) {
                return curSlice;
            }
        }
    }
    return std::nullopt;
}

std::optional<StreamSlicePtr> StreamJoinOperatorHandler::getSliceBySliceIdentifier(const WLockedSlices& slicesLocked,
                                                                                   uint64_t sliceIdentifier) {
    {
        for (auto& curSlice : *slicesLocked) {
            if (curSlice->getSliceIdentifier() == sliceIdentifier) {
                return curSlice;
            }
        }
    }
    return std::nullopt;
}


void StreamJoinOperatorHandler::triggerAllSlices(PipelineExecutionContext* pipelineCtx) {
    {
        auto [slicesLocked, windowToSlicesLocked] = folly::acquireLocked(slices, windowToSlices);
        for (auto& [windowInfo, slicesAndStateForWindow] : *windowToSlicesLocked) {
            switch (slicesAndStateForWindow.windowState) {
                case WindowInfoState::BOTH_SIDES_FILLING: slicesAndStateForWindow.windowState = WindowInfoState::ONCE_SEEN_DURING_TERMINATION;
                case WindowInfoState::EMITTED_TO_PROBE: continue;
                case WindowInfoState::ONCE_SEEN_DURING_TERMINATION: {
                    slicesAndStateForWindow.windowState = WindowInfoState::EMITTED_TO_PROBE;

                    // Performing a cross product of all slices to make sure that each slice gets probe with each other slice
                    // For bucketing, this should be only done once
                    for (auto& sliceLeft : slicesAndStateForWindow.slices) {
                        for (auto& sliceRight : slicesAndStateForWindow.slices) {
                            emitSliceIdsToProbe(*sliceLeft, *sliceRight, windowInfo, pipelineCtx);
                        }
                    }
                }
            }
        }
    }
}

void StreamJoinOperatorHandler::checkAndTriggerWindows(const BufferMetaData& bufferMetaData, PipelineExecutionContext* pipelineCtx) {
    // The watermark processor handles the minimal watermark across both streams
    uint64_t newGlobalWatermark = watermarkProcessorBuild->updateWatermark(bufferMetaData.watermarkTs,
                                                                           bufferMetaData.seqNumber,
                                                                           bufferMetaData.originId);
    NES_DEBUG("newGlobalWatermark {} bufferMetaData {} ", newGlobalWatermark, bufferMetaData.toString());

    {
        auto [slicesLocked, windowToSlicesLocked] = folly::acquireLocked(slices, windowToSlices);
        for (auto& [windowInfo, slicesAndStateForWindow] : *windowToSlicesLocked) {
            if (windowInfo.windowEnd > newGlobalWatermark ||
                slicesAndStateForWindow.windowState == WindowInfoState::EMITTED_TO_PROBE) {
                // This window can not be triggered yet or has already been triggered
                continue;
            }
            slicesAndStateForWindow.windowState = WindowInfoState::EMITTED_TO_PROBE;
            NES_INFO("Emitting all slices for window {}", windowInfo.toString());


            // Performing a cross product of all slices to make sure that each slice gets probe with each other slice
            // For bucketing, this should be only done once
            for (auto& sliceLeft : slicesAndStateForWindow.slices) {
                for (auto& sliceRight : slicesAndStateForWindow.slices) {
                    emitSliceIdsToProbe(*sliceLeft, *sliceRight, windowInfo, pipelineCtx);
                }
            }
        }
    }
}

void StreamJoinOperatorHandler::deleteSlices(const BufferMetaData& bufferMetaData) {
    uint64_t newGlobalWaterMarkProbe = watermarkProcessorProbe->updateWatermark(bufferMetaData.watermarkTs,
                                                                                bufferMetaData.seqNumber,
                                                                                bufferMetaData.originId);
    NES_DEBUG("newGlobalWaterMarkProbe {} bufferMetaData {}", newGlobalWaterMarkProbe, bufferMetaData.toString());

    auto slicesLocked = slices.wlock();
    for (auto it = slicesLocked->begin(); it != slicesLocked->end(); ++it) {
        auto& curSlice = *it;
        if (curSlice->getSliceStart() + windowSize < newGlobalWaterMarkProbe) {
            // We can delete this slice/window
            NES_DEBUG("Deleting slice: {} as sliceStart+windowSize {} is smaller then watermark {}",
                      curSlice->toString(), curSlice->getSliceStart() + windowSize, newGlobalWaterMarkProbe);
            it = slicesLocked->erase(it);
        }
    }
}

uint64_t StreamJoinOperatorHandler::getNumberOfSlices() { return slices.rlock()->size(); }

uint64_t StreamJoinOperatorHandler::getNumberOfTuplesInSlice(uint64_t sliceIdentifier,
                                                             QueryCompilation::JoinBuildSideType buildSide) {
    auto slice = getSliceBySliceIdentifier(sliceIdentifier);
    if (slice.has_value()) {
        auto& sliceVal = slice.value();
        switch (buildSide) {
            case QueryCompilation::JoinBuildSideType::Left: return sliceVal->getNumberOfTuplesLeft();
            case QueryCompilation::JoinBuildSideType::Right: return sliceVal->getNumberOfTuplesRight();
        }
    }
    return -1;
}

OriginId StreamJoinOperatorHandler::getOutputOriginId() const { return outputOriginId; }

uint64_t StreamJoinOperatorHandler::getNextSequenceNumber() { return sequenceNumber++; }

void StreamJoinOperatorHandler::setNumberOfWorkerThreads(uint64_t numberOfWorkerThreads) {
    if (StreamJoinOperatorHandler::alreadySetup) {
        NES_DEBUG("StreamJoinOperatorHandler::setup was called already!");
        return;
    }
    StreamJoinOperatorHandler::alreadySetup = true;

    NES_DEBUG("HashJoinOperatorHandler::setup was called!");
    StreamJoinOperatorHandler::numberOfWorkerThreads = numberOfWorkerThreads;
}

void StreamJoinOperatorHandler::updateWatermarkForWorker(uint64_t watermark, uint64_t workerId) {
    workerIdToWatermarkMap[workerId] = watermark;
}

uint64_t StreamJoinOperatorHandler::getMinWatermarkForWorker() {
    auto minVal =
        std::min_element(std::begin(workerIdToWatermarkMap), std::end(workerIdToWatermarkMap), [](const auto& l, const auto& r) {
            return l.second < r.second;
        });
    return minVal == workerIdToWatermarkMap.end() ? -1 : minVal->second;
}

uint64_t StreamJoinOperatorHandler::getWindowSlide() const { return sliceAssigner.getWindowSlide(); }

uint64_t StreamJoinOperatorHandler::getWindowSize() const { return sliceAssigner.getWindowSize(); }

StreamJoinOperatorHandler::StreamJoinOperatorHandler(const std::vector<OriginId>& inputOrigins,
                                                     const OriginId outputOriginId,
                                                     const uint64_t windowSize,
                                                     const uint64_t windowSlide,
                                                     uint64_t sizeOfRecordLeft,
                                                     uint64_t sizeOfRecordRight)
    : numberOfWorkerThreads(1), sliceAssigner(windowSize, windowSlide), windowSize(windowSize), windowSlide(windowSlide),
      watermarkProcessorBuild(std::make_unique<MultiOriginWatermarkProcessor>(inputOrigins)),
      watermarkProcessorProbe(std::make_unique<MultiOriginWatermarkProcessor>(std::vector<OriginId>(1, outputOriginId))),
      outputOriginId(outputOriginId), sequenceNumber(1), sizeOfRecordLeft(sizeOfRecordLeft), sizeOfRecordRight(sizeOfRecordRight) {}

} // namespace NES::Runtime::Execution::Operators