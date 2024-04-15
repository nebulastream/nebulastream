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

namespace NES::Runtime::Execution::Operators {
void StreamJoinOperatorHandler::start(PipelineExecutionContextPtr pipelineCtx, uint32_t) {
    NES_INFO("Started StreamJoinOperatorHandler!");
    setNumberOfWorkerThreads(pipelineCtx->getNumberOfWorkerThreads());
    setBufferManager(pipelineCtx->getBufferManager());
}

void StreamJoinOperatorHandler::stop(QueryTerminationType queryTerminationType, PipelineExecutionContextPtr pipelineCtx) {
    NES_INFO("Stopped StreamJoinOperatorHandler with {}!", magic_enum::enum_name(queryTerminationType));
    if (queryTerminationType == QueryTerminationType::Graceful) {
#ifdef UNIKERNEL_LIB
        triggerAllSlices(pipelineCtx);
#else
        triggerAllSlices(pipelineCtx.get());
#endif
    }
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
    std::vector<std::pair<WindowInfo, std::vector<StreamSlicePtr>>> toBeEmitted;
    {
        auto [slicesLocked, windowToSlicesLocked] = folly::acquireLocked(slices, windowToSlices);
        for (auto& [windowInfo, slicesAndStateForWindow] : *windowToSlicesLocked) {
            switch (slicesAndStateForWindow.windowState) {
                case WindowInfoState::BOTH_SIDES_FILLING:
                    slicesAndStateForWindow.windowState = WindowInfoState::ONCE_SEEN_DURING_TERMINATION;
                case WindowInfoState::EMITTED_TO_PROBE: continue;
                case WindowInfoState::ONCE_SEEN_DURING_TERMINATION: {
                    slicesAndStateForWindow.windowState = WindowInfoState::EMITTED_TO_PROBE;
                    toBeEmitted.emplace_back(windowInfo, std::move(slicesAndStateForWindow.slices));
                }
            }
        }
    }

    // Performing a cross product of all slices to make sure that each slice gets probe with each other slice
    // For bucketing, this should be only done once
    for (const auto& [window, emittedSlices] : toBeEmitted) {
        for (auto& sliceLeft : emittedSlices) {
            for (auto& sliceRight : emittedSlices) {
                emitSliceIdsToProbe(*sliceLeft, *sliceRight, window, pipelineCtx);
            }
        }
    }
}

void StreamJoinOperatorHandler::deleteAllSlices() {
    {
        auto [slicesLocked, windowToSlicesLocked] = folly::acquireLocked(slices, windowToSlices);
        slicesLocked->clear();
        windowToSlicesLocked->clear();
    }
}

void StreamJoinOperatorHandler::checkAndTriggerWindows(const BufferMetaData& bufferMetaData,
                                                       PipelineExecutionContext* pipelineCtx) {
    // The watermark processor handles the minimal watermark across both streams
    uint64_t newGlobalWatermark =
        watermarkProcessorBuild->updateWatermark(bufferMetaData.watermarkTs, bufferMetaData.seqNumber, bufferMetaData.originId);
    NES_DEBUG("newGlobalWatermark {} bufferMetaData {} ", newGlobalWatermark, bufferMetaData.toString());

    {
        auto [slicesLocked, windowToSlicesLocked] = folly::acquireLocked(slices, windowToSlices);
        for (auto& [windowInfo, slicesAndStateForWindow] : *windowToSlicesLocked) {
            if (windowInfo.windowEnd > newGlobalWatermark
                || slicesAndStateForWindow.windowState == WindowInfoState::EMITTED_TO_PROBE) {
                // This window can not be triggered yet or has already been triggered
                continue;
            }
            slicesAndStateForWindow.windowState = WindowInfoState::EMITTED_TO_PROBE;
            NES_DEBUG("Emitting all slices for window {}", windowInfo.toString());

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
    uint64_t newGlobalWaterMarkProbe =
        watermarkProcessorProbe->updateWatermark(bufferMetaData.watermarkTs, bufferMetaData.seqNumber, bufferMetaData.originId);
    NES_DEBUG("newGlobalWaterMarkProbe {} bufferMetaData {}", newGlobalWaterMarkProbe, bufferMetaData.toString());

    auto slicesLocked = slices.wlock();
    for (auto it = slicesLocked->begin(); it != slicesLocked->end(); ++it) {
        auto& curSlice = *it;
        if (curSlice->getSliceStart() + windowSize < newGlobalWaterMarkProbe) {
            // We can delete this slice/window
            NES_DEBUG("Deleting slice: {} as sliceStart+windowSize {} is smaller then watermark {}",
                      curSlice->toString(),
                      curSlice->getSliceStart() + windowSize,
                      newGlobalWaterMarkProbe);
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

    NES_DEBUG("StreamJoinOperatorHandler::setup was called!");
    StreamJoinOperatorHandler::numberOfWorkerThreads = numberOfWorkerThreads;
}

void StreamJoinOperatorHandler::updateWatermarkForWorker(uint64_t watermark, WorkerThreadId workerThreadId) {
    workerThreadIdToWatermarkMap[workerThreadId] = watermark;
}

uint64_t StreamJoinOperatorHandler::getMinWatermarkForWorker() {
    auto minVal = std::min_element(std::begin(workerThreadIdToWatermarkMap),
                                   std::end(workerThreadIdToWatermarkMap),
                                   [](const auto& l, const auto& r) {
                                       return l.second < r.second;
                                   });
    return minVal == workerThreadIdToWatermarkMap.end() ? -1 : minVal->second;
}

uint64_t StreamJoinOperatorHandler::getWindowSlide() const { return sliceAssigner.getWindowSlide(); }

uint64_t StreamJoinOperatorHandler::getWindowSize() const { return sliceAssigner.getWindowSize(); }

void StreamJoinOperatorHandler::setBufferManager(const NES::Runtime::BufferManagerPtr& bufManager) {
    this->bufferManager = bufManager;
}

StreamJoinOperatorHandler::StreamJoinOperatorHandler(const std::vector<OriginId>& inputOrigins,
                                                     const OriginId outputOriginId,
                                                     const uint64_t windowSize,
                                                     const uint64_t windowSlide)
    : numberOfWorkerThreads(1), sliceAssigner(windowSize, windowSlide), windowSize(windowSize), windowSlide(windowSlide),
      watermarkProcessorBuild(std::make_unique<MultiOriginWatermarkProcessor>(inputOrigins)),
      watermarkProcessorProbe(std::make_unique<MultiOriginWatermarkProcessor>(std::vector<OriginId>(1, outputOriginId))),
      outputOriginId(outputOriginId), sequenceNumber(1) {}

}// namespace NES::Runtime::Execution::Operators
