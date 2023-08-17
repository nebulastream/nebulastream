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

#include <API/AttributeField.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/DataStructure/NLJSlice.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/DataStructure/StreamHashJoinWindow.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/StreamHashJoinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <numeric>
#include <optional>

namespace NES::Runtime::Execution::Operators {

StreamSlicePtr StreamJoinOperatorHandler::getSliceByTimestampOrCreateIt(uint64_t timestamp) {
    auto [slicesWriteLocked, windowSliceIdToStateLocked] = folly::acquireLocked(slices, windowSliceIdToState);
    for (auto& curSlice : *slicesWriteLocked) {
        if (curSlice->getSliceStart() <= timestamp && timestamp < curSlice->getSliceEnd()) {
            return curSlice;
        }
    }

    auto sliceStart = sliceAssigner.getSliceStartTs(timestamp);
    auto sliceEnd = sliceAssigner.getSliceEndTs(timestamp);

    //TODO create a factory class for stream join windows #3900
    StreamSlicePtr newSlice;
    if (joinStrategy == QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN) {
        auto* nljOpHandler = dynamic_cast<NLJOperatorHandler*>(this);
        NES_DEBUG("Create NLJ slice for slice start={} and end={} for ts={}", sliceStart, sliceEnd, timestamp);
        newSlice = std::make_unique<NLJSlice>(sliceStart,
                                              sliceEnd,
                                              numberOfWorkerThreads,
                                              sizeOfRecordLeft,
                                              nljOpHandler->getLeftPageSize(),
                                              sizeOfRecordRight,
                                              nljOpHandler->getRightPageSize());
    } else {
        auto* ptr = dynamic_cast<StreamHashJoinOperatorHandler*>(this);
        newSlice = std::make_shared<StreamHashJoinWindow>(numberOfWorkerThreads,
                                                          sliceStart,
                                                          sliceEnd,
                                                          sizeOfRecordLeft,
                                                          sizeOfRecordRight,
                                                          ptr->getTotalSizeForDataStructures(),
                                                          ptr->getPageSize(),
                                                          ptr->getPreAllocPageSizeCnt(),
                                                          ptr->getNumPartitions(),
                                                          joinStrategy);
        NES_DEBUG("Create Hash slice for slice start={} and end={} for ts={}", sliceStart, sliceEnd, timestamp);
    }

    slicesWriteLocked->emplace_back(newSlice);

    // For all possible slices in their respective windows, reset the state
    for (auto& windowInfo : getAllWindowsForSlice(*newSlice)) {
        NES_DEBUG("reset the state for window {}", windowInfo.toString());
        windowSliceIdToStateLocked->insert({WindowSliceIdKey(newSlice->getSliceIdentifier(), windowInfo.windowId),
                                            StreamSlice::SliceState::BOTH_SIDES_FILLING});
    }
    return newSlice;
}

void StreamJoinOperatorHandler::deleteSlice(uint64_t sliceIdentifier) {
    NES_DEBUG("StreamJoinOperatorHandler trying to delete slice with id={}", sliceIdentifier);
    {
        auto slicesLocked = slices.wlock();
        for (auto it = slicesLocked->begin(); it != slicesLocked->end(); ++it) {
            const auto curSlice = *it;
            if (curSlice->getSliceIdentifier() == sliceIdentifier) {
                slicesLocked->erase(it);
                return;
            }
        }
    }
}

uint64_t StreamJoinOperatorHandler::getNumberOfSlices() { return slices.rlock()->size(); }

std::optional<StreamSlicePtr> StreamJoinOperatorHandler::getSliceBySliceIdentifier(uint64_t sliceIdentifier) {
    {
        auto slicesLocked = slices.rlock();
        for (auto& curSlice : *slicesLocked) {
            if (curSlice->getSliceIdentifier() == sliceIdentifier) {
                return curSlice;
            }
        }
    }
    return std::nullopt;
}

StreamJoinOperatorHandler::StreamJoinOperatorHandler(const std::vector<OriginId>& inputOrigins,
                                                     const OriginId outputOriginId,
                                                     const uint64_t windowSize,
                                                     const uint64_t windowSlide,
                                                     const QueryCompilation::StreamJoinStrategy joinStrategy,
                                                     uint64_t sizeOfRecordLeft,
                                                     uint64_t sizeOfRecordRight)
    : numberOfWorkerThreads(1), sliceAssigner(windowSize, windowSlide),
      watermarkProcessorBuild(std::make_unique<MultiOriginWatermarkProcessor>(inputOrigins)),
      watermarkProcessorProbe(std::make_unique<MultiOriginWatermarkProcessor>(std::vector<OriginId>(1, outputOriginId))),
      outputOriginId(outputOriginId), sequenceNumber(1), joinStrategy(joinStrategy), sizeOfRecordLeft(sizeOfRecordLeft),
      sizeOfRecordRight(sizeOfRecordRight) {}

void StreamJoinOperatorHandler::start(PipelineExecutionContextPtr, StateManagerPtr, uint32_t) {
    NES_DEBUG("start StreamJoinOperatorHandler");
}

void StreamJoinOperatorHandler::stop(QueryTerminationType, PipelineExecutionContextPtr) {
    NES_DEBUG("stop StreamJoinOperatorHandler");
}

uint64_t StreamJoinOperatorHandler::getNextSequenceNumber() { return sequenceNumber++; }

void StreamJoinOperatorHandler::setup(uint64_t newNumberOfWorkerThreads) {
    if (alreadySetup) {
        NES_DEBUG("StreamJoinOperatorHandler::setup was called already!");
        return;
    }
    alreadySetup = true;

    NES_DEBUG("HashJoinOperatorHandler::setup was called!");
    this->numberOfWorkerThreads = newNumberOfWorkerThreads;
}

void StreamJoinOperatorHandler::updateWatermarkForWorker(uint64_t watermark, uint64_t workerId) {
    workerIdToWatermarkMap[workerId] = watermark;
}

OriginId StreamJoinOperatorHandler::getOutputOriginId() const { return outputOriginId; }

uint64_t StreamJoinOperatorHandler::getMinWatermarkForWorker() {
    auto minVal =
        std::min_element(std::begin(workerIdToWatermarkMap), std::end(workerIdToWatermarkMap), [](const auto& l, const auto& r) {
            return l.second < r.second;
        });
    return minVal == workerIdToWatermarkMap.end() ? -1 : minVal->second;
}

TriggerableWindows StreamJoinOperatorHandler::triggerAllSlices() {
    TriggerableWindows sliceIdentifiers;
    {
        // Getting a lock and then iterating over all slices. For each slice, we change the state to ONCE_SEEN_DURING_TERMINATION.
        // If this was already the state, the slice has been seen by both sides during termination, and we can emit it to the probe
        auto [slicesLocked, windowSliceIdToStateLocked] = folly::acquireLocked(slices, windowSliceIdToState);
        for (auto& slice : *slicesLocked) {
            for (auto& windowInfo : getAllWindowsForSlice(*slice)) {
                const auto windowId = windowInfo.windowId;
                auto& sliceState = windowSliceIdToStateLocked->at(WindowSliceIdKey(slice->getSliceIdentifier(), windowId));

                if (sliceState == StreamSlice::SliceState::ONCE_SEEN_DURING_TERMINATION) {
                    sliceState = StreamSlice::SliceState::EMITTED_TO_PROBE;
                    sliceIdentifiers.windowIdToTriggerableSlices[windowId].add(slice);
                    sliceIdentifiers.windowIdToTriggerableSlices[windowId].windowInfo = windowInfo;
                } else if (sliceState == StreamSlice::SliceState::BOTH_SIDES_FILLING) {
                    sliceState = StreamSlice::SliceState::ONCE_SEEN_DURING_TERMINATION;
                }
            }
        }
    }
    return sliceIdentifiers;
}

void StreamJoinOperatorHandler::deleteSlices(uint64_t watermarkTs, uint64_t sequenceNumber, OriginId originId) {
    uint64_t newGlobalWaterMarkProbe = watermarkProcessorProbe->updateWatermark(watermarkTs, sequenceNumber, originId);
    NES_DEBUG("newGlobalWaterMarkProbe {} watermarkTs {} sequenceNumber {} originid {}",
              newGlobalWaterMarkProbe, watermarkTs, sequenceNumber, originId);

    auto slicesLocked = slices.wlock();
    for (auto it = slicesLocked->begin(); it != slicesLocked->end(); ++it) {
        auto& curSlice = *it;
        if (curSlice->getSliceStart() + sliceAssigner.getWindowSize() < newGlobalWaterMarkProbe) {
            // We can delete this slice
            NES_DEBUG("Deleting slice: {}", curSlice->toString());
            it = slicesLocked->erase(it);
        }
    }
}

TriggerableWindows StreamJoinOperatorHandler::checkSlicesTrigger(const uint64_t watermarkTs,
                                                                const uint64_t sequenceNumber,
                                                                const OriginId originId) {
    // The watermark processor handles the minimal watermark across both streams
    uint64_t newGlobalWatermark = watermarkProcessorBuild->updateWatermark(watermarkTs, sequenceNumber, originId);
    NES_DEBUG("newGlobalWatermark {} watermarkTs {} sequenceNumber {} originid {}",
              newGlobalWatermark, watermarkTs, sequenceNumber, originId);

    TriggerableWindows sliceIdentifiers;
    {
        auto [slicesLocked, windowSliceIdToStateLocked] = folly::acquireLocked(slices, windowSliceIdToState);
        for (auto& curSlice : *slicesLocked) {
            if (curSlice->getSliceEnd() > newGlobalWatermark) {
                continue;
            }
            auto allWindows = getAllWindowsForSlice(*curSlice);
            for (auto& windowInfo : allWindows) {
                const auto windowId = windowInfo.windowId;
                auto& state = windowSliceIdToStateLocked->at(WindowSliceIdKey(curSlice->getSliceIdentifier(), windowId));

                if (state == StreamSlice::SliceState::BOTH_SIDES_FILLING ||
                    state == StreamSlice::SliceState::ONCE_SEEN_DURING_TERMINATION) {
                    state = StreamSlice::SliceState::EMITTED_TO_PROBE;
                    sliceIdentifiers.windowIdToTriggerableSlices[windowId].add(curSlice);
                    sliceIdentifiers.windowIdToTriggerableSlices[windowId].windowInfo = windowInfo;
                }
            }
        }
    }

    return sliceIdentifiers;
}

std::vector<std::pair<StreamSlicePtr, StreamSlicePtr>> StreamJoinOperatorHandler::getSlicesLeftRightForWindow(uint64_t windowId) {
    std::vector<std::pair<WindowSliceIdKey, StreamSlice::SliceState>> allSlicesForWindow;
    {
        auto windowSliceIdToStateLocked = windowSliceIdToState.rlock();
        std::copy_if(windowSliceIdToStateLocked->begin(),
                     windowSliceIdToStateLocked->end(),
                     std::back_inserter(allSlicesForWindow),
                     [windowId](const auto& entry) {
                         return entry.first.windowId == windowId;
                     });
    }

    std::vector<std::pair<StreamSlicePtr, StreamSlicePtr>> retVector;
    for (const auto& outer : allSlicesForWindow) {
        for (const auto& inner : allSlicesForWindow) {
            auto streamSliceOuter = getSliceBySliceIdentifier(outer.first.sliceId).value();
            auto streamSliceInner = getSliceBySliceIdentifier(inner.first.sliceId).value();
            retVector.emplace_back(streamSliceOuter, streamSliceInner);
        }
    }

    return retVector;
}

std::vector<WindowInfo> StreamJoinOperatorHandler::getAllWindowsForSlice(StreamSlice& slice) {
    std::vector<WindowInfo> allWindows;

    const auto sliceStart = slice.getSliceStart();
    const auto sliceEnd = slice.getSliceEnd();
    const auto windowSize = sliceAssigner.getWindowSize();
    const auto windowSlide = sliceAssigner.getWindowSlide();

    /**
     * To get all windows, we start with the window (firstWindow) that the current slice is the last slice in it.
     * Or in other words: The sliceEnd is equal to the windowEnd.
     * We then add all windows with a slide until we reach the window (lastWindow) that that current slice is the first slice.
     * Or in other words: The sliceStart is equal to the windowStart;
     */
    const auto firstWindowEnd = sliceEnd;
    const auto lastWindowEnd = sliceStart + windowSize;
    for (auto curWindowEnd = firstWindowEnd; curWindowEnd <= lastWindowEnd; curWindowEnd += windowSlide) {
        // For now, we expect the windowEnd to be the windowId
        allWindows.emplace_back(curWindowEnd - windowSize, curWindowEnd);
    }

    return allWindows;
}

}// namespace NES::Runtime::Execution::Operators