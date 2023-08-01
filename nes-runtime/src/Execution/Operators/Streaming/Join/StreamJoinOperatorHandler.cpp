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
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/DataStructure/NLJWindow.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/DataStructure/StreamHashJoinWindow.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/StreamHashJoinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <optional>

namespace NES::Runtime::Execution::Operators {

StreamWindowPtr StreamJoinOperatorHandler::createNewWindow(uint64_t timestamp) {
    auto windowsWriteLocked = windows.wlock();
    for (auto& curWindow : *windowsWriteLocked) {
        if (curWindow->getWindowStart() <= timestamp && timestamp < curWindow->getWindowEnd()) {
            return curWindow;
        }
    }

    auto windowStart = sliceAssigner.getSliceStartTs(timestamp);
    auto windowEnd = sliceAssigner.getSliceEndTs(timestamp);

    //TODO create a factory class for stream join windows #3900
    StreamWindowPtr newWindow;
    if (joinStrategy == QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN) {
        auto* nljOpHandler = dynamic_cast<NLJOperatorHandler*>(this);
        NES_DEBUG("Create NLJ Window for window start={} windowend={} for ts={}", windowStart, windowEnd, timestamp);
        newWindow = std::make_unique<NLJWindow>(windowStart,
                                                windowEnd,
                                                numberOfWorkerThreads,
                                                sizeOfRecordLeft,
                                                nljOpHandler->getLeftPageSize(),
                                                sizeOfRecordRight,
                                                nljOpHandler->getRightPageSize());
    } else {
        auto* ptr = dynamic_cast<StreamHashJoinOperatorHandler*>(this);
        newWindow = std::make_shared<StreamHashJoinWindow>(numberOfWorkerThreads,
                                                           windowStart,
                                                           windowEnd,
                                                           sizeOfRecordLeft,
                                                           sizeOfRecordRight,
                                                           ptr->getTotalSizeForDataStructures(),
                                                           ptr->getPageSize(),
                                                           ptr->getPreAllocPageSizeCnt(),
                                                           ptr->getNumPartitions(),
                                                           joinStrategy);
        NES_DEBUG("Create Hash Window for window start={} windowend={} for ts={}", windowStart, windowEnd, timestamp);
    }

    windowsWriteLocked->emplace_back(newWindow);
    return newWindow;
}

void StreamJoinOperatorHandler::deleteWindow(uint64_t windowIdentifier) {
    NES_DEBUG("StreamJoinOperatorHandler trying to delete window with id={}", windowIdentifier);
    {
        auto windowsLocked = windows.wlock();
        for (auto it = windowsLocked->begin(); it != windowsLocked->end(); ++it) {
            const auto curWindow = *it;
            if (curWindow->getWindowIdentifier() == windowIdentifier) {
                windowsLocked->erase(it);
                return;
            }
        }
    }
}

StreamWindowPtr StreamJoinOperatorHandler::getWindowByTimestampOrCreateIt(uint64_t timestamp) {
    {
        auto windowsLocked = windows.rlock();
        for (auto& curWindow : *windowsLocked) {
            if (curWindow->getWindowStart() <= timestamp && timestamp < curWindow->getWindowEnd()) {
                return curWindow;
            }
        }
    }
    return createNewWindow(timestamp);
}

uint64_t StreamJoinOperatorHandler::getNumberOfWindows() { return windows.rlock()->size(); }

std::optional<StreamWindowPtr> StreamJoinOperatorHandler::getWindowByWindowIdentifier(uint64_t windowIdentifier) {
    {
        auto windowsLocked = windows.rlock();
        for (auto& curWindow : *windowsLocked) {
            if (curWindow->getWindowIdentifier() == windowIdentifier) {
                return curWindow;
            }
        }
    }
    return std::nullopt;
}

std::pair<uint64_t, uint64_t> StreamJoinOperatorHandler::getWindowStartEnd(uint64_t windowIdentifier) {
    const auto& window = getWindowByWindowIdentifier(windowIdentifier);
    if (window.has_value()) {
        return {window.value()->getWindowStart(), window.value()->getWindowEnd()};
    }
    return {};
}

StreamJoinOperatorHandler::StreamJoinOperatorHandler(const std::vector<OriginId>& inputOrigins,
                                                     const OriginId outputOriginId,
                                                     const uint64_t windowSize,
                                                     const QueryCompilation::StreamJoinStrategy joinStrategy,
                                                     uint64_t sizeOfRecordLeft,
                                                     uint64_t sizeOfRecordRight)
    : numberOfWorkerThreads(1), sliceAssigner(windowSize, windowSize),
      watermarkProcessor(std::make_unique<MultiOriginWatermarkProcessor>(inputOrigins)), joinStrategy(joinStrategy),
      outputOriginId(outputOriginId), sequenceNumber(1), sizeOfRecordLeft(sizeOfRecordLeft),
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

OperatorId StreamJoinOperatorHandler::getOutputOriginId() const { return outputOriginId; }

uint64_t StreamJoinOperatorHandler::getMinWatermarkForWorker() {
    auto minVal =
        std::min_element(std::begin(workerIdToWatermarkMap), std::end(workerIdToWatermarkMap), [](const auto& l, const auto& r) {
            return l.second < r.second;
        });
    return minVal == workerIdToWatermarkMap.end() ? -1 : minVal->second;
}

std::vector<uint64_t> StreamJoinOperatorHandler::triggerAllWindows() {
    std::vector<uint64_t> windowIdentifiers;
    {
        // Getting a lock and then iterating over all windows. For each window, we change the state to ONCE_SEEN_DURING_TERMINATION.
        // If this was already the state, the window has been seen by both sides during termination, and we can emit it to the probe
        auto windowsLocked = windows.rlock();
        for (auto& window : *windowsLocked) {

            if (window->shouldTriggerDuringTerminate() && window->getNumberOfTuplesLeft() > 0
                && window->getNumberOfTuplesRight() > 0) {
                windowIdentifiers.emplace_back(window->getWindowIdentifier());
                NES_DEBUG("Added window with id {} to the triggerable windows with {} tuples left and {} tuples right...",
                          window->getWindowIdentifier(),
                          window->getNumberOfTuplesLeft(),
                          window->getNumberOfTuplesRight());
            }
        }
    }
    return windowIdentifiers;
}

std::vector<uint64_t> StreamJoinOperatorHandler::checkWindowsTrigger(const uint64_t watermarkTs,
                                                                     const uint64_t sequenceNumber,
                                                                     const OriginId originId) {
    // The watermark processor handles the minimal watermark across both streams
    uint64_t newGlobalWatermark = watermarkProcessor->updateWatermark(watermarkTs, sequenceNumber, originId);

    std::vector<uint64_t> triggerableWindowIdentifiers;
    {
        auto windowsLocked = windows.rlock();
        for (auto& window : *windowsLocked) {
            if (window->getWindowEnd() > newGlobalWatermark || window->isAlreadyEmitted()) {
                continue;
            }

            // Checking if the window has not been emitted and both sides (left and right) have at least one tuple in their windows.
            auto expected = StreamWindow::WindowState::BOTH_SIDES_FILLING;
            if (window->compareExchangeStrong(expected, StreamWindow::WindowState::EMITTED_TO_PROBE)
                && window->getNumberOfTuplesLeft() > 0 && window->getNumberOfTuplesRight() > 0) {
                triggerableWindowIdentifiers.emplace_back(window->getWindowIdentifier());
                NES_DEBUG("Added window with id {} to the triggerable windows with {} tuples left and {} tuples right...",
                          window->getWindowIdentifier(),
                          window->getNumberOfTuplesLeft(),
                          window->getNumberOfTuplesRight());
            }
        }
    }

    return triggerableWindowIdentifiers;
}
}// namespace NES::Runtime::Execution::Operators