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

#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandlerSlicing.hpp>

namespace NES::Runtime::Execution::Operators {

StreamSlicePtr StreamJoinOperatorHandlerSlicing::getSliceByTimestampOrCreateIt(uint64_t timestamp) {
    auto [slicesWriteLocked, windowSliceIdToStateLocked] = folly::acquireLocked(slices, windowSliceIdToState);
    for (auto& curSlice : *slicesWriteLocked) {
        if (curSlice->getSliceStart() <= timestamp && timestamp < curSlice->getSliceEnd()) {
            return curSlice;
        }
    }

    auto sliceStart = sliceAssigner.getSliceStartTs(timestamp);
    auto sliceEnd = sliceAssigner.getSliceEndTs(timestamp);
    auto newSlice = createNewSlice(sliceStart, sliceEnd);
    NES_DEBUG("Create NLJ slice for slice start={} and end={} for ts={}", sliceStart, sliceEnd, timestamp);

    slicesWriteLocked->emplace_back(newSlice);

    // For all possible slices in their respective windows, reset the state
    for (auto& windowInfo : getAllWindowsForSlice(*newSlice)) {
        NES_DEBUG("reset the state for window {}", windowInfo.toString());
        windowSliceIdToStateLocked->insert({WindowSliceIdKey(newSlice->getSliceIdentifier(), windowInfo.windowId),
                                            StreamSlice::SliceState::BOTH_SIDES_FILLING});
    }
    return newSlice;
}

StreamSlice* StreamJoinOperatorHandlerSlicing::getCurrentWindowOrCreate() {
    if (slices.rlock()->empty()) {
        return StreamJoinOperatorHandlerSlicing::getSliceByTimestampOrCreateIt(0).get();
    }
    return slices.rlock()->back().get();
}

std::vector<std::pair<StreamSlicePtr, StreamSlicePtr>>
StreamJoinOperatorHandlerSlicing::getSlicesLeftRightForWindow(uint64_t windowId) {
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

void StreamJoinOperatorHandlerSlicing::triggerSlices(TriggerableWindows& triggerableWindows,
                                                     PipelineExecutionContext* pipelineCtx) {
    /**
     * We expect the sliceIdentifiersToBeTriggered to be sorted.
     * Otherwise, it can happen that the buffer <seq 2, ts 1000> gets processed after <seq 1, ts 20000>, which will lead to wrong results upstream
     * Also, we have to set the seq number in order of the slice end timestamps.
     * Furthermore, we expect the sliceIdentifier to be the sliceEnd.
     */
    for (const auto& [windowId, slices] : triggerableWindows.windowIdToTriggerableSlices) {
        for (auto& [curSliceLeft, curSliceRight] : getSlicesLeftRightForWindow(windowId)) {
            emitSliceIdsToProbe(*curSliceLeft, *curSliceRight, slices.windowInfo, pipelineCtx);
        }
    }
}
std::vector<WindowInfo> StreamJoinOperatorHandlerSlicing::getAllWindowsForSlice(StreamSlice& slice) {
    std::vector<WindowInfo> allWindows;

    const auto sliceStart = slice.getSliceStart();
    const auto sliceEnd = slice.getSliceEnd();
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
}// namespace namespace NES::Runtime::Execution::Operators