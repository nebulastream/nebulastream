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

#include <Execution/Operators/Streaming/Join/HashJoin/HJSlice.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJSlice.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandlerSlicing.hpp>

namespace NES::Runtime::Execution::Operators {

StreamSlicePtr StreamJoinOperatorHandlerSlicing::getSliceByTimestampOrCreateIt(uint64_t timestamp) {
    auto [slicesWriteLocked, windowToSlicesLocked] = folly::acquireLocked(slices, windowToSlices);

    // Checking, if we maybe already have this slice
    auto sliceStart = sliceAssigner.getSliceStartTs(timestamp);
    auto sliceEnd = sliceAssigner.getSliceEndTs(timestamp);
    auto sliceId = StreamSlice::getSliceIdentifier(sliceStart, sliceEnd);
    auto slice = getSliceBySliceIdentifier(slicesWriteLocked, sliceId);
    if (slice.has_value()) {
        return slice.value();
    }

    // No slice was found for the timestamp
    NES_DEBUG("Creating slice for slice start={} and end={} for ts={}", sliceStart, sliceEnd, timestamp);
    auto newSlice = createNewSlice(sliceStart, sliceEnd);
    slicesWriteLocked->emplace_back(newSlice);

    // For all possible slices in their respective windows, reset the state
    for (auto windowInfo : getAllWindowsForSlice(*newSlice)) {
        NES_DEBUG("reset the state for window {}", windowInfo.toString());
        auto& window = (*windowToSlicesLocked)[windowInfo];
        window.windowState = WindowInfoState::BOTH_SIDES_FILLING;
        window.slices.emplace_back(newSlice);
        NES_DEBUG("Added slice {} to window {}", newSlice->toString(), windowInfo.toString());
    }
    return newSlice;
}

StreamSlice* StreamJoinOperatorHandlerSlicing::getCurrentSliceOrCreate() {
    if (slices.rlock()->empty()) {
        return StreamJoinOperatorHandlerSlicing::getSliceByTimestampOrCreateIt(0).get();
    }
    return slices.rlock()->back().get();
}

std::vector<WindowInfo> StreamJoinOperatorHandlerSlicing::getAllWindowsForSlice(StreamSlice& slice) {
    std::vector<WindowInfo> allWindows;

    const auto sliceStart = slice.getSliceStart();
    const auto sliceEnd = slice.getSliceEnd();

    NES_INFO("calculating windows for slice: sliceStart {}, sliceEnd {}", sliceStart, sliceEnd)

    /**
     * We might have multiple deployment times of the same join shared in this handler. Each of these times defines the starting
     * point for the tumbling/sliding windows for this join with this definition. For each of these definitions we calculate all
     * windows for which this slice is included. All calculated windows of all definitions get returned as one vector.
     * For each definition do:
     * To get all windows, we start with the window (firstWindow) that the current slice is the last slice in it.
     * Or in other words: The sliceEnd is equal to the windowEnd.
     * We then add all windows with a slide until we reach the window (lastWindow) that that current slice is the first slice.
     * Or in other words: The sliceStart is equal to the windowStart;
     */

    for (auto [_, deploymentTime] : deploymentTimes) {
        // The end of the first window will be calculated in the same way as in sliceAssigner (explanation is there). There we calculate the
        // nextWindowEndAfterTs which is essentially the first window that contains this slice. (ts = sliceStart)
        // if (ts < windowDeploymentTime + windowSize){ nextWindowEndAfterTs = windowDeploymentTime + windowSize; }
        // else { nextWindowEndAfterTs = ts - ((ts - windowDeploymentTime - windowSize) % windowSlide) + windowSlide; }
        auto firstWindowEnd = sliceStart - ((sliceStart - windowSize - deploymentTime) % windowSlide) + windowSlide;
        if (sliceStart < deploymentTime + windowSize) {
            firstWindowEnd = deploymentTime + windowSize;
        }

        // The end of the last window will be calculated in a similar way as in sliceAssigner. There we calculate the
        // lastWindowStartBeforeTs which is the last window that will contain this slice and to calculate its end we add windowSize.
        // ts - ((ts - windowDeploymentTime) % windowSlide); //ts = sliceStart
        auto lastWindowEnd = sliceStart - ((sliceStart - deploymentTime) % windowSlide) + windowSize;

        for (auto curWindowEnd = firstWindowEnd; curWindowEnd <= lastWindowEnd; curWindowEnd += windowSlide) {
            // For now, we expect the windowEnd to be the windowId
            allWindows.emplace_back(curWindowEnd - windowSize, curWindowEnd);
        }

        NES_INFO("Adding calculated windows for deployment time {}, fWEnd {}, lWEnd {}, total number of calculated windows {}",
                 deploymentTime,
                 firstWindowEnd,
                 lastWindowEnd,
                 allWindows.size());
    }
    return allWindows;
}
}// namespace NES::Runtime::Execution::Operators
