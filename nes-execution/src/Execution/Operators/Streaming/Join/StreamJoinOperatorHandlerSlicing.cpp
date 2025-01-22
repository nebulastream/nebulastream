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

    return getSliceByTimestampOrCreateItLocked(timestamp, slicesWriteLocked, windowToSlicesLocked);
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

std::vector<WindowInfo> StreamJoinOperatorHandlerSlicing::getAllWindowsOfDeploymentTimeForSlice(StreamSlice& slice,
                                                                                                uint64_t deploymentTime) {
    std::vector<WindowInfo> allWindows;

    const auto sliceStart = slice.getSliceStart();
    const auto sliceEnd = slice.getSliceEnd();

    NES_INFO("calculating windows for slice: sliceStart {}, sliceEnd {}", sliceStart, sliceEnd)

    /**
     * To get all windows, we start with the window (firstWindow) that the current slice is the last slice in it.
     * Or in other words: The sliceEnd is equal to the windowEnd.
     * We then add all windows with a slide until we reach the window (lastWindow) that that current slice is the first slice.
     * Or in other words: The sliceStart is equal to the windowStart;
     */

    // The end of the first window will be calculated in the same way as in sliceAssigner (explanation is there). There we calculate the
    // nextWindowEndAfterTs which is essentially the first window that contains this slice.
    auto firstWindowEnd = sliceStart - ((sliceStart - windowSize - deploymentTime) % windowSlide) + windowSlide;
    if (sliceStart < deploymentTime + windowSize) {
        firstWindowEnd = deploymentTime + windowSize;
    }

    // The end of the last window will be calculated in a similar way as in sliceAssigner. There we calculate the
    // lastWindowStartBeforeTs which is the last window that will contain this slice and to calculate its end we add windowSize.
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

    return allWindows;
}

void StreamJoinOperatorHandlerSlicing::addQueryToSharedJoinApproachProbing(QueryId queryId, uint64_t deploymentTime) {
    if (getApproach() == SharedJoinApproach::UNSHARED || getApproach() == SharedJoinApproach::APPROACH_PROBING) {
        setApproach(SharedJoinApproach::APPROACH_PROBING);
    } else {
        NES_ERROR("approach to adding a query to a shared join should not be changed during a test")
    }

    deploymentTimes.emplace(queryId, deploymentTime);
    sliceAssigner.addWindowDeploymentTime(deploymentTime);

    NES_INFO("Add new query that uses same joinOperatorHandler with approach ?????. QueryId {}, deployment time {}, number "
             "of queries handler is "
             "handling now {}",
             queryId,
             deploymentTime,
             deploymentTimes.size())

    auto [slicesWriteLocked, windowToSlicesLocked] = folly::acquireLocked(slices, windowToSlices);

    for (auto slice : *slicesWriteLocked) {
        if (slice->getSliceEnd() >= deploymentTime) {

            NES_INFO("update windows for slice with start {}, end {}", slice->getSliceStart(), slice->getSliceEnd())

            // A slice would be cut in half if a second query arrives that this handler needs to take care of. If this already
            // happened and another query arrives this slice would be split up into even more parts. Now we want to add it to all
            // new windows, and it's enough if we pretend it's just the smallest or just the biggest part as one of these parts is
            // definitely in the window.
            uint64_t sliceStart = slice->getSliceStart();
            uint64_t firstPartSliceEnd = sliceAssigner.getSliceEndTs(slice->getSliceStart());
            uint64_t lastPartSliceStart = sliceAssigner.getSliceStartTs(slice->getSliceEnd() - 1);
            uint64_t sliceEnd = slice->getSliceEnd();

            for (auto sliceStartEnd :
                 {std::make_pair(sliceStart, firstPartSliceEnd), std::make_pair(lastPartSliceStart, sliceEnd)}) {
                slice->setSliceStart(sliceStartEnd.first);
                slice->setSliceEnd(sliceStartEnd.second);
                NES_INFO("adding windows for old slice by pretending: start {}, end {}",
                         slice->getSliceStart(),
                         slice->getSliceEnd())

                //add slice to all newly added windows. Skip windows of which it is already a part of
                for (auto windowInfo : getAllWindowsForSlice(*slice)) {
                    auto& window = (*windowToSlicesLocked)[windowInfo];

                    if (std::find(window.slices.begin(), window.slices.end(), slice) != window.slices.end()) {
                        std::ostringstream oss;
                        for (const auto& s : window.slices) {
                            oss << s->toString() << "\n";
                        }
                        std::string result = oss.str();
                        NES_INFO("dont add slice to current window {} \t as it already contains these slices {}",
                                 windowInfo.toString(),
                                 result);
                        continue;
                    }

                    window.slices.emplace_back(slice);
                    NES_DEBUG("Added slice (sliceStart: {} sliceEnd: {} sliceId: {}) to window {}, #slices {}",
                              sliceStart,
                              sliceEnd,
                              slice->getSliceId(),
                              windowInfo.toString(),
                              window.slices.size())
                }
            }

            //original start and end
            slice->setSliceStart(sliceStart);
            slice->setSliceEnd(sliceEnd);
        }
    }
}

void StreamJoinOperatorHandlerSlicing::removeQueryFromSharedJoin(QueryId queryId) {
    NES_INFO("Removing Query {} from a shared join operator handler", queryId)

    if (deploymentTimes.find(queryId) == deploymentTimes.end()) {
        NES_THROW_RUNTIME_ERROR("tried to remove a query that did not exist");
    }

    auto deploymentTime = deploymentTimes.at(queryId);
    sliceAssigner.removeWindowDeploymentTime(deploymentTime);
    deploymentTimes.erase(queryId);

    auto [slicesLocked, windowToSlicesLocked] = folly::acquireLocked(slices, windowToSlices);
    for (auto& [windowInfo, slicesAndStateForWindow] : *windowToSlicesLocked) {
        // windows that do not start at a time that is x * windowSize + deploymentTime do not belong to this query and we do not need to do anything for them
        if ((windowInfo.windowStart - deploymentTime) % windowSize != 0) {
            continue;
        }

        // setting all slices that are needed for windows of the removed query to not writable, so the correct tuples can be sent to the probe later
        // we cant simply sent the tuples to probe in this method as the emitting is connected to the watermark and will invalidate assumptions
        for (auto& slice : slicesAndStateForWindow.slices) {
            slice->setImmutable();
            NES_INFO("set slice {} to not writable, as an associated query got removed and we want to emit all tuples for this "
                     "query that currently belong",
                     slice->toString())
        }
    }
}
StreamSlicePtr StreamJoinOperatorHandlerSlicing::getSliceByTimestampOrCreateItLocked(uint64_t timestamp,
                                                                                     WLockedSlices& slicesWriteLocked,
                                                                                     WLockedWindows& windowToSlicesLocked) {
    // Checking, if we maybe already have this slice
    auto sliceStart = sliceAssigner.getSliceStartTs(timestamp);
    auto sliceEnd = sliceAssigner.getSliceEndTs(timestamp);
    auto slice = getSliceByStartEnd(slicesWriteLocked, sliceStart, sliceEnd);
    if (slice.has_value()) {
        // NES_DEBUG("Slice had value for: slice start {}, slice end {}, slice {}", sliceStart, sliceEnd, slice.value()->toString())
        return slice.value();
    }

    // No slice was found for the timestamp
    NES_DEBUG("Creating slice for slice start={} and end={} for ts={}", sliceStart, sliceEnd, timestamp);
    auto newSlice = createNewSlice(sliceStart, sliceEnd, getNextSliceId());
    slicesWriteLocked->emplace_back(newSlice);

    // For all possible slices in their respective windows, reset the state
    for (auto windowInfo : getAllWindowsForSlice(*newSlice)) {
        NES_DEBUG("reset the state for window {}", windowInfo.toString());

        auto& window = (*windowToSlicesLocked)[windowInfo];
        // If the window existed its state is probably already BOTH_SIDES_FILLING. There is only one scenario (I am aware of)
        // where a window with a different state could exist as there should be no tuples ingested for windows that are already
        // emitted. However, if the standard slice for ts=0 gets created (like in the build setup) this did reset the state and a
        // test failed
        if (windowToSlicesLocked->find(windowInfo) == windowToSlicesLocked->end()) {
            window.windowState = WindowInfoState::BOTH_SIDES_FILLING;
        }
        window.slices.emplace_back(newSlice);
        NES_DEBUG("Added slice {} to window {} #slices {}", newSlice->toString(), windowInfo.toString(), window.slices.size());
    }
    return newSlice;
}

}// namespace NES::Runtime::Execution::Operators
