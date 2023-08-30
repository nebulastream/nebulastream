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

#include <Execution/Operators/Streaming/Join/StreamJoinOperator.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandlerBucketing.hpp>
#include <numeric>

namespace NES::Runtime::Execution::Operators {

std::vector<StreamSlice*>* StreamJoinOperatorHandlerBucketing::getAllWindowsToFillForTs(uint64_t ts) {
    auto [slicesWriteLocked, windowSliceIdToStateLocked] = folly::acquireLocked(slices, windowSliceIdToState);

    int64_t timestamp = ts;
    int64_t remainder = (timestamp % windowSlide);
    int64_t lastStart = (timestamp - remainder);
    int64_t lowerBound = timestamp - windowSize;

    windowsToFill.clear();
    for (int64_t start = lastStart; start >= 0 && start > lowerBound; start -= windowSlide) {
        WindowInfo windowInfo(start, start + windowSize);
        auto window = getSliceBySliceIdentifier(slicesWriteLocked, windowInfo.windowId);
        if (window.has_value()) {
            windowsToFill.emplace_back(window.value().get());
        } else {
            auto newWindow = createNewSlice(windowInfo.windowStart, windowInfo.windowEnd);
            windowSliceIdToStateLocked->insert({WindowSliceIdKey(newWindow->getSliceIdentifier(), windowInfo.windowId),
                                                StreamSlice::SliceState::BOTH_SIDES_FILLING});
            slicesWriteLocked->emplace_back(newWindow);
            windowsToFill.emplace_back(newWindow.get());
        }
    }

    std::string windowsToFillStr = std::accumulate(windowsToFill.begin(), windowsToFill.end(), std::string(),
                                                   [] (const std::string& acc, auto* window) {
                                                      return acc.empty() ? window->toString() : acc + "," + window->toString();
                                                   });
    NES_INFO("getAllWindowsToFillForTs {}: windowsToFill:\n{}", ts, windowsToFillStr);
    return &windowsToFill;
}

void StreamJoinOperatorHandlerBucketing::triggerSlices(TriggerableWindows& triggerableWindows, PipelineExecutionContext* pipelineCtx) {
    /**
     * We expect the sliceIdentifiersToBeTriggered to be sorted.
     * Otherwise, it can happen that the buffer <seq 2, ts 1000> gets processed after <seq 1, ts 20000>, which will lead to wrong results upstream
     * Also, we have to set the seq number in order of the slice end timestamps.
     * Furthermore, we expect the sliceIdentifier to be the sliceEnd.
     */
    for (const auto& [windowId, slices] : triggerableWindows.windowIdToTriggerableSlices) {
        if (slices.slicesToTrigger.size() == 1) {
            emitSliceIdsToProbe(*slices.slicesToTrigger[0], *slices.slicesToTrigger[0], slices.windowInfo, pipelineCtx);
        } else {
            NES_THROW_RUNTIME_ERROR("There should only be one window during bucketing for a single windowId");
        }

    }
}

std::vector<WindowInfo> StreamJoinOperatorHandlerBucketing::getAllWindowsForSlice(StreamSlice& slice) {
    // During bucketing one slice represents one window
    return {WindowInfo(slice.getSliceStart(), slice.getSliceEnd())};
}


void* getAllWindowsToFillProxy(void* ptrOpHandler, uint64_t ts, uint64_t joinStrategyInt, uint64_t windowingStrategyInt) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = StreamJoinOperator::getSpecificOperatorHandler<StreamJoinOperatorHandlerBucketing>(ptrOpHandler,
                                                                                                         joinStrategyInt,
                                                                                                         windowingStrategyInt);
    return opHandler->getAllWindowsToFillForTs(ts);
}

}// namespace NES::Runtime::Execution::Operators