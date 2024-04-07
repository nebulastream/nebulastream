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

#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandlerBucketing.hpp>

namespace NES::Runtime::Execution::Operators {

void StreamJoinOperatorHandlerBucketing::setNumberOfWorkerThreads(uint64_t numberOfWorkerThreads) {
    StreamJoinOperatorHandler::setNumberOfWorkerThreads(numberOfWorkerThreads);

    windowsToFill.reserve(numberOfWorkerThreads);
    for (auto i = 0_u64; i < numberOfWorkerThreads; ++i) {
        windowsToFill.emplace_back(std::vector<StreamSlice*>());
    }
}

std::vector<StreamSlice*>* StreamJoinOperatorHandlerBucketing::getAllWindowsToFillForTs(uint64_t ts, uint64_t workerId) {
    auto [slicesWriteLocked, windowToSlicesLocked] = folly::acquireLocked(slices, windowToSlices);

    int64_t timestamp = ts;
    int64_t remainder = (timestamp % windowSlide);
    int64_t lastStart = (timestamp - remainder);
    int64_t lowerBound = timestamp - windowSize;

    // Getting the vector for the current worker (via workerId)
    auto& workerVec = windowsToFill[workerId % windowsToFill.size()];
    workerVec.clear();
    for (int64_t start = lastStart; start >= 0 && start > lowerBound; start -= windowSlide) {
        WindowInfo windowInfo(start, start + windowSize);
        auto window = getSliceBySliceIdentifier(slicesWriteLocked, windowInfo.windowId);
        if (window.has_value()) {
            workerVec.emplace_back(window.value().get());
        } else {
            auto newWindow = createNewSlice(windowInfo.windowStart, windowInfo.windowEnd);
            (*windowToSlicesLocked)[windowInfo].slices.emplace_back(newWindow);
            (*windowToSlicesLocked)[windowInfo].windowState = WindowInfoState::BOTH_SIDES_FILLING;
            slicesWriteLocked->emplace_back(newWindow);
            workerVec.emplace_back(newWindow.get());
        }
    }
    return &workerVec;
}

std::vector<WindowInfo> StreamJoinOperatorHandlerBucketing::getAllWindowsForSlice(StreamSlice& slice) {
    // During bucketing one slice represents one window
    return {WindowInfo(slice.getSliceStart(), slice.getSliceEnd())};
}
}// namespace NES::Runtime::Execution::Operators