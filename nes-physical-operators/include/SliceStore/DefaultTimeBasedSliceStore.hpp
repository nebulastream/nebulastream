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

#pragma once
#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <vector>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <folly/Synchronized.h>

#include <Identifiers/Identifiers.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SliceAssigner.hpp>
#include <Time/Timestamp.hpp>

namespace NES
{


/// This struct stores a slice ptr and the state. We require this information, as we have to know the state of a slice for a given window
struct SlicesAndState
{
    explicit SlicesAndState(const uint64_t numberOfExpectedSlices) { windowSlices.reserve(numberOfExpectedSlices); }

    std::vector<std::shared_ptr<Slice>> windowSlices;
    WindowInfoState windowState{WindowInfoState::WINDOW_FILLING};
};

class DefaultTimeBasedSliceStore final : public WindowSlicesStoreInterface
{
public:
    DefaultTimeBasedSliceStore(uint64_t windowSize, uint64_t windowSlide);

    ~DefaultTimeBasedSliceStore() override;
    std::vector<std::shared_ptr<Slice>> getSlicesOrCreate(
        Timestamp timestamp, const std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>& createNewSlice) override;
    std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>
    getTriggerableWindowSlices(Timestamp globalWatermark) override;
    std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>> getAllNonTriggeredSlices() override;
    std::optional<std::shared_ptr<Slice>> getSliceBySliceEnd(SliceEnd sliceEnd) override;
    void garbageCollectSlicesAndWindows(Timestamp newGlobalWaterMark) override;
    void deleteState() override;
    void incrementNumberOfInputPipelines() override;
    uint64_t getWindowSize() const override;

private:
    /// We need to store the windows and slices in two separate maps. This is necessary as we need to access the slices during the join build phase,
    /// while we need to access windows during the triggering of windows.
    folly::Synchronized<std::map<WindowInfo, SlicesAndState>> windows;
    folly::Synchronized<std::map<SliceEnd, std::shared_ptr<Slice>>> slices;
    SliceAssigner sliceAssigner;

    /// We need to store the sequence number for the triggerable window infos. This is necessary, as we have to ensure that the sequence number is unique
    /// and increases for each window info.
    std::atomic<SequenceNumber::Underlying> sequenceNumber;

    /// If a window build operator appears in multiple pipelines, it may get terminated multiple times
    /// We need to track how many input pipelines have not terminated yet, to only release pending slices after the last termination
    std::atomic<uint64_t> numberOfActiveInputPipelines;
};

}
