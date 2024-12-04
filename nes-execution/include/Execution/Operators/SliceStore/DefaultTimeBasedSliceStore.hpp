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
#include <Execution/Operators/SliceStore/WindowSlicesStoreInterface.hpp>
#include <folly/Synchronized.h>

#include <Execution/Operators/SliceStore/Slice.hpp>
#include <Execution/Operators/SliceStore/SliceAssigner.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Execution.hpp>

namespace NES::Runtime::Execution
{


/// This struct stores a slice ptr and the state. We require this information, as we have to know the state of a slice for a given window
struct SlicesAndState
{
    std::vector<std::shared_ptr<Slice>> windowSlices;
    WindowInfoState windowState;
};

class DefaultTimeBasedSliceStore final : public WindowSlicesStoreInterface
{
public:
    DefaultTimeBasedSliceStore(uint64_t windowSize, uint64_t windowSlide, uint8_t numberOfInputOrigins);
    DefaultTimeBasedSliceStore(const DefaultTimeBasedSliceStore& other);
    DefaultTimeBasedSliceStore(DefaultTimeBasedSliceStore&& other) noexcept;
    DefaultTimeBasedSliceStore& operator=(const DefaultTimeBasedSliceStore& other);
    DefaultTimeBasedSliceStore& operator=(DefaultTimeBasedSliceStore&& other) noexcept;

    ~DefaultTimeBasedSliceStore() override;
    std::vector<std::shared_ptr<Slice>> getSlicesOrCreate(
        Timestamp timestamp, const std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>& createNewSlice) override;
    std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>
    getTriggerableWindowSlices(Timestamp globalWatermark) override;
    std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>> getAllNonTriggeredSlices() override;
    std::optional<std::shared_ptr<Slice>> getSliceBySliceEnd(SliceEnd sliceEnd) override;
    void garbageCollectSlicesAndWindows(Timestamp newGlobalWaterMark) override;
    void deleteState() override;
    uint64_t getWindowSize() const override;

private:
    /// Retrieves all window identifiers that correspond to this slice
    std::vector<WindowInfo> getAllWindowInfosForSlice(const Slice& slice) const;


    /// We need to store the windows and slices in two separate maps. This is necessary as we need to access the slices during the join build phase,
    /// while we need to access windows during the triggering of windows.
    folly::Synchronized<std::map<WindowInfo, SlicesAndState>> windows;
    folly::Synchronized<std::map<SliceEnd, std::shared_ptr<Slice>>> slices;
    Operators::SliceAssigner sliceAssigner;

    /// We need to store the sequence number for the triggerable window infos. This is necessary, as we have to ensure that the sequence number is unique
    /// and increases for each window info.
    std::atomic<SequenceNumber::Underlying> sequenceNumber;

    /// Depending, if we have one or two input origins, we have to treat the slices differently
    /// For example, in getAllNonTriggeredSlices(), we have to wait until both origins have called this method to ensure correctness
    uint8_t numberOfInputOrigins;
};

}
