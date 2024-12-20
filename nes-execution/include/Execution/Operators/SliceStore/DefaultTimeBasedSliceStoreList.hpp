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
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <vector>
#include <Execution/Operators/SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <Execution/Operators/SliceStore/WindowSlicesStoreInterface.hpp>
#include <folly/Synchronized.h>

#include <Execution/Operators/SliceStore/Slice.hpp>
#include <Execution/Operators/SliceStore/SliceAssigner.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Execution.hpp>

namespace NES::Runtime::Execution
{

class DefaultTimeBasedSliceStoreList final : public WindowSlicesStoreInterface
{
public:
    DefaultTimeBasedSliceStoreList(Operators::SliceAssigner sliceAssigner, uint8_t numberOfInputOrigins);
    DefaultTimeBasedSliceStoreList(const DefaultTimeBasedSliceStoreList& other);
    DefaultTimeBasedSliceStoreList(DefaultTimeBasedSliceStoreList&& other) noexcept;
    DefaultTimeBasedSliceStoreList& operator=(const DefaultTimeBasedSliceStoreList& other);
    DefaultTimeBasedSliceStoreList& operator=(DefaultTimeBasedSliceStoreList&& other) noexcept;

    ~DefaultTimeBasedSliceStoreList() override;
    std::vector<std::shared_ptr<Slice>> getSlicesOrCreate(
        Timestamp timestamp, const std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>& createNewSlice) override;
    std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>
    getTriggerableWindowSlices(Timestamp globalWatermark) override;
    std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>> getAllNonTriggeredSlices() override;
    std::optional<std::shared_ptr<Slice>> getSliceBySliceEnd(SliceEnd sliceEnd) override;
    std::vector<SliceEnd> garbageCollectSlicesAndWindows(Timestamp newGlobalWaterMark) override;
    void deleteState() override;
    uint64_t getWindowSize() const override;

private:
    /// Retrieves all window identifiers that correspond to this slice
    std::vector<WindowInfo> getAllWindowInfosForSlice(const Slice& slice) const;


    /// We need to store the windows and slices in two separate maps. This is necessary as we need to access the slices during the join build phase,
    /// while we need to access windows during the triggering of windows.
    folly::Synchronized<std::map<WindowInfo, SlicesAndState>> windows;
    folly::Synchronized<std::list<std::shared_ptr<Slice>>> slices;
    Operators::SliceAssigner sliceAssigner;

    /// We need to store the sequence number for the triggerable window infos. This is necessary, as we have to ensure that the sequence number is unique
    /// and increases for each window info.
    std::atomic<SequenceNumber::Underlying> sequenceNumber;

    /// Depending, if we have one or two input origins, we have to treat the slices differently
    /// For example, in getAllNonTriggeredSlices(), we have to wait until both origins have called this method to ensure correctness
    uint8_t numberOfInputOrigins;
};

}
