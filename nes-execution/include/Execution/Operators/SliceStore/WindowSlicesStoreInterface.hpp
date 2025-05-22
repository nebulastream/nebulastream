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

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <vector>
#include <Execution/Operators/SliceStore/Slice.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Execution.hpp>

namespace NES::Runtime::Execution
{


/// This struct stores a slice and the window info
struct SlicesAndWindowInfo
{
    std::vector<std::shared_ptr<Slice>> windowSlices;
    WindowInfo windowInfo;
};

struct WindowInfoAndSequenceNumber
{
    WindowInfo windowInfo;
    SequenceNumber sequenceNumber;

    bool operator<(const WindowInfoAndSequenceNumber& other) const { return windowInfo < other.windowInfo; }
};

/// This is the interface for storing windows and slices in a window-based operator
/// It provides an interface to operate on slices and windows for a time-based window operator, e.g., join or aggregation
class WindowSlicesStoreInterface
{
public:
    virtual ~WindowSlicesStoreInterface() = default;
    /// Retrieves the slices that corresponds to the timestamp. If no slices exist for the timestamp, they are created by calling the method createNewSlice
    virtual std::vector<std::shared_ptr<Slice>>
    getSlicesOrCreate(Timestamp timestamp, const std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>& createNewSlice)
        = 0;

    /// Retrieves all slices that can be triggered by the given global watermark
    /// This method returns all slices for each window that can be triggered. It returns the slices for all windows that have been filled and have a window end smaller than the global watermark
    /// Additionally, it returns a sequence number per window that is incremented for each window and thus, it can be used to set it in the emitted tuple buffer for the probe operator.
    virtual std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>> getTriggerableWindowSlices(Timestamp globalWatermark)
        = 0;

    /// Retrieves the slice by its end timestamp. If no slice exists for the given slice end, the optional return value is nullopt
    virtual std::optional<std::shared_ptr<Slice>> getSliceBySliceEnd(SliceEnd sliceEnd) = 0;

    /// Retrieves all current non-deleted slices that have not been triggered yet
    /// This method returns for each window all slices that have not been triggered yet, regardless of any watermark timestamp
    /// Additionally, it returns a sequence number per window that is incremented for each window and thus, it can be used to set it in the emitted tuple buffer for the probe operator.
    virtual std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>> getAllNonTriggeredSlices() = 0;

    /// Garbage collect all slices and windows that are not valid anymore
    /// It is open for the implementation to delete the slices in this call or to mark them for deletion
    /// There is no guarantee that the slices are deleted after this call
    virtual void garbageCollectSlicesAndWindows(Timestamp newGlobalWaterMark) = 0;

    /// Deletes all slices, directly in this call
    virtual void deleteState() = 0;

    /// Returns the window size
    [[nodiscard]] virtual uint64_t getWindowSize() const = 0;
};
}
