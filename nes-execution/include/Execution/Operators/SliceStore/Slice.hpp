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
#include <Time/Timestamp.hpp>

namespace NES::Runtime
{
using SliceStart = Timestamp;
using SliceEnd = Timestamp;
}

namespace NES::Runtime::Execution
{


/// This enum helps to keep track of the status of a window by classifying the stages of the join
/// The state transitions are as follows:
///                Current State |     Action                        | Next State
/// ----------------------------------------------------------------------------------------------
/// Start                        | Create Slice                      | WINDOW_FILLING
/// WINDOW_FILLING               | Global Watermark Ts > WindowEnd   | EMITTED_TO_PROBE
/// WINDOW_FILLING               | Left or Right Pipeline Terminated | ONCE_SEEN_DURING_TERMINATION
/// ONCE_SEEN_DURING_TERMINATION | Query/Pipeline Terminated         | EMITTED_TO_PROBE
/// EMITTED_TO_PROBE             | Tuples join in Probe              | CAN_BE_DELETED
enum class WindowInfoState : uint8_t
{
    WINDOW_FILLING,
    ONCE_SEEN_DURING_TERMINATION,
    EMITTED_TO_PROBE
};

/// Stores the window start and window end field names
struct WindowMetaData
{
    WindowMetaData(std::string windowStartFieldName, std::string windowEndFieldName)
        : windowStartFieldName(std::move(windowStartFieldName)), windowEndFieldName(std::move(windowEndFieldName))
    {
    }

    const std::string windowStartFieldName;
    const std::string windowEndFieldName;
};

/// Stores the information of a window. The start, end, and the identifier
struct WindowInfo
{
    WindowInfo(const Timestamp windowStart, const Timestamp windowEnd) : windowStart(windowStart), windowEnd(windowEnd)
    {
        if (windowEnd < windowStart)
        {
            this->windowStart = Timestamp(0);
        }
    }

    WindowInfo(const uint64_t windowStart, const uint64_t windowEnd) : windowStart(windowStart), windowEnd(windowEnd)
    {
        if (windowEnd < windowStart)
        {
            this->windowStart = Timestamp(0);
        }
    }

    bool operator<(const WindowInfo& other) const { return windowEnd < other.windowEnd; }
    Timestamp windowStart;
    Timestamp windowEnd;
};


/// This class represents a single slice
class Slice
{
public:
    Slice(SliceStart sliceStart, SliceEnd sliceEnd);
    virtual ~Slice() = default;

    [[nodiscard]] SliceStart getSliceStart() const;
    [[nodiscard]] SliceEnd getSliceEnd() const;

    [[nodiscard]] virtual uint64_t getNumberOfTuplesLeft() = 0;
    [[nodiscard]] virtual uint64_t getNumberOfTuplesRight() = 0;

    bool operator==(const Slice& rhs) const;
    bool operator!=(const Slice& rhs) const;

protected:
    const SliceStart sliceStart;
    const SliceEnd sliceEnd;
};
}
