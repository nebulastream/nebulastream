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

namespace NES
{
using SliceStart = Timestamp;
using SliceEnd = Timestamp;

struct CreateNewSlicesArguments
{
    virtual ~CreateNewSlicesArguments() = default;
};

/// This enum helps to keep track of the status of a window by classifying the stages of the join
/// The state transitions are as follows:
///                Current State |     Action                        | Next State
/// ----------------------------------------------------------------------------------------------
/// Start                        | Create Slice                      | WINDOW_FILLING
/// WINDOW_FILLING               | Global Watermark Ts > WindowEnd   | EMITTED_TO_PROBE
/// WINDOW_FILLING               | Left or Right Pipeline Terminated | WAITING_ON_TERMINATION
/// WAITING_ON_TERMINATION       | Query/Pipeline Terminated         | EMITTED_TO_PROBE
/// EMITTED_TO_PROBE             | Tuples join in Probe              | CAN_BE_DELETED
enum class WindowInfoState : uint8_t
{
    WINDOW_FILLING,
    WAITING_ON_TERMINATION,
    EMITTED_TO_PROBE
};

/// This class represents a single slice
class Slice
{
public:
    Slice(SliceStart sliceStart, SliceEnd sliceEnd);
    Slice(const Slice& other);
    Slice(Slice&& other) noexcept;
    Slice& operator=(const Slice& other);
    Slice& operator=(Slice&& other) noexcept;
    virtual ~Slice() = default;

    [[nodiscard]] SliceStart getSliceStart() const;
    [[nodiscard]] SliceEnd getSliceEnd() const;

    bool operator==(const Slice& rhs) const;
    bool operator!=(const Slice& rhs) const;

protected:
    SliceStart sliceStart;
    SliceEnd sliceEnd;
};
}
