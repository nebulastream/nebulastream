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

#include <API/Schema.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinSlice.hpp>

namespace NES::Runtime::Execution
{
class OperatorHandler;

namespace Operators
{
class StreamJoinOperatorHandler;
}

/// This enum helps to keep track of the status of a window by classifying the stages of the join
/// The state transitions are as follows:
///                Current State |     Action                        | Next State
/// ----------------------------------------------------------------------------------------------
/// Start                        | Create Slice                      | BOTH_SIDES_FILLING
/// BOTH_SIDES_FILLING           | Global Watermark Ts > WindowEnd   | EMITTED_TO_PROBE
/// BOTH_SIDES_FILLING           | Left or Right Pipeline Terminated | ONCE_SEEN_DURING_TERMINATION
/// ONCE_SEEN_DURING_TERMINATION | Query/Pipeline Terminated         | EMITTED_TO_PROBE
/// EMITTED_TO_PROBE             | Tuples join in Probe              | CAN_BE_DELETED
enum class WindowInfoState : uint8_t
{
    BOTH_SIDES_FILLING,
    ONCE_SEEN_DURING_TERMINATION,
    EMITTED_TO_PROBE,
    CAN_BE_DELETED
};

/// This struct stores a slice ptr and the state. We require this information, as we have to know the state of a slice for a given window
struct SlicesAndState
{
    std::vector<std::shared_ptr<StreamJoinSlice>> windowSlices;
    WindowInfoState windowState;
};

/// This stores the left, right and output schema for a binary join
struct JoinSchema
{
    JoinSchema(
        const std::shared_ptr<Schema>& leftSchema, const std::shared_ptr<Schema>& rightSchema, const std::shared_ptr<Schema>& joinSchema)
        : leftSchema(leftSchema), rightSchema(rightSchema), joinSchema(joinSchema)
    {
    }

    const std::shared_ptr<Schema> leftSchema;
    const std::shared_ptr<Schema> rightSchema;
    const std::shared_ptr<Schema> joinSchema;
};

/// Stores the window start and window end
struct WindowMetaData
{
    WindowMetaData(const std::string& windowStartFieldName, const std::string& windowEndFieldName)
        : windowStartFieldName(windowStartFieldName), windowEndFieldName(windowEndFieldName)
    {
    }

    const std::string windowStartFieldName;
    const std::string windowEndFieldName;
};

/// Stores the information of a window. The start, end, and the identifier
struct WindowInfo
{
    WindowInfo(uint64_t windowStart, uint64_t windowEnd);
    std::strong_ordering operator<=>(const WindowInfo& other) const = default;
    Timestamp windowStart;
    Timestamp windowEnd;
};

namespace Util
{
/// Creates the join schema from the left and right schema
SchemaPtr createJoinSchema(const SchemaPtr& leftSchema, const SchemaPtr& rightSchema);
}
}

template <>
struct std::hash<NES::Runtime::Execution::WindowInfo>
{
    std::size_t operator()(const NES::Runtime::Execution::WindowInfo& s) const noexcept
    {
        std::size_t h1 = std::hash<NES::Timestamp>{}(s.windowEnd);
        std::size_t h2 = std::hash<NES::Timestamp>{}(s.windowStart);
        return h1 ^ (h2 << 1);
    }
};
