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

#include <ostream>
#include <string_view>

#include <InputFormatters/InputFormatterTask.hpp>

namespace NES::InputFormatters
{
/// Implements format-specific (CSV, JSON, Protobuf, etc.) indexing of raw buffers.
/// The InputFormatterTask uses the InputFormatter to determine byte offsets of all fields of a given tuple and all tuples of a given buffer.
/// The offsets allow the InputFormatterTask to parse only the fields that it needs to for the particular query.
/// @Note All InputFormatter implementations must be thread-safe. NebulaStream's query engine concurrently executes InputFormatterTasks.
///       Thus, the InputFormatterTask calls the interface functions of the InputFormatter concurrently.
class InputFormatter
{
public:
    struct FirstAndLastTupleDelimiterOffsets
    {
        /// If the offsetOfFirstTupleDelimiter is >= size of the raw buffer, the InputFormatterTask assumes there is no tuple delimiter in the buffer
        FieldOffsetsType offsetOfFirstTupleDelimiter;
        FieldOffsetsType offsetOfLastTupleDelimiter;
    };
    InputFormatter() = default;
    virtual ~InputFormatter() = default;

    InputFormatter(const InputFormatter&) = default;
    InputFormatter& operator=(const InputFormatter&) = delete;
    InputFormatter(InputFormatter&&) = delete;
    InputFormatter& operator=(InputFormatter&&) = delete;

    /// Determines the byte-offsets to all fields in one tuple, including the offset to the first byte that is not part of the tuple.
    /// Must write all offsets to the correct position to the fieldOffsets pointer.
    /// @Note Must be thread-safe (see description of class)
    virtual void indexTuple(std::string_view tuple, FieldOffsetsType* fieldOffsets, FieldOffsetsType startIdxOfTuple) const = 0;

    /// Determines the byte-offsets to all fields in one buffer.
    /// Must write all offsets to the correct position to the fieldOffsets pointer.
    /// @return the bytes-offset to the first and last tuple delimiter (we use these offsets to construct tuples that span buffers (see SequenceShredder))
    /// @Note Must be thread-safe (see description of class)
    virtual FirstAndLastTupleDelimiterOffsets indexBuffer(std::string_view bufferView, FieldOffsets& fieldOffsets) const = 0;

    friend std::ostream& operator<<(std::ostream& os, const InputFormatter& inputFormatter) { return inputFormatter.toString(os); }

protected:
    virtual std::ostream& toString(std::ostream& os) const = 0;
};
}
