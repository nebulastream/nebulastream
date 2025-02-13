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

#include <InputFormatters/InputFormatterTask.hpp>

namespace NES::InputFormatters
{
/// Implements format-specific (CSV, JSON, Protobuf, etc.) indexing of raw buffers.
/// The InputFormatterTask uses the InputFormatter to determine the indexes of all full tuples in a raw buffer
/// and to determine the indexes of spanning tuples.
/// @Note All InputFormatter implementations must be thread-safe. NebulaStream's task queue concurrently executes InputFormatterTasks.
///       Thus, the InputFormatterTask calls the interface functions of the InputFormatter concurrently.
///       The most straight-forward way to comply is to implement the input formatter stateless, i.e., each call to its interface functions
///       starts from a clean state.
class InputFormatter
{
public:
    struct BufferOffsets
    {
        FieldOffsetsType offsetOfFirstTupleDelimiter;
        FieldOffsetsType offsetOfLastTupleDelimiter;
    };
    InputFormatter() = default;
    virtual ~InputFormatter() = default;

    InputFormatter(const InputFormatter&) = default;
    InputFormatter& operator=(const InputFormatter&) = delete;
    InputFormatter(InputFormatter&&) = delete;
    InputFormatter& operator=(InputFormatter&&) = delete;

    /// Determines all indexes of a spanning tuple (a tuple that spans at least two raw buffers).
    /// Must write all indexes to the correct position to the fieldOffsets pointer.
    /// @Note Must be thread-safe (see description of class)
    virtual void indexSpanningTuple(
        std::string_view tuple,
        std::string_view fieldDelimiter,
        FieldOffsetsType* fieldOffsets,
        FieldOffsetsType startIdxOfCurrentTuple,
        FieldOffsetsType endIdxOfCurrentTuple,
        FieldOffsetsType currentFieldIndex)
        = 0;

    /// Determines all indexes of all full tuples in a raw buffer. A raw buffer may start and end with a partial tuple.
    /// Must write all indexes to the fieldOffsets pointer.
    /// @Note Must be thread-safe (see description of class)
    virtual BufferOffsets indexRawBuffer(
        std::string_view bufferView,
        FieldOffsets& fieldOffsets,
        std::string_view tupleDelimiter,
        std::string_view fieldDelimiter)
        = 0;

    friend std::ostream& operator<<(std::ostream& os, const InputFormatter& inputFormatter) { return inputFormatter.toString(os); }

protected:
    virtual std::ostream& toString(std::ostream& os) const = 0;
};
}
