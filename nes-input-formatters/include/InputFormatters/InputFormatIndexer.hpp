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

#include <concepts>
#include <cstddef>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES::InputFormatters
{

/// Restricts the MetaData that an InputFormatIndexer receives from the InputFormatterTask
template <typename T>
concept IndexerMetaData = requires(ParserConfig config, Schema schema, T metaData, std::ostream& spanningTuple) {
    T(config, schema);
    /// Assumes a fixed set of symbols that separate tuples
    /// InputFormatIndexers without tuple delimiters should return an empty string
    { metaData.getTupleDelimitingBytes() } -> std::same_as<std::string_view>;
};

/// Implements format-specific (CSV, JSON, Protobuf, etc.) indexing of raw buffers.
/// The InputFormatIndexerTask uses the InputFormatIndexer to determine byte offsets of all fields of a given tuple and all tuples of a given buffer.
/// The offsets allow the InputFormatIndexerTask to parse only the fields that it needs to for the particular query.
/// @Note All InputFormatIndexer implementations must be thread-safe. NebulaStream's query engine concurrently executes InputFormatIndexerTasks.
///       Thus, the InputFormatIndexerTask calls the interface functions of the InputFormatIndexer concurrently.
template <typename FieldAccessFunctionType, IndexerMetaData MetaData, bool RequiresFormatting>
class InputFormatIndexer
{
public:
    static constexpr bool IsFormattingRequired = RequiresFormatting;
    InputFormatIndexer() = default;
    virtual ~InputFormatIndexer() = default;

    InputFormatIndexer(const InputFormatIndexer&) = default;
    InputFormatIndexer& operator=(const InputFormatIndexer&) = delete;
    InputFormatIndexer(InputFormatIndexer&&) = delete;
    InputFormatIndexer& operator=(InputFormatIndexer&&) = delete;

    /// Determines the byte-offsets to all fields in one buffer.
    /// Must write all offsets to the correct position to the fieldOffsets pointer.
    /// @return the bytes-offset to the first and last tuple delimiter (we use these offsets to construct tuples that span buffers (see SequenceShredder))
    /// @Note Must be thread-safe (see description of class)
    virtual void setupFieldAccessFunctionForBuffer(
        FieldAccessFunctionType& fieldAccessFunction, const RawTupleBuffer& rawBuffer, const MetaData& tupleMetadata) const
        = 0;

    friend std::ostream& operator<<(std::ostream& os, const InputFormatIndexer& inputFormatIndexer)
    {
        return inputFormatIndexer.toString(os);
    }

protected:
    virtual std::ostream& toString(std::ostream& os) const = 0;
};

}
