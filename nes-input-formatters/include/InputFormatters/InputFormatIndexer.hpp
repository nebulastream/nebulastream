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

#include <cstddef>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>

#include <RawBufferIndex.hpp>

#include <RawValueParser.hpp>

namespace NES
{

/// Implements format-specific (CSV, JSON, XML, etc.) indexing of raw buffers.
/// The InputFormatIndexerTask uses the InputFormatIndexer to determine byte offsets of all fields of a given tuple and all tuples of a given buffer.
/// The offsets allow the InputFormatIndexerTask to parse only the fields that it needs to for the particular query.
/// @Note All InputFormatIndexer implementations must be thread-safe. NebulaStream's query engine concurrently executes InputFormatIndexerTasks.
///       Thus, the InputFormatIndexerTask calls the interface functions of the InputFormatIndexer concurrently.
class InputFormatIndexer
{
public:
    /// `tailPadding` is the number of bytes this indexer may over-read past the logical end of its input as part
    /// of SIMD lookahead. Callers that own the input memory must guarantee at least this many addressable bytes
    /// past the content end. Defaults to 0 for indexers with no over-read; SIMD-based indexers (e.g. simdjson)
    /// pass their padding constant here.
    explicit InputFormatIndexer(std::size_t tailPadding = 0) noexcept : tailPaddingBytes(tailPadding) { }
    virtual ~InputFormatIndexer() = default;

    [[nodiscard]] virtual std::unique_ptr<RawBufferIndex> indexRawBuffer(std::string_view rawBuffer) const = 0;

    /// See ctor doc. Cached on the instance — no virtual call.
    [[nodiscard]] std::size_t requiredTailPadding() const noexcept { return tailPaddingBytes; }

    [[nodiscard]] virtual std::string_view getTupleDelimitingBytes() const = 0;
    [[nodiscard]] virtual std::string_view getFieldDelimitingBytes() const = 0;
    [[nodiscard]] virtual QuotationType getQuotationType() const = 0;
    [[nodiscard]] virtual const std::vector<std::string>& getNullValues() const = 0;

    friend std::ostream& operator<<(std::ostream& out, const InputFormatIndexer& indexer);

protected:
    /// Implemented by children of InputFormatIndexer. Called by '<<'. Allows to use '<<' on abstract InputFormatIndexer.
    [[nodiscard]] virtual std::ostream& toString(std::ostream& str) const = 0;

private:
    std::size_t tailPaddingBytes;
};
}
