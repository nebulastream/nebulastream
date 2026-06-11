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

#include <Sources/RawSource.hpp>

#include <cstddef>
#include <span>
#include <stop_token>
#include <string_view>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
/// simdjson over-reads up to SIMDJSON_PADDING (64) bytes past the JSON content during SIMD lookahead.
/// Mirrored here rather than including <simdjson.h> to keep nes-sources independent of the formatter plugin.
constexpr std::size_t SIMDJSON_TAIL_PADDING = 64;
}

std::size_t RawSource::requiredTailPaddingFor(std::string_view inputFormatterType) noexcept
{
    if (inputFormatterType == "JSON")
    {
        return SIMDJSON_TAIL_PADDING;
    }
    return 0;
}

RawSource::RawSource(std::size_t tailPadding) noexcept : tailPadding(tailPadding)
{
}

Source::FillTupleBufferResult RawSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    const auto bufferSize = tupleBuffer.getBufferSize();
    PRECONDITION(
        bufferSize > tailPadding, "TupleBuffer ({} B) smaller than required formatter tail padding ({} B)", bufferSize, tailPadding);
    auto fullArea = tupleBuffer.getAvailableMemoryArea<std::byte>();
    const std::span<std::byte> writable{fullArea.data(), bufferSize - tailPadding};
    return fillRaw(writable, stopToken);
}

}
