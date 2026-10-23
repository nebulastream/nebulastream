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

#include <SinksParsing/BufferIterator.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <Runtime/TupleBuffer.hpp>

namespace NES
{

std::optional<BufferIterator::BufferElement> BufferIterator::getNextElement()
{
    /// Return nothing if all buffers were returned already.
    if (bufferIndex > tupleBuffer.getNumberOfChildBuffers())
    {
        return std::nullopt;
    }

    /// The very first buffer to be returned as element is the "main" tuple buffer
    if (bufferIndex == 0)
    {
        const uint64_t bytesInBuffer = tupleBuffer.getNumberOfTuples();
        ++bufferIndex;
        return std::optional<BufferElement>({.buffer = tupleBuffer, .contentLength = bytesInBuffer});
    }

    /// Iterate through the children
    /// Child buffers store the number of written bytes in them as the number of tuples
    const TupleBuffer childBuffer = tupleBuffer.loadChildBuffer(ChildBufferIndex(bufferIndex - 1));
    ++bufferIndex;
    return std::optional<BufferElement>({.buffer = childBuffer, .contentLength = childBuffer.getNumberOfTuples()});
}

size_t getTotalContentLength(const TupleBuffer& buffer)
{
    size_t total = buffer.getNumberOfTuples();
    for (uint32_t childIndex = 0; childIndex < buffer.getNumberOfChildBuffers(); ++childIndex)
    {
        total += buffer.loadChildBuffer(ChildBufferIndex(childIndex)).getNumberOfTuples();
    }
    return total;
}

size_t copyInto(const TupleBuffer& buffer, std::span<std::byte> destination)
{
    size_t written = 0;
    BufferIterator iterator{buffer};
    for (auto element = iterator.getNextElement(); element.has_value() && written < destination.size(); element = iterator.getNextElement())
    {
        const auto content = std::as_bytes(element->buffer.getAvailableMemoryArea<char>().first(element->contentLength));
        const auto source = content.first(std::min(content.size(), destination.size() - written));
        std::ranges::copy(source, destination.subspan(written).begin());
        written += source.size();
    }
    return written;
}
}
