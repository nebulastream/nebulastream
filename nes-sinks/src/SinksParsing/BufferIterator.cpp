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

#include <cstdint>
#include <optional>
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
        return std::optional<BufferElement>({.buffer = tupleBuffer.share(), .contentLength = bytesInBuffer});
    }

    /// Iterate through the children
    /// Child buffers store the number of written bytes in them as the number of tuples
    TupleBuffer childBuffer = tupleBuffer.loadChildBuffer(ChildBufferIndex(bufferIndex - 1));
    ++bufferIndex;
    const auto contentLength = childBuffer.getNumberOfTuples();
    return std::optional<BufferElement>({.buffer = std::move(childBuffer), .contentLength = contentLength});
}
}
