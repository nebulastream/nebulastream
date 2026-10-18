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
#include <cstdint>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES
{
/// Deep-copies a TupleBuffer: copies the raw bytes, replicates all metadata, and recursively copies every child buffer into freshly allocated buffers obtained from `provider`.
TupleBuffer deepCopyBuffer(const TupleBuffer& buffer, AbstractBufferProvider& provider);

/// Returns a buffer for the specified size and optimizes internally for either pooled or unpooled.
/// TODO #1582: This logic will be refactored into the Buffer Provider
TupleBuffer getBuffer(uint64_t size, AbstractBufferProvider& provider);
}

/**
 * @brief Computes aligned buffer size based on original buffer size and alignment
 */
constexpr size_t alignBufferSize(const size_t bufferSize, const uint32_t withAlignment)
{
    if ((bufferSize % withAlignment) != 0U)
    {
        /// make sure that each buffer is a multiple of the alignment
        return bufferSize + (withAlignment - bufferSize % withAlignment);
    }
    return bufferSize;
}
