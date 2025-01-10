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

#include <algorithm>
#include <cstddef>
#include <span>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <MemoryTestUtils.hpp>

namespace NES::Testing
{
Memory::TupleBuffer copyBuffer(const Memory::TupleBuffer& buffer, Memory::AbstractBufferProvider& provider)
{
    auto copiedBuffer = provider.getBufferBlocking();
    PRECONDITION(copiedBuffer.getBufferSize() >= buffer.getBufferSize(), "Attempt to copy buffer into smaller buffer");
    auto bufferData = std::span(buffer.getBuffer(), buffer.getBufferSize());
    std::ranges::copy(bufferData, copiedBuffer.getBuffer());
    copiedBuffer.setWatermark(buffer.getWatermark());
    copiedBuffer.setChunkNumber(buffer.getChunkNumber());
    copiedBuffer.setSequenceNumber(buffer.getSequenceNumber());
    copiedBuffer.setCreationTimestampInMS(buffer.getCreationTimestampInMS());
    copiedBuffer.setLastChunk(buffer.isLastChunk());
    copiedBuffer.setOriginId(buffer.getOriginId());
    copiedBuffer.setSequenceNumber(buffer.getSequenceNumber());
    copiedBuffer.setChunkNumber(buffer.getChunkNumber());
    copiedBuffer.setLastChunk(buffer.isLastChunk());
    copiedBuffer.setNumberOfTuples(buffer.getNumberOfTuples());

    for (size_t childIdx = 0; childIdx < buffer.getNumberOfChildrenBuffer(); ++childIdx)
    {
        auto childBuffer = buffer.loadChildBuffer(childIdx);
        auto copiedChildBuffer = copyBuffer(childBuffer, provider);
        INVARIANT(copiedBuffer.storeChildBuffer(copiedChildBuffer) == childIdx, "Child buffer index does not match");
    }

    return copiedBuffer;
}
}
