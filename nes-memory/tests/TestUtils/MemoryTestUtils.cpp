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

#include <MemoryTestUtils.hpp>

#include <algorithm>
#include <cstddef>
#include <span>
#include <MemoryLayout/VariableSizedAccess.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>

namespace NES::Testing
{
TupleBuffer copyBuffer(const TupleBuffer& buffer, AbstractBufferProvider& provider)
{
    auto copiedBuffer = provider.getBufferBlocking();
    PRECONDITION(
        copiedBuffer.getBufferSize() >= buffer.getBufferSize(),
        "Attempt to copy buffer of size: {} into smaller buffer of size: {}",
        copiedBuffer.getBufferSize(),
        buffer.getBufferSize());
    auto bufferData = buffer.getAvailableMemoryArea<std::byte>();
    std::ranges::copy(bufferData, copiedBuffer.getAvailableMemoryArea().begin());
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

    for (size_t childIdx = 0; childIdx < buffer.getNumberOfChildBuffers(); ++childIdx)
    {
        auto childBuffer = buffer.loadChildBuffer(childIdx);
        auto copiedChildBuffer = copyBuffer(childBuffer, provider);
        auto ret = copiedBuffer.storeChildBuffer(copiedChildBuffer);
        INVARIANT(
            ret == childIdx,
            "Child buffer index: {}, does not match index: {}",
            childIdx,
            copiedBuffer.storeChildBuffer(copiedChildBuffer));
    }

    return copiedBuffer;
}
}
