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

#include <OutputFormatters/OutputFormatter.hpp>

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>

namespace NES
{
void OutputFormatter::writeWithChildBuffers(
    std::string value,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider,
    int8_t* bufferStartingAddress)
{
    size_t remainingBytes = value.size();
    uint32_t numOfChildBuffers = tupleBuffer->getNumberOfChildBuffers();
    /// Fill up the remaing space in the main tuple buffer before allocating any child buffers
    if (numOfChildBuffers == 0)
    {
        std::memcpy(bufferStartingAddress, value.c_str(), remainingSpace);
        remainingBytes -= remainingSpace;
        /// Create the first child buffer, if necessary
        if (remainingBytes > 0)
        {
            auto newChildBuffer = bufferProvider->getBufferBlocking();
            auto index = tupleBuffer->storeChildBuffer(newChildBuffer);
            numOfChildBuffers++;
        }
    }
    while (remainingBytes > 0)
    {
        /// Write as many bytes in the latest child buffer as possible and allocate a new one if space does not suffice
        const VariableSizedAccess::Index childIndex(numOfChildBuffers-1);
        auto lastChildBuffer = tupleBuffer->loadChildBuffer(childIndex);
        const auto bufferOffset = lastChildBuffer.getNumberOfTuples();
        const uint32_t valueOffset = value.size() - remainingBytes;
        const uint64_t writable = std::min(remainingBytes, lastChildBuffer.getBufferSize() - bufferOffset);
        std::memcpy(lastChildBuffer.getAvailableMemoryArea<>().data() + bufferOffset, value.c_str() + valueOffset, writable);
        remainingBytes -= writable;
        lastChildBuffer.setNumberOfTuples(bufferOffset + writable);
        if (remainingBytes > 0)
        {
            auto newChildBuffer = bufferProvider->getBufferBlocking();
            auto index = tupleBuffer->storeChildBuffer(newChildBuffer);
            numOfChildBuffers++;
        }
    }
}

std::ostream& operator<<(std::ostream& os, const OutputFormatter& obj)
{
    return obj.toString(os);
}
}
