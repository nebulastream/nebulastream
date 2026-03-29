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


#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>

#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputParser.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{
/// Write the string completely into the tuple buffer.
/// Child buffers may be allocated if it does not fit completely into the main memory of the tuple buffer.
/// String may span between children or between the main buffer and the first child.
/// RemainingSpace tells the function the amount of space that is left in the main buffer.
/// Will return the amount of bytes written in the main memory of the buffer
inline uint64_t writeValueToBuffer(
    const char* value,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider,
    int8_t* bufferStartingAddress)
{
    const std::string_view valueString{value};
    size_t remainingBytes = valueString.size();
    uint32_t numOfChildBuffers = tupleBuffer->getNumberOfChildBuffers();
    uint64_t writtenToMainMemory = 0;
    /// Fill up the remaing space in the main tuple buffer before allocating any child buffers
    if (numOfChildBuffers == 0)
    {
        const size_t fitsInMainBuffer = std::min(remainingBytes, remainingSpace);
        writtenToMainMemory += fitsInMainBuffer;
        std::memcpy(bufferStartingAddress, valueString.data(), fitsInMainBuffer);
        remainingBytes -= fitsInMainBuffer;
        /// Create the first child buffer, if necessary
        if (remainingBytes > 0)
        {
            auto newChildBuffer = bufferProvider->getBufferBlocking();
            (void)tupleBuffer->storeChildBuffer(newChildBuffer);
            ++numOfChildBuffers;
        }
    }
    while (remainingBytes > 0)
    {
        /// Write as many bytes in the latest child buffer as possible and allocate a new one if space does not suffice
        const VariableSizedAccess::Index childIndex{numOfChildBuffers - 1};
        auto lastChildBuffer = tupleBuffer->loadChildBuffer(childIndex);
        const auto bufferOffset = lastChildBuffer.getNumberOfTuples();
        const uint32_t valueOffset = valueString.size() - remainingBytes;
        const uint64_t writable = std::min(remainingBytes, lastChildBuffer.getBufferSize() - bufferOffset);
        std::memcpy(lastChildBuffer.getAvailableMemoryArea<>().data() + bufferOffset, valueString.data() + valueOffset, writable);
        remainingBytes -= writable;
        lastChildBuffer.setNumberOfTuples(bufferOffset + writable);
        if (remainingBytes > 0)
        {
            auto newChildBuffer = bufferProvider->getBufferBlocking();
            (void)tupleBuffer->storeChildBuffer(newChildBuffer);
            ++numOfChildBuffers;
        }
    }
    return writtenToMainMemory;
}

/// Fetches OutputParser from Registry
std::unique_ptr<OutputParser> provideOutputParser(const std::string& parserType);
}
