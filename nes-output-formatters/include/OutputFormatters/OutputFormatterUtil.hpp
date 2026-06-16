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
#include <utility>
#include <magic_enum/magic_enum.hpp>

#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputParser.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <OutputParserRegistry.hpp>

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

/// Config parameters for output parsers
struct OutputParserConfig
{
    bool quoted;
};

/// Overrides the parser types for the datatypes based on the input of the string. The function expects the string to be formatted like this:
/// [TYPENAME]:[PARSERTYPE],...
inline void parseOutputParserOverrides(const std::string& overrides, std::unordered_map<DataType::Type, std::string>& parsersMap)
{
    size_t typeNameStart = 0;
    size_t typeNameEnd = overrides.find(':', typeNameStart);
    while (typeNameEnd != std::string::npos)
    {
        const std::string typeName = overrides.substr(typeNameStart, typeNameEnd - typeNameStart);
        const size_t parserTypeStart = typeNameEnd + 1;
        const size_t parserTypeEnd = std::min(overrides.size(), overrides.find(',', parserTypeStart));
        const std::string parserType = overrides.substr(parserTypeStart, parserTypeEnd - parserTypeStart);

        if (std::optional<DataType::Type> dataType = magic_enum::enum_cast<DataType::Type>(typeName))
        {
            parsersMap[dataType.value()] = parserType;
        }
        typeNameStart = parserTypeEnd + 1;
        typeNameEnd = overrides.find(':', typeNameStart);
    }
}

/// Fetches OutputParser from Registry
inline std::unique_ptr<OutputParser> provideOutputParser(const std::string& parserType, const OutputParserConfig& config)
{
    const OutputParserRegistryArguments arguments{.quoted = config.quoted};
    if (auto parser = OutputParserRegistry::instance().create(parserType, arguments))
    {
        return std::move(parser.value());
    }
    throw UnknownOutputParserType("Unknown Output Parser: {}", parserType);
}
}
