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
#include <unordered_map>
#include <utility>
#include <vector>

#include <Identifiers/QualifiedIdentifier.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <OutputFormatters/ValueSerializer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <ValueSerializerRegistry.hpp>

namespace NES
{

/// Scratch space for composing one field's output before it is written into the record buffer.
/// Serializers run per field per record in both execution modes, so building a fresh std::string there
/// allocates on the hot path; clear() keeps the capacity, which makes the steady state allocation-free.
/// The buffer is only valid until the same thread serializes its next field.
inline std::string& serializationBuffer()
{
    thread_local std::string buffer;
    buffer.clear();
    return buffer;
}

/// Write the serialized value represented by valuePtr and valueSize completely into the buffer.
/// Child buffers may be allocated if it does not fit completely into the main memory of the tuple buffer.
/// String may span between children or between the main buffer and the first child.
/// RemainingSpace tells the function the amount of space that is left in the main buffer.
/// Will return the amount of bytes written in the main memory of the buffer
inline uint64_t writeValueToBuffer(
    const char* valuePtr,
    const size_t valueSize,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider,
    int8_t* bufferStartingAddress)
{
    const std::string_view value{valuePtr, valueSize};
    size_t remainingBytes = value.size();
    uint32_t numOfChildBuffers = tupleBuffer->getNumberOfChildBuffers();
    uint64_t writtenToMainMemory = 0;
    /// Fill up the remaing space in the main tuple buffer before allocating any child buffers
    if (numOfChildBuffers == 0)
    {
        const size_t fitsInMainBuffer = std::min(remainingBytes, remainingSpace);
        writtenToMainMemory += fitsInMainBuffer;
        std::memcpy(bufferStartingAddress, value.data(), fitsInMainBuffer);
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
        const ChildBufferIndex childIndex{numOfChildBuffers - 1};
        auto lastChildBuffer = tupleBuffer->loadChildBuffer(childIndex);
        const auto bufferOffset = lastChildBuffer.getNumberOfTuples();
        const uint32_t valueOffset = value.size() - remainingBytes;
        const uint64_t writable = std::min(remainingBytes, lastChildBuffer.getBufferSize() - bufferOffset);
        std::memcpy(lastChildBuffer.getAvailableMemoryArea<>().data() + bufferOffset, value.data() + valueOffset, writable);
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

/// Resolves the serializer that the user configured for individual fields against the fields of the output schema.
/// The function expects the overrides string to be formatted like this: [FIELD-NAME]:[SERIALIZER-KEY],...
/// Throws an InvalidConfigParameter for a malformed entry or an entry that names no field of the output schema.
[[nodiscard]] inline std::unordered_map<Record::RecordFieldIdentifier, std::string>
parseValueSerializerOverrides(const std::string& overrides, const std::vector<Record::RecordFieldIdentifier>& fieldNames)
{
    std::unordered_map<Record::RecordFieldIdentifier, std::string> serializerTypes;
    for (const auto& entry : splitOnMultipleDelimiters(overrides, {','}))
    {
        const auto separator = entry.find(':');
        if (separator == std::string_view::npos)
        {
            throw InvalidConfigParameter(
                "VALUE_SERIALIZERS entry '{}' is not of the form [FIELD-NAME]:[SERIALIZER-KEY]", escapeSpecialCharacters(entry));
        }
        const auto configuredName = QualifiedIdentifier::tryParse(trimWhiteSpaces(entry.substr(0, separator)));
        if (not configuredName.has_value())
        {
            throw InvalidConfigParameter(
                "VALUE_SERIALIZERS entry '{}' does not start with a valid field name: {}",
                escapeSpecialCharacters(entry),
                configuredName.error().what());
        }
        /// Ignoring an entry that names no field of the output schema would hide a typo until someone wonders why the configured
        /// serializer never ran.
        if (std::ranges::find(fieldNames, configuredName.value()) == fieldNames.end())
        {
            throw InvalidConfigParameter(
                "VALUE_SERIALIZERS configures a serializer for the field '{}', which is not part of the output schema. Known fields: {}",
                configuredName.value(),
                fmt::join(fieldNames, ", "));
        }
        serializerTypes[configuredName.value()] = std::string{trimWhiteSpaces(entry.substr(separator + 1))};
    }
    return serializerTypes;
}

/// Fetches ValueSerializer from Registry
inline std::unique_ptr<ValueSerializer> provideValueSerializer(const std::string& serializerType, const ValueSerializerConfig& config)
{
    const ValueSerializerRegistryArguments arguments{.quoted = config.quoted};
    if (auto serializer = ValueSerializerRegistry::instance().create(serializerType, arguments))
    {
        return std::move(serializer.value());
    }
    throw UnknownValueSerializerType("Unknown Value Serializer: {}", serializerType);
}
}
