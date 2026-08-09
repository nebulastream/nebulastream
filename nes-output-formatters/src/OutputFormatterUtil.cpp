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

#include <OutputFormatters/OutputFormatterUtil.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <variant>
#include <magic_enum/magic_enum.hpp>

#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <Interface/RecordBuffer.hpp>
#include <OutputFormatters/ValueSerializer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <ValueSerializerRegistry.hpp>

namespace NES
{

uint64_t writeValueToBuffer(
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

[[nodiscard]] std::optional<std::string> getPluginTypeDefaultSerializer(const std::string& pluginName)
{
    const std::string defaultSerializerName = "Default" + pluginName;
    if (const auto deserializerFactory = ValueSerializerRegistry::instance().find(defaultSerializerName))
    {
        return std::optional{defaultSerializerName};
    }
    return std::nullopt;
}

[[nodiscard]] std::string
getSerializerType(const DataType& dataType, const std::unordered_map<DataType::Type, std::string>& serializerTypes)
{
    if (dataType.type == DataType::Type::STRUCT)
    {
        /// Use the default serializer for this type, if it exists.
        if (auto pluginDefault = getPluginTypeDefaultSerializer(dataType.structName))
        {
            return std::move(*pluginDefault);
        }
    }
    if (const auto it = serializerTypes.find(dataType.type); it != serializerTypes.end())
    {
        return it->second;
    }
    throw UnknownValueSerializerType("No ValueSerializer configured for DataType {}", magic_enum::enum_name(dataType.type));
}

[[nodiscard]] std::unordered_map<Record::RecordFieldIdentifier, std::string>
parseValueSerializerOverrides(const std::string& overrides, const std::vector<Record::RecordFieldIdentifier>& fieldNames)
{
    std::unordered_map<Record::RecordFieldIdentifier, std::string> serializerTypes;
    for (const auto& entry : splitOnMultipleDelimiters(overrides, {','}, {'"'}))
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

std::unique_ptr<ValueSerializer> provideValueSerializer(const std::string& serializerType, const ValueSerializerConfig& config)
{
    const ValueSerializerRegistryArguments arguments{.quoted = config.quoted};
    if (const auto serializerFactory = ValueSerializerRegistry::instance().find(serializerType))
    {
        return (*serializerFactory)(arguments);
    }
    throw UnknownValueSerializerType("Unknown Value Serializer: {}", serializerType);
}

}
