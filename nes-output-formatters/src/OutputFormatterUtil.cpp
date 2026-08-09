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

void parseValueSerializerOverrides(const std::string& overrides, std::unordered_map<SerializerKey, std::string>& serializersMap)
{
    size_t typeNameStart = 0;
    size_t typeNameEnd = overrides.find(':', typeNameStart);
    while (typeNameEnd != std::string::npos)
    {
        const std::string typeName = overrides.substr(typeNameStart, typeNameEnd - typeNameStart);
        const size_t serializerTypeStart = typeNameEnd + 1;
        const size_t serializerTypeEnd = std::min(overrides.size(), overrides.find(',', serializerTypeStart));
        const std::string serializerType = overrides.substr(serializerTypeStart, serializerTypeEnd - serializerTypeStart);

        if (std::optional<DataType::Type> dataType = magic_enum::enum_cast<DataType::Type>(typeName))
        {
            serializersMap[dataType.value()] = serializerType;
        }
        else
        {
            /// Store as struct-name
            serializersMap[typeName] = serializerType;
        }
        typeNameStart = serializerTypeEnd + 1;
        typeNameEnd = overrides.find(':', typeNameStart);
    }
}

std::unique_ptr<ValueSerializer> provideValueSerializer(const std::string& serializerType, const ValueSerializerConfig& config)
{
    const ValueSerializerRegistryArguments arguments{.quoted = config.quoted};
    if (auto serializer = ValueSerializerRegistry::instance().create(serializerType, arguments))
    {
        return std::move(serializer.value());
    }
    throw UnknownValueSerializerType("Unknown Value Serializer: {}", serializerType);
}

std::unique_ptr<ValueSerializer> provideSerializerForType(
    const DataType& dataType, const ValueSerializerConfig& config, const std::unordered_map<SerializerKey, std::string>& serializersMap)
{
    /// If the type is a struct, we try to either get the specifically configured serializer for this struct-name, or the default, if it exists.
    /// Otherwise, we fall back to the standard struct serializers.
    if (dataType.type == DataType::Type::STRUCT)
    {
        if (const auto it = serializersMap.find(dataType.structName); it != serializersMap.end())
        {
            return provideValueSerializer(it->second, config);
        }
        /// Try the default serializer for this type, if it exists
        const std::string defaultSerializerName = "Default" + dataType.structName;
        try
        {
            return provideValueSerializer(defaultSerializerName, config);
        }
        catch (Exception& e)
        {
            /// Fall back to DataType::Type specific serializers.
            if (const auto it = serializersMap.find(dataType.type); it != serializersMap.end())
            {
                return provideValueSerializer(it->second, config);
            }
            throw UnknownValueSerializerType("No serializer configured for datatype {}", magic_enum::enum_name(dataType.type));
        }
    }
    if (const auto it = serializersMap.find(dataType.type); it != serializersMap.end())
    {
        return provideValueSerializer(it->second, config);
    }
    throw UnknownValueSerializerType("No serializer configured for datatype {}", magic_enum::enum_name(dataType.type));
}
}
