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

#include <NestedJSONValueDeserializers.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#include <simdjson.h>
#include <magic_enum/magic_enum.hpp>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/FixedSizedData.hpp>
#include <DataTypes/StructData.hpp>
#include <DataTypes/VarArrayData.hpp>
#include <DataTypes/VarVal.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <ValueDeserializer.hpp>
#include <ValueDeserializerRegistry.hpp>
#include <ValueDeserializerUtil.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES::NestedJSONValueDeserializer
{
struct JSONElement
{
    const int8_t* elementPtr;
    uint64_t elementSize;
};

JSONElement* getArrayElementAt(const int8_t* arrayAddress, const uint64_t arraySize, const size_t i)
{
    /// We create a padded string view of the array, extending SIMDJSON_PADDING over arraySize.
    /// In this case, this is safe, because we know that the array view is part of a larger simdjson document. Therefore, SIMDJSON_PADDING
    /// bytes after arraySize should always be allocated and readable at this point.
    const simdjson::padded_string_view arrayView(
        reinterpret_cast<const char*>(arrayAddress), arraySize, arraySize + simdjson::SIMDJSON_PADDING);
    simdjson::ondemand::parser arrayParser;
    thread_local JSONElement element{.elementPtr = nullptr, .elementSize = 0};

    simdjson::ondemand::document doc = arrayParser.iterate(arrayView);
    simdjson::ondemand::array simdArray = doc.get_array();
    const std::string_view rawElement = simdArray.at(i).raw_json().value();
    element.elementPtr = reinterpret_cast<const int8_t*>(rawElement.data());
    element.elementSize = rawElement.size();
    return &element;
}

JSONElement* getStructElementAt(const int8_t* structAddress, const uint64_t structSize, const char* elementIdentifier)
{
    const simdjson::padded_string_view structView(
        reinterpret_cast<const char*>(structAddress), structSize, structSize + simdjson::SIMDJSON_PADDING);
    simdjson::ondemand::parser structParser;
    thread_local JSONElement element{.elementPtr = nullptr, .elementSize = 0};
    const std::string_view elementName{elementIdentifier};

    simdjson::ondemand::document doc = structParser.iterate(structView);
    simdjson::ondemand::object simdStruct = doc.get_object();
    auto simdElement = simdStruct.find_field_unordered(elementName);
    const std::string_view rawElement = simdElement.raw_json().value();
    element.elementPtr = reinterpret_cast<const int8_t*>(rawElement.data());
    element.elementSize = rawElement.size();
    return &element;
}

size_t getNumberOfVectorElements(const int8_t* vectorAddress, const uint64_t vectorSize)
{
    /// Create a simdjson array out of the pointer and size like in the function above
    const simdjson::padded_string_view arrayView(
        reinterpret_cast<const char*>(vectorAddress), vectorSize, vectorSize + simdjson::SIMDJSON_PADDING);
    simdjson::ondemand::parser arrayParser;

    simdjson::ondemand::document doc = arrayParser.iterate(arrayView);
    simdjson::ondemand::array simdArray = doc.get_array();
    return simdArray.count_elements();
}

template <typename T>
void writeFlatValToBuffer(int8_t* bufferAddress, T val)
{
    *reinterpret_cast<T*>(bufferAddress) = val;
}
}

namespace NES
{


VarVal JSONFIXEDSIZEDValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const std::unordered_map<DeserializerKey, std::string>& deserializerTypes,
    const DataType& valueType,
    ArenaRef& arena) const
{
    /// Allocate memory for the fixedsized array's buffer
    const auto elementSize = valueType.elementType[0].getSizeInBytesWithoutNull();
    const nautilus::val<int8_t*> buffer = arena.allocateMemory(static_cast<size_t>(valueType.count) * elementSize);

    /// Create deserializer for element type
    const ValueDeserializerConfig config{.nullable = false, .quoted = true, .hasTrailingSpaces = true};
    const std::unique_ptr<ValueDeserializer> elementDeserializer
        = provideDeserializerForType(valueType.elementType[0], config, deserializerTypes);
    for (nautilus::static_val<uint32_t> i; i < valueType.count; ++i)
    {
        /// Get address and size of element i
        const auto element
            = nautilus::invoke(NestedJSONValueDeserializer::getArrayElementAt, fieldAddress, fieldSize, nautilus::val<size_t>{i});
        const nautilus::val<int8_t*> elementAddress
            = *getMemberWithOffset<int8_t*>(element, offsetof(NestedJSONValueDeserializer::JSONElement, elementPtr));
        const nautilus::val<uint64_t> rawSize
            = *getMemberWithOffset<uint64_t>(element, offsetof(NestedJSONValueDeserializer::JSONElement, elementSize));

        /// Deserialize element into buffer
        elementDeserializer->deserializeIntoBuffer(
            elementAddress,
            rawSize,
            nullValues,
            deserializerTypes,
            valueType.elementType[0],
            arena,
            buffer + nautilus::val<uint64_t>{i * elementSize});
    }
    const FixedSizedData fixedSized{buffer, valueType.count, valueType.elementType[0]};
    return VarVal{fixedSized, false, false};
}

void JSONFIXEDSIZEDValueDeserializer::deserializeIntoBuffer(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const std::unordered_map<DeserializerKey, std::string>& deserializerTypes,
    const DataType& valueType,
    ArenaRef& arena,
    const nautilus::val<int8_t*>& bufferAddress) const
{
    const auto elementSize = valueType.elementType[0].getSizeInBytesWithoutNull();

    /// Create deserializer for element type
    const ValueDeserializerConfig config{.nullable = false, .quoted = true, .hasTrailingSpaces = true};
    const std::unique_ptr<ValueDeserializer> elementDeserializer
        = provideDeserializerForType(valueType.elementType[0], config, deserializerTypes);

    /// Deserialize array element per element and byte align the deserialized contents into the provided buffer.
    for (nautilus::static_val<uint32_t> i; i < valueType.count; ++i)
    {
        /// Get address and size of element i
        const auto element
            = nautilus::invoke(NestedJSONValueDeserializer::getArrayElementAt, fieldAddress, fieldSize, nautilus::val<size_t>{i});
        const nautilus::val<int8_t*> elementAddress
            = *getMemberWithOffset<int8_t*>(element, offsetof(NestedJSONValueDeserializer::JSONElement, elementPtr));
        const nautilus::val<uint64_t> rawSize
            = *getMemberWithOffset<uint64_t>(element, offsetof(NestedJSONValueDeserializer::JSONElement, elementSize));

        /// Deserialize element into buffer
        elementDeserializer->deserializeIntoBuffer(
            elementAddress,
            rawSize,
            nullValues,
            deserializerTypes,
            valueType.elementType[0],
            arena,
            bufferAddress + nautilus::val<uint64_t>{i * elementSize});
    }
}

VarVal JSONVARARRAYValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const std::unordered_map<DeserializerKey, std::string>& deserializerTypes,
    const DataType& valueType,
    ArenaRef& arena) const
{
    /// Allocate memory for the vararray's buffer
    const auto elementSize = valueType.elementType[0].getSizeInBytesWithoutNull();
    /// The number of elements in a vararray depends on the value itself. We obtain them from the value itself.
    const nautilus::val<size_t> varArrayCount
        = nautilus::invoke(NestedJSONValueDeserializer::getNumberOfVectorElements, fieldAddress, fieldSize);
    const nautilus::val<int8_t*> buffer = arena.allocateMemory(varArrayCount * elementSize);

    /// Create deserializer for element type
    const ValueDeserializerConfig config{.nullable = false, .quoted = true, .hasTrailingSpaces = true};
    const std::unique_ptr<ValueDeserializer> elementDeserializer
        = provideDeserializerForType(valueType.elementType[0], config, deserializerTypes);

    /// Deserialize vector element per element and byte align the deserialized contents in the allocated buffer
    for (nautilus::val<size_t> i; i < varArrayCount; ++i)
    {
        /// Get address and size of element i
        const auto element = nautilus::invoke(NestedJSONValueDeserializer::getArrayElementAt, fieldAddress, fieldSize, i);
        const nautilus::val<int8_t*> elementAddress
            = *getMemberWithOffset<int8_t*>(element, offsetof(NestedJSONValueDeserializer::JSONElement, elementPtr));
        const nautilus::val<uint64_t> rawSize
            = *getMemberWithOffset<uint64_t>(element, offsetof(NestedJSONValueDeserializer::JSONElement, elementSize));

        /// Deserialize element into buffer
        elementDeserializer->deserializeIntoBuffer(
            elementAddress,
            rawSize,
            nullValues,
            deserializerTypes,
            valueType.elementType[0],
            arena,
            buffer + nautilus::val{i * elementSize});
    }
    const VarArrayData varArray{buffer, valueType.elementType[0], varArrayCount * elementSize};
    return VarVal{varArray, false, false};
}

void JSONVARARRAYValueDeserializer::deserializeIntoBuffer(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const std::unordered_map<DeserializerKey, std::string>& deserializerTypes,
    const DataType& valueType,
    ArenaRef& arena,
    const nautilus::val<int8_t*>& bufferAddress) const
{
    /// Allocate memory for the vararray's buffer
    const auto elementSize = valueType.elementType[0].getSizeInBytesWithoutNull();
    /// The number of elements in a vararray depends on the value itself. We obtain them from the value itself.
    const nautilus::val<size_t> varArrayCount
        = nautilus::invoke(NestedJSONValueDeserializer::getNumberOfVectorElements, fieldAddress, fieldSize);
    const nautilus::val<int8_t*> buffer = arena.allocateMemory(varArrayCount * elementSize);

    /// Create deserializer for element type
    const ValueDeserializerConfig config{.nullable = false, .quoted = true, .hasTrailingSpaces = true};
    const std::unique_ptr<ValueDeserializer> elementDeserializer
        = provideDeserializerForType(valueType.elementType[0], config, deserializerTypes);

    /// Deserialize vector element per element and byte align the deserialized contents in the allocated buffer
    for (nautilus::val<size_t> i; i < varArrayCount; ++i)
    {
        /// Get address and size of element i
        const auto element = nautilus::invoke(NestedJSONValueDeserializer::getArrayElementAt, fieldAddress, fieldSize, i);
        const nautilus::val<int8_t*> elementAddress
            = *getMemberWithOffset<int8_t*>(element, offsetof(NestedJSONValueDeserializer::JSONElement, elementPtr));
        const nautilus::val<uint64_t> rawSize
            = *getMemberWithOffset<uint64_t>(element, offsetof(NestedJSONValueDeserializer::JSONElement, elementSize));

        /// Deserialize element into buffer
        elementDeserializer->deserializeIntoBuffer(
            elementAddress,
            rawSize,
            nullValues,
            deserializerTypes,
            valueType.elementType[0],
            arena,
            buffer + nautilus::val{i * elementSize});
    }
    /// Write the vararray buffer address and size into the buffer provided by the function
    nautilus::invoke(NestedJSONValueDeserializer::writeFlatValToBuffer<int8_t*>, bufferAddress, buffer);
    nautilus::invoke(
        NestedJSONValueDeserializer::writeFlatValToBuffer<uint64_t>,
        bufferAddress + nautilus::val<uint64_t>{sizeof(int8_t*)},
        nautilus::val<uint64_t>{elementSize * varArrayCount});
}

VarVal JSONSTRUCTValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const std::unordered_map<DeserializerKey, std::string>& deserializerTypes,
    const DataType& valueType,
    ArenaRef& arena) const
{
    /// Allocate the memory for the struct buffer
    const nautilus::val<int8_t*> buffer = arena.allocateMemory(valueType.getSizeInBytesWithoutNull());
    nautilus::val<int8_t*> currentBufferPos = buffer;
    for (nautilus::static_val<size_t> i; i < valueType.fields.size(); ++i)
    {
        const auto& [subFieldName, subFieldType] = valueType.fields.at(i);
        /// Create deserializer for the subfield at position i
        const ValueDeserializerConfig config{.nullable = false, .quoted = true, .hasTrailingSpaces = true};
        const std::unique_ptr<ValueDeserializer> fieldDeserializer = provideDeserializerForType(subFieldType, config, deserializerTypes);
        /// Get position of the value at the ith field of the struct
        const auto element = nautilus::invoke(
            NestedJSONValueDeserializer::getStructElementAt, fieldAddress, fieldSize, nautilus::val<const char*>{subFieldName.c_str()});
        const nautilus::val<int8_t*> elementAddress
            = *getMemberWithOffset<int8_t*>(element, offsetof(NestedJSONValueDeserializer::JSONElement, elementPtr));
        const nautilus::val<uint64_t> rawSize
            = *getMemberWithOffset<uint64_t>(element, offsetof(NestedJSONValueDeserializer::JSONElement, elementSize));

        /// Deserialize element
        fieldDeserializer->deserializeIntoBuffer(
            elementAddress, rawSize, nullValues, deserializerTypes, subFieldType, arena, currentBufferPos);
        /// Move buffer pos
        currentBufferPos += subFieldType.getSizeInBytesWithoutNull();
    }
    const StructData structData{buffer, valueType.fields};
    return VarVal{structData, false, false};
}

void JSONSTRUCTValueDeserializer::deserializeIntoBuffer(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const std::unordered_map<DeserializerKey, std::string>& deserializerTypes,
    const DataType& valueType,
    ArenaRef& arena,
    const nautilus::val<int8_t*>& bufferAddress) const
{
    nautilus::val<int8_t*> currentBufferPos = bufferAddress;
    for (nautilus::static_val<size_t> i; i < valueType.fields.size(); ++i)
    {
        const auto& [subFieldName, subFieldType] = valueType.fields.at(i);
        /// Create deserializer for the subfield at position i
        const ValueDeserializerConfig config{.nullable = false, .quoted = true, .hasTrailingSpaces = true};
        const std::unique_ptr<ValueDeserializer> fieldDeserializer = provideDeserializerForType(subFieldType, config, deserializerTypes);
        /// Get position of the value at the ith field of the struct
        const auto element = nautilus::invoke(
            NestedJSONValueDeserializer::getStructElementAt, fieldAddress, fieldSize, nautilus::val<const char*>{subFieldName.c_str()});
        const nautilus::val<int8_t*> elementAddress
            = *getMemberWithOffset<int8_t*>(element, offsetof(NestedJSONValueDeserializer::JSONElement, elementPtr));
        const nautilus::val<uint64_t> rawSize
            = *getMemberWithOffset<uint64_t>(element, offsetof(NestedJSONValueDeserializer::JSONElement, elementSize));

        /// Deserialize element
        fieldDeserializer->deserializeIntoBuffer(
            elementAddress, rawSize, nullValues, deserializerTypes, subFieldType, arena, currentBufferPos);
        /// Move buffer pos
        currentBufferPos += subFieldType.getSizeInBytesWithoutNull();
    }
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterJSONSTRUCTValueDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<JSONSTRUCTValueDeserializer>();
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterJSONFIXEDSIZEDValueDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<JSONFIXEDSIZEDValueDeserializer>();
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterJSONVARARRAYValueDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<JSONVARARRAYValueDeserializer>();
}
}
