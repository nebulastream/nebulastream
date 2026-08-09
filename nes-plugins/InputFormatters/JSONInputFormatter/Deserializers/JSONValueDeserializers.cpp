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

#include <JSONValueDeserializers.hpp>

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <simdjson.h>
#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <DataTypes/VectorData.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <ValueDeserializerRegistry.hpp>
#include <ValueDeserializerUtil.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>
#include <val_std.hpp>

namespace NES::JSONValueDeserializer
{

struct JSONElement
{
    const int8_t* elementPtr;
    uint64_t elementSize;
};

namespace
{
/// simdjson's unescape routine lives on the parser, which builds it during allocate(). We never parse a document with this parser,
/// we only borrow its (stateless) string parsing implementation, so the smallest possible allocation suffices.
simdjson::ondemand::parser& unescapeParser()
{
    thread_local auto parser = []
    {
        simdjson::ondemand::parser allocatedParser;
        if (const auto error = allocatedParser.allocate(0); error != simdjson::SUCCESS)
        {
            throw CannotFormatSourceData("Could not allocate the simdjson parser used for unescaping: {}", simdjson::error_message(error));
        }
        return allocatedParser;
    }();
    return parser;
}
}

/// Decodes the JSON string that starts at fieldAddress into destination and returns the size of the decoded value.
/// fieldAddress points at the opening quote of the raw JSON token; simdjson stops at the terminating unescaped quote on its own,
/// so trailing bytes behind the token (whitespace up to the next token) do not have to be trimmed first. It scans for that quote
/// without an end bound, so it relies on the token coming from a successful parse, which guarantees the quote to be there. Without
/// it, the decoded value could outgrow the memory that the caller reserved for it.
uint64_t decodeProxy(const int8_t* fieldAddress, const uint64_t fieldSize, int8_t* destination)
{
    PRECONDITION(fieldAddress != nullptr, "The raw value of a JSON string must not be null at this point");
    if (fieldSize < 2 || *fieldAddress != '"')
    {
        /// Without this check a non-string value would send simdjson scanning past the end of the field, looking for a quote that
        /// belongs to some later value.
        throw CannotFormatMalformedStringValue(
            "Expected a JSON string, but got: {}", std::string_view{reinterpret_cast<const char*>(fieldAddress), fieldSize});
    }

    /// A raw_json_string points behind the opening quote of the JSON string
    const simdjson::ondemand::raw_json_string rawJsonString{reinterpret_cast<const uint8_t*>(fieldAddress) + 1};
    auto* writePosition = reinterpret_cast<uint8_t*>(destination);
    const auto decoded = unescapeParser().unescape(rawJsonString, writePosition, false);
    if (const auto error = decoded.error(); error != simdjson::SUCCESS)
    {
        throw CannotFormatMalformedStringValue(
            "Cannot decode the JSON string {}: {}",
            std::string_view{reinterpret_cast<const char*>(fieldAddress), fieldSize},
            simdjson::error_message(error));
    }
    return decoded.value_unsafe().size();
}

void getStructElementAt(const int8_t* structAddress, const uint64_t structSize, const char* elementIdentifier, JSONElement* element)
{
    /// We create a padded string view of the struct, extending SIMDJSON_PADDING over structSize.
    /// In this case, this is safe, because we know that the struct view is part of a larger simdjson document. Therefore, SIMDJSON_PADDING
    /// bytes after structSize should always be allocated and readable at this point.
    const simdjson::padded_string_view structView(
        reinterpret_cast<const char*>(structAddress), structSize, structSize + simdjson::SIMDJSON_PADDING);
    simdjson::ondemand::parser structParser;
    const std::string_view elementName{elementIdentifier};

    simdjson::ondemand::document doc = structParser.iterate(structView);
    simdjson::ondemand::object simdStruct = doc.get_object();
    auto simdElement = simdStruct.find_field_unordered(elementName);
    const std::string_view rawElement = simdElement.raw_json().value();
    element->elementPtr = reinterpret_cast<const int8_t*>(rawElement.data());
    element->elementSize = rawElement.size();
}

void getArrayElementAt(const int8_t* arrayAddress, const uint64_t arraySize, const size_t i, JSONElement* element)
{
    /// We create a padded string view of the array, extending SIMDJSON_PADDING over arraySize.
    /// In this case, this is safe, because we know that the array view is part of a larger simdjson document. Therefore, SIMDJSON_PADDING
    /// bytes after arraySize should always be allocated and readable at this point.
    const simdjson::padded_string_view arrayView(
        reinterpret_cast<const char*>(arrayAddress), arraySize, arraySize + simdjson::SIMDJSON_PADDING);
    simdjson::ondemand::parser arrayParser;

    simdjson::ondemand::document doc = arrayParser.iterate(arrayView);
    simdjson::ondemand::array simdArray = doc.get_array();
    const std::string_view rawElement = simdArray.at(i).raw_json().value();
    element->elementPtr = reinterpret_cast<const int8_t*>(rawElement.data());
    element->elementSize = rawElement.size();
}

/// We need this function as for VECTOR typed fields, we do not know the number of elements beforehand.
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

}

namespace NES
{

namespace
{
/// Decodes the raw JSON string into memory of the arena and returns the address and size of the decoded value
std::pair<nautilus::val<int8_t*>, nautilus::val<uint64_t>>
decode(const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const ArenaRef& arena)
{
    /// Decoding only ever shrinks a string, but simdjson may write up to SIMDJSON_PADDING bytes beyond the end of the decoded value
    const auto destination = arena.allocateMemory(fieldSize + nautilus::val<uint64_t>{simdjson::SIMDJSON_PADDING});
    const auto decodedSize = nautilus::invoke(JSONValueDeserializer::decodeProxy, fieldAddress, fieldSize, destination);
    return {destination, decodedSize};
}

/// Reads the single character that the decoded JSON string consists of
nautilus::val<char> readSingleCharacter(const nautilus::val<int8_t*>& address, const nautilus::val<uint64_t>& size)
{
    return nautilus::invoke(
        +[](const int8_t* valueAddress, const uint64_t valueSize)
        {
            if (valueSize != 1)
            {
                throw CannotFormatMalformedStringValue(
                    "{} is not a supported char value", std::string_view{reinterpret_cast<const char*>(valueAddress), valueSize});
            }
            return static_cast<char>(*valueAddress);
        },
        address,
        size);
}
}

VarVal JSONCHARValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>&,
    const ArenaRef& arena,
    const std::unordered_map<DataType::Type, std::string>&,
    const DataType&) const
{
    const auto [address, size] = decode(fieldAddress, fieldSize, arena);
    return VarVal{readSingleCharacter(address, size), false, false};
}

void JSONCHARValueDeserializer::deserializeIntoBuffer(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef& arena,
    const std::unordered_map<DataType::Type, std::string>& deserializerTypes,
    const DataType& valueType,
    const nautilus::val<int8_t*>& bufferAddress) const
{
    const VarVal deserializedVal = deserializeToVarVal(fieldAddress, fieldSize, nullValues, arena, deserializerTypes, valueType);
    deserializedVal.writeToMemory(bufferAddress);
}

VarVal JSONVARSIZEDValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>&,
    const ArenaRef& arena,
    const std::unordered_map<DataType::Type, std::string>&,
    const DataType&) const
{
    const auto [address, size] = decode(fieldAddress, fieldSize, arena);
    return VarVal{VariableSizedData{address, size}, false, false};
}

void JSONVARSIZEDValueDeserializer::deserializeIntoBuffer(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef& arena,
    const std::unordered_map<DataType::Type, std::string>& deserializerTypes,
    const DataType& valueType,
    const nautilus::val<int8_t*>& bufferAddress) const
{
    const VarVal deserializedVal = deserializeToVarVal(fieldAddress, fieldSize, nullValues, arena, deserializerTypes, valueType);
    deserializedVal.writeToMemory(bufferAddress);
}

VarVal JSONSTRUCTValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef& arena,
    const std::unordered_map<DataType::Type, std::string>& deserializerTypes,
    const DataType& valueType) const
{
    /// Allocate the memory for the struct buffer
    const nautilus::val<int8_t*> buffer = arena.allocateMemory(valueType.getSizeInBytesWithoutNull());
    nautilus::val<int8_t*> currentBufferPos = buffer;
    for (nautilus::static_val<size_t> i; i < valueType.fields.size(); ++i)
    {
        const auto& [subFieldName, subFieldType] = valueType.fields.at(i);
        /// Create deserializer for the subfield at position i
        const ValueDeserializerConfig config{.nullable = false, .quoted = true, .hasTrailingSpaces = true};
        const std::unique_ptr<ValueDeserializer> fieldDeserializer
            = provideValueDeserializer(getDeserializerType(subFieldType, deserializerTypes), config);

        /// Get position of the value at the ith field of the struct
        nautilus::val<JSONValueDeserializer::JSONElement> element;
        nautilus::invoke(
            JSONValueDeserializer::getStructElementAt, fieldAddress, fieldSize, nautilus::val<const char*>{subFieldName.c_str()}, &element);
        const nautilus::val<const int8_t*> elementAddress = element.get(&JSONValueDeserializer::JSONElement::elementPtr);
        const nautilus::val<uint64_t> rawSize = element.get(&JSONValueDeserializer::JSONElement::elementSize);

        /// Deserialize element into the allocated buffer
        fieldDeserializer->deserializeIntoBuffer(
            static_cast<nautilus::val<int8_t*>>(elementAddress),
            rawSize,
            nullValues,
            arena,
            deserializerTypes,
            subFieldType,
            currentBufferPos);
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
    const ArenaRef& arena,
    const std::unordered_map<DataType::Type, std::string>& deserializerTypes,
    const DataType& valueType,
    const nautilus::val<int8_t*>& bufferAddress) const
{
    nautilus::val<int8_t*> currentBufferPos = bufferAddress;
    for (nautilus::static_val<size_t> i; i < valueType.fields.size(); ++i)
    {
        const auto& [subFieldName, subFieldType] = valueType.fields.at(i);
        /// Create deserializer for the subfield at position i
        const ValueDeserializerConfig config{.nullable = false, .quoted = true, .hasTrailingSpaces = true};
        const std::unique_ptr<ValueDeserializer> fieldDeserializer
            = provideValueDeserializer(getDeserializerType(subFieldType, deserializerTypes), config);

        /// Get position of the value at the ith field of the struct
        nautilus::val<JSONValueDeserializer::JSONElement> element;
        nautilus::invoke(
            JSONValueDeserializer::getStructElementAt, fieldAddress, fieldSize, nautilus::val<const char*>{subFieldName.c_str()}, &element);
        const nautilus::val<const int8_t*> elementAddress = element.get(&JSONValueDeserializer::JSONElement::elementPtr);
        const nautilus::val<uint64_t> rawSize = element.get(&JSONValueDeserializer::JSONElement::elementSize);

        /// Deserialize element into the provided buffer
        fieldDeserializer->deserializeIntoBuffer(
            static_cast<nautilus::val<int8_t*>>(elementAddress),
            rawSize,
            nullValues,
            arena,
            deserializerTypes,
            subFieldType,
            currentBufferPos);
        /// Move buffer pos
        currentBufferPos += subFieldType.getSizeInBytesWithoutNull();
    }
}

VarVal JSONFIXEDSIZEDValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef& arena,
    const std::unordered_map<DataType::Type, std::string>& deserializerTypes,
    const DataType& valueType) const
{
    /// Allocate memory for the fixedsized array's buffer
    const auto elementSize = valueType.elementType->getSizeInBytesWithoutNull();
    const nautilus::val<int8_t*> buffer = arena.allocateMemory(static_cast<size_t>(valueType.count) * elementSize);

    /// Create deserializer for element type
    const ValueDeserializerConfig config{.nullable = false, .quoted = true, .hasTrailingSpaces = true};
    const std::unique_ptr<ValueDeserializer> elementDeserializer
        = provideValueDeserializer(getDeserializerType(*valueType.elementType, deserializerTypes), config);
    for (nautilus::static_val<uint32_t> i; i < valueType.count; ++i)
    {
        /// Get address and size of element i
        nautilus::val<JSONValueDeserializer::JSONElement> element;
        nautilus::invoke(JSONValueDeserializer::getArrayElementAt, fieldAddress, fieldSize, nautilus::val<size_t>{i}, &element);
        const nautilus::val<const int8_t*> elementAddress = element.get(&JSONValueDeserializer::JSONElement::elementPtr);
        const nautilus::val<uint64_t> rawSize = element.get(&JSONValueDeserializer::JSONElement::elementSize);

        /// Deserialize element into the allocated buffer
        elementDeserializer->deserializeIntoBuffer(
            static_cast<nautilus::val<int8_t*>>(elementAddress),
            rawSize,
            nullValues,
            arena,
            deserializerTypes,
            *valueType.elementType,
            buffer + nautilus::val<uint64_t>{i * elementSize});
    }
    const FixedSizedData fixedSized{buffer, valueType.count, *valueType.elementType};
    return VarVal{fixedSized, false, false};
}

void JSONFIXEDSIZEDValueDeserializer::deserializeIntoBuffer(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef& arena,
    const std::unordered_map<DataType::Type, std::string>& deserializerTypes,
    const DataType& valueType,
    const nautilus::val<int8_t*>& bufferAddress) const
{
    const auto elementSize = valueType.elementType->getSizeInBytesWithoutNull();

    /// Create deserializer for element type
    const ValueDeserializerConfig config{.nullable = false, .quoted = true, .hasTrailingSpaces = true};
    const std::unique_ptr<ValueDeserializer> elementDeserializer
        = provideValueDeserializer(getDeserializerType(*valueType.elementType, deserializerTypes), config);
    for (nautilus::static_val<uint32_t> i; i < valueType.count; ++i)
    {
        /// Get address and size of element i
        nautilus::val<JSONValueDeserializer::JSONElement> element;
        nautilus::invoke(JSONValueDeserializer::getArrayElementAt, fieldAddress, fieldSize, nautilus::val<size_t>{i}, &element);
        const nautilus::val<const int8_t*> elementAddress = element.get(&JSONValueDeserializer::JSONElement::elementPtr);
        const nautilus::val<uint64_t> rawSize = element.get(&JSONValueDeserializer::JSONElement::elementSize);

        /// Deserialize element into the provided buffer
        elementDeserializer->deserializeIntoBuffer(
            static_cast<nautilus::val<int8_t*>>(elementAddress),
            rawSize,
            nullValues,
            arena,
            deserializerTypes,
            *valueType.elementType,
            bufferAddress + nautilus::val<uint64_t>{i * elementSize});
    }
}

VarVal JSONVECTORValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef& arena,
    const std::unordered_map<DataType::Type, std::string>& deserializerTypes,
    const DataType& valueType) const
{
    /// Allocate memory for the vector buffer
    const auto elementSize = valueType.elementType->getSizeInBytesWithoutNull();
    const nautilus::val<size_t> elementCount = nautilus::invoke(JSONValueDeserializer::getNumberOfVectorElements, fieldAddress, fieldSize);
    const nautilus::val<int8_t*> buffer = arena.allocateMemory(elementCount * elementSize);

    /// Create deserializer for element type
    const ValueDeserializerConfig config{.nullable = false, .quoted = true, .hasTrailingSpaces = true};
    const std::unique_ptr<ValueDeserializer> elementDeserializer
        = provideValueDeserializer(getDeserializerType(*valueType.elementType, deserializerTypes), config);
    for (nautilus::val<size_t> i; i < elementCount; ++i)
    {
        /// Get address and size of element i
        nautilus::val<JSONValueDeserializer::JSONElement> element;
        nautilus::invoke(JSONValueDeserializer::getArrayElementAt, fieldAddress, fieldSize, nautilus::val<size_t>{i}, &element);
        const nautilus::val<const int8_t*> elementAddress = element.get(&JSONValueDeserializer::JSONElement::elementPtr);
        const nautilus::val<uint64_t> rawSize = element.get(&JSONValueDeserializer::JSONElement::elementSize);

        /// Deserialize element into the allocated buffer
        elementDeserializer->deserializeIntoBuffer(
            static_cast<nautilus::val<int8_t*>>(elementAddress),
            rawSize,
            nullValues,
            arena,
            deserializerTypes,
            *valueType.elementType,
            buffer + nautilus::val{i * elementSize});
    }
    const VectorData vectorData{buffer, *valueType.elementType, elementCount * elementSize};
    return VarVal{vectorData, false, false};
}

void JSONVECTORValueDeserializer::deserializeIntoBuffer(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>& nullValues,
    const ArenaRef& arena,
    const std::unordered_map<DataType::Type, std::string>& deserializerTypes,
    const DataType& valueType,
    const nautilus::val<int8_t*>& bufferAddress) const
{
    /// BufferAddress only allocated 16 bytes for the ptr and size of the memory of the vector, because we do not know the size of every received vector beforehand.
    /// Therefore, we can call deserializeToVarVal here and write the 16 byte representation of the varval to bufferAddress.
    const VarVal vectorVal = deserializeToVarVal(fieldAddress, fieldSize, nullValues, arena, deserializerTypes, valueType);
    vectorVal.writeToMemory(bufferAddress);
}

VarVal NullableJSONCHARValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>&,
    const ArenaRef& arena,
    const std::unordered_map<DataType::Type, std::string>&,
    const DataType&) const
{
    nautilus::val<char> value{char{0}};
    nautilus::val<bool> isNull{true};
    /// A field that the RawBufferIndex could not find, or that holds a JSON null, arrives as {nullptr, 0}
    if (not(fieldAddress == nullptr and fieldSize == nautilus::val<uint64_t>{0}))
    {
        const auto [address, size] = decode(fieldAddress, fieldSize, arena);
        value = readSingleCharacter(address, size);
        isNull = false;
    }
    return VarVal{value, true, isNull};
}

void NullableJSONCHARValueDeserializer::deserializeIntoBuffer(
    const nautilus::val<int8_t*>&,
    const nautilus::val<uint64_t>&,
    const std::vector<std::string>&,
    const ArenaRef&,
    const std::unordered_map<DataType::Type, std::string>&,
    const DataType&,
    const nautilus::val<int8_t*>&) const
{
    PRECONDITION(false, "Extensible DataTypes POC does not include nullable struct and array elements");
}

VarVal NullableJSONVARSIZEDValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>&,
    const ArenaRef& arena,
    const std::unordered_map<DataType::Type, std::string>&,
    const DataType&) const
{
    nautilus::val<int8_t*> address{nullptr};
    nautilus::val<uint64_t> size{0};
    nautilus::val<bool> isNull{true};
    /// A field that the RawBufferIndex could not find, or that holds a JSON null, arrives as {nullptr, 0}
    if (not(fieldAddress == nullptr and fieldSize == nautilus::val<uint64_t>{0}))
    {
        const auto [decodedAddress, decodedSize] = decode(fieldAddress, fieldSize, arena);
        address = decodedAddress;
        size = decodedSize;
        isNull = false;
    }
    return VarVal{VariableSizedData{address, size}, true, isNull};
}

void NullableJSONVARSIZEDValueDeserializer::deserializeIntoBuffer(
    const nautilus::val<int8_t*>&,
    const nautilus::val<uint64_t>&,
    const std::vector<std::string>&,
    const ArenaRef&,
    const std::unordered_map<DataType::Type, std::string>&,
    const DataType&,
    const nautilus::val<int8_t*>&) const
{
    PRECONDITION(false, "Extensible DataTypes POC does not include nullable struct and array elements");
}

ValueDeserializerRegistryReturnType JSONCHARValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<JSONCHARValueDeserializer>();
}

ValueDeserializerRegistryReturnType JSONVARSIZEDValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<JSONVARSIZEDValueDeserializer>();
}

ValueDeserializerRegistryReturnType JSONSTRUCTValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<JSONSTRUCTValueDeserializer>();
}

ValueDeserializerRegistryReturnType JSONFIXEDSIZEDValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<JSONFIXEDSIZEDValueDeserializer>();
}

ValueDeserializerRegistryReturnType JSONVECTORValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<JSONVECTORValueDeserializer>();
}

ValueDeserializerRegistryReturnType NullableJSONCHARValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<NullableJSONCHARValueDeserializer>();
}

ValueDeserializerRegistryReturnType NullableJSONVARSIZEDValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<NullableJSONVARSIZEDValueDeserializer>();
}
}
