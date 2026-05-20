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
#include <utility>
#include <vector>

#include <simdjson.h>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <ValueDeserializerRegistry.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES::JSONValueDeserializer
{

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
    const ArenaRef& arena) const
{
    const auto [address, size] = decode(fieldAddress, fieldSize, arena);
    return VarVal{readSingleCharacter(address, size), false, false};
}

VarVal JSONVARSIZEDValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>&,
    const ArenaRef& arena) const
{
    const auto [address, size] = decode(fieldAddress, fieldSize, arena);
    return VarVal{VariableSizedData{address, size}, false, false};
}

VarVal NullableJSONCHARValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>&,
    const ArenaRef& arena) const
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

VarVal NullableJSONVARSIZEDValueDeserializer::deserializeToVarVal(
    const nautilus::val<int8_t*>& fieldAddress,
    const nautilus::val<uint64_t>& fieldSize,
    const std::vector<std::string>&,
    const ArenaRef& arena) const
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

ValueDeserializerRegistryReturnType JSONCHARValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<JSONCHARValueDeserializer>();
}

ValueDeserializerRegistryReturnType JSONVARSIZEDValueDeserializer::provideDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<JSONVARSIZEDValueDeserializer>();
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
