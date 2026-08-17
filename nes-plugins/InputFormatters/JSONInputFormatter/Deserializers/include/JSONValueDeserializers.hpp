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

#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <simdjson.h>
#include <DataTypes/DataType.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <ValueDeserializer.hpp>

namespace NES::JSONValueDeserializer
{

namespace detail
{
/// simdjson's unescape routine lives on the parser, which builds it during allocate(). We never parse a document with this parser,
/// we only borrow its (stateless) string parsing implementation, so the smallest possible allocation suffices.
inline simdjson::ondemand::parser& unescapeParser()
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

/// Decodes the JSON string that starts at fieldAddress into destination and returns the size of the decoded value.
/// fieldAddress points at the opening quote of the raw JSON token; simdjson stops at the terminating unescaped quote on its own,
/// so trailing bytes behind the token (whitespace up to the next token) do not have to be trimmed first. It scans for that quote
/// without an end bound, so it relies on the token coming from a successful parse, which guarantees the quote to be there. Without
/// it, the decoded value could outgrow the memory that the caller reserved for it.
inline uint64_t decode(const int8_t* fieldAddress, const uint64_t fieldSize, int8_t* destination)
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
    const auto decoded = detail::unescapeParser().unescape(rawJsonString, writePosition, false);
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

/// Arena allocation, decode, single-character check and null test all happen here rather than in traced code, so
/// a JSON field costs one call instead of three invokes and a branch.
/// The raw JSON text of a string is not its value: '"ABC"' is a nine byte token encoding the three bytes 'ABC'.
/// Decoding only shrinks a string, but simdjson may write up to SIMDJSON_PADDING bytes past the decoded value.
/// A field the RawBufferIndex could not find, or holding a JSON null, arrives as {nullptr, 0}; null detection
/// happens there, so the nullValues list is not consulted here.
template <bool Nullable, bool SingleCharacter>
DeserializedValue* deserializeJson(int8_t* fieldAddress, const uint64_t fieldSize, const std::vector<std::string>*, Arena* arena)
{
    PRECONDITION(arena != nullptr, "The arena is expected to be not null!");

    thread_local static DeserializedValue result;
    result.isNull = false;

    if constexpr (Nullable)
    {
        if (fieldAddress == nullptr && fieldSize == 0)
        {
            result.varsizedPtr = nullptr;
            result.varsizedSize = 0;
            result.isNull = true;
            result.value = {};
            return &result;
        }
    }

    auto* destination = reinterpret_cast<int8_t*>(arena->allocateMemory(fieldSize + simdjson::SIMDJSON_PADDING).data());
    const auto decodedSize = detail::decode(fieldAddress, fieldSize, destination);

    if constexpr (SingleCharacter)
    {
        if (decodedSize != 1)
        {
            throw CannotFormatMalformedStringValue(
                "{} is not a supported char value", std::string_view{reinterpret_cast<const char*>(destination), decodedSize});
        }
        const auto character = static_cast<char>(*destination);
        std::memcpy(result.value.data(), &character, sizeof(char));
        return &result;
    }
    else
    {
        result.varsizedPtr = destination;
        result.varsizedSize = decodedSize;
        return &result;
    }
}

}

namespace NES
{

/// Deserializes a JSON string into its value, decoding escape sequences ('\n', '\uXXXX', ...).
/// SingleCharacter selects the CHAR variant, which also asserts the decoded value is exactly one byte.
template <bool Nullable, bool SingleCharacter>
class JsonValueDeserializer final : public ValueDeserializer
{
public:
    explicit JsonValueDeserializer(const std::string_view name) : ValueDeserializer(name) { }

    [[nodiscard]] DeserializeProxy proxy() const override { return &JSONValueDeserializer::deserializeJson<Nullable, SingleCharacter>; }

    [[nodiscard]] DataType::Type producedType() const override { return SingleCharacter ? DataType::Type::CHAR : DataType::Type::VARSIZED; }
};
}
