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


#include <UnnestDecoding.hpp>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <string_view>
#include <vector>
#include <nlohmann/json.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
/// Enough of the input to recognise it in an error message.
std::string preview(const std::string_view text)
{
    constexpr size_t maxLength = 64;
    return text.size() <= maxLength ? std::string{text} : std::string{text.substr(0, maxLength)} + "…";
}

uint64_t readU64(const std::byte* at)
{
    uint64_t value = 0;
    std::memcpy(&value, at, sizeof(value));
    return value;
}

void writeU64(std::byte* at, const uint64_t value)
{
    std::memcpy(at, &value, sizeof(value));
}

const std::byte* offsetsOf(const std::byte* block)
{
    return block + sizeof(uint64_t);
}

}

std::vector<std::string> decodeJsonArrayElements(const std::string_view text)
{
    const auto json = nlohmann::json::parse(text, nullptr, /*allow_exceptions=*/false);
    if (json.is_discarded())
    {
        throw CannotFormatMalformedStringValue("UNNEST expects a JSON array, but got text that is not JSON: '{}'", preview(text));
    }
    if (!json.is_array())
    {
        throw CannotFormatMalformedStringValue("UNNEST expects a JSON array, but got a JSON {}: '{}'", json.type_name(), preview(text));
    }
    std::vector<std::string> elements;
    elements.reserve(json.size());
    for (const auto& element : json)
    {
        elements.push_back(element.is_string() ? element.get<std::string>() : element.dump());
    }
    return elements;
}

size_t packedElementsSize(const std::vector<std::string>& elements)
{
    size_t data = 0;
    for (const auto& element : elements)
    {
        data += element.size();
    }
    return sizeof(uint64_t) * (elements.size() + 2) + data;
}

void packElements(const std::vector<std::string>& elements, const std::span<std::byte> block)
{
    PRECONDITION(block.size() >= packedElementsSize(elements), "UNNEST block too small");
    writeU64(block.data(), elements.size());
    auto* offsets = block.data() + sizeof(uint64_t);
    auto* data = offsets + (sizeof(uint64_t) * (elements.size() + 1));
    uint64_t offset = 0;
    for (size_t i = 0; i < elements.size(); ++i)
    {
        writeU64(offsets + (sizeof(uint64_t) * i), offset);
        std::memcpy(data + offset, elements[i].data(), elements[i].size());
        offset += elements[i].size();
    }
    writeU64(offsets + (sizeof(uint64_t) * elements.size()), offset);
}

uint64_t packedCount(const std::byte* block)
{
    return readU64(block);
}

const std::byte* packedElement(const std::byte* block, const uint64_t count, const uint64_t index)
{
    const auto* data = offsetsOf(block) + (sizeof(uint64_t) * (count + 1));
    return data + readU64(offsetsOf(block) + (sizeof(uint64_t) * index));
}

uint64_t packedElementSize(const std::byte* block, const uint64_t index)
{
    return readU64(offsetsOf(block) + (sizeof(uint64_t) * (index + 1))) - readU64(offsetsOf(block) + (sizeof(uint64_t) * index));
}

}
