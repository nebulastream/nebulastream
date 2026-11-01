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

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace NES
{

/// The elements UNNEST takes from a JSON array, kept apart from the Nautilus code so it can be tested on its own.
/// A string element is its unescaped text; any other element (number, boolean, null, object, array) is its compact
/// JSON text, as Postgres' json_array_elements_text does. Throws CannotFormatMalformedStringValue if `text` is not
/// valid JSON or not an array.
std::vector<std::string> decodeJsonArrayElements(std::string_view text);

/// The elements packed into one block, so that the Nautilus code can address them without owning a C++ object:
/// a uint64 count n, then n + 1 uint64 offsets relative to the start of the data, then the data. Element i is
/// [offset i, offset i + 1). The block is written byte-wise, so it needs no alignment.
size_t packedElementsSize(const std::vector<std::string>& elements);
void packElements(const std::vector<std::string>& elements, std::span<std::byte> block);
uint64_t packedCount(const std::byte* block);
/// Start and size of element `index` in a block of `count` elements.
const std::byte* packedElement(const std::byte* block, uint64_t count, uint64_t index);
uint64_t packedElementSize(const std::byte* block, uint64_t index);

}
