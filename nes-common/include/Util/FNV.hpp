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

namespace NES
{

constexpr uint64_t FNV1A_64_OFFSET_BASIS = 14695981039346656037ULL;
constexpr uint64_t FNV1A_64_PRIME = 1099511628211ULL;

/// This hash is used to verify the header written to disk by the store manager is valid
constexpr uint64_t fnv1a64(const char* data, size_t len)
{
    uint64_t hash = FNV1A_64_OFFSET_BASIS;
    for (size_t i = 0; i < len; ++i)
    {
        hash ^= static_cast<uint8_t>(data[i]);
        hash *= FNV1A_64_PRIME;
    }
    return hash;
}

}
