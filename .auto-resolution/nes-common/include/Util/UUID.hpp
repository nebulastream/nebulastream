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
#include <array>
#include <cstddef>
#include <functional>
#include <optional>
#include <string>

namespace NES
{
constexpr size_t UUID_STRING_LENGTH = 36;
constexpr size_t UUID_BYTES = 16;
using UUID = std::array<unsigned char, UUID_BYTES>;

UUID generateUUID();
std::string UUIDToString(const UUID& uuid);
std::optional<UUID> stringToUUID(const std::string&);
UUID stringToUUIDOrThrow(const std::string&);
}

namespace std
{
template <>
struct hash<NES::UUID>
{
    size_t operator()(const NES::UUID& uuid) const noexcept;
};
}
