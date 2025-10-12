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

#include <Util/UUID.hpp>

#include <array>
#include <bit>
#include <cstddef>
#include <optional>
#include <string>
#include <utility>
#include <folly/hash/Hash.h>
#include <uuid/uuid.h>
#include <ErrorHandling.hpp>

NES::UUID NES::generateUUID()
{
    UUID bytes;
    uuid_generate(bytes.data());
    return bytes;
}

std::string NES::UUIDToString(const UUID& uuid)
{
    std::string uuidString;
    uuidString.resize(UUID_STRING_LENGTH);
    uuid_unparse(uuid.data(), uuidString.data());
    return uuidString;
}

std::optional<NES::UUID> NES::stringToUUID(const std::string& uuidString)
{
    UUID uuid;
    if (uuid_parse(uuidString.c_str(), uuid.data()) == 0)
    {
        return uuid;
    }
    return std::nullopt;
}

NES::UUID NES::stringToUUIDOrThrow(const std::string& uuidString)
{
    if (const auto result = stringToUUID(uuidString))
    {
        return *result;
    }
    throw InvalidUUID("'{}'", uuidString);
}

size_t std::hash<NES::UUID>::operator()(const NES::UUID& uuid) const noexcept
{
    const auto [right, left] = std::bit_cast<std::array<size_t, 2>>(uuid);
    return folly::hash::hash_combine(right, left);
}
