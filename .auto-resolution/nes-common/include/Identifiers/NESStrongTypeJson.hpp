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
#include <string>
#include <Identifiers/NESStrongType.hpp>
#include <nlohmann/json.hpp>

/**
 * Adds NESStrongType overloads for the nlohmann json library.
 * This allows assignements of json values with identifiers.
 * To retrieve Identifiers from json `.get<WorkerId>()` is used instead of manually creating an Identifier using the raw value.
 */
namespace nlohmann
{
template <typename T, typename Tag, T invalid, T initial>
struct adl_serializer<NES::NESStrongType<T, Tag, invalid, initial>>
{
    ///NOLINTNEXTLINE(readability-identifier-naming)
    static NES::NESStrongType<T, Tag, invalid, initial> from_json(const json& jsonObject)
    {
        return NES::NESStrongType<T, Tag, invalid, initial>{jsonObject.get<T>()};
    }

    ///NOLINTNEXTLINE(readability-identifier-naming)
    static void to_json(json& jsonObject, NES::NESStrongType<T, Tag, invalid, initial> strongType)
    {
        jsonObject = strongType.getRawValue();
    }
};

template <typename Tag, NES::StringLiteral Invalid>
struct adl_serializer<NES::NESStrongStringType<Tag, Invalid>>
{
    ///NOLINTNEXTLINE(readability-identifier-naming)
    static NES::NESStrongStringType<Tag, Invalid> from_json(const json& jsonObject)
    {
        return NES::NESStrongStringType<Tag, Invalid>{jsonObject.get<std::string>()};
    }

    ///NOLINTNEXTLINE(readability-identifier-naming)
    static void to_json(json& jsonObject, NES::NESStrongStringType<Tag, Invalid> strongType) { jsonObject = strongType.getRawValue(); }
};

template <typename Tag>
struct adl_serializer<NES::NESStrongUUIDType<Tag>>
{
    ///NOLINTNEXTLINE(readability-identifier-naming)
    static NES::NESStrongUUIDType<Tag> from_json(const json& jsonObject)
    {
        return NES::NESStrongUUIDType<Tag>{jsonObject.get<std::string>()};
    }

    ///NOLINTNEXTLINE(readability-identifier-naming)
    static void to_json(json& jsonObject, NES::NESStrongUUIDType<Tag> strongType) { jsonObject = strongType.getRawValue(); }
};
}
