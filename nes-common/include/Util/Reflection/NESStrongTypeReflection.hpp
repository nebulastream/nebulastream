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

#include <Identifiers/NESStrongType.hpp>
#include <Util/Reflection/ReflectionCore.hpp>

namespace NES
{

/// Reflector for NESStrongType - reflects the underlying value
template <typename T, typename Tag, T invalid, T initial>
struct Reflector<NESStrongType<T, Tag, invalid, initial>>
{
    Reflected operator()(const NESStrongType<T, Tag, invalid, initial>& data) const { return reflect(data.getRawValue()); }
};

/// Unreflector for NESStrongType - unreflects to underlying type and constructs strong type
template <typename T, typename Tag, T invalid, T initial>
struct Unreflector<NESStrongType<T, Tag, invalid, initial>>
{
    NESStrongType<T, Tag, invalid, initial> operator()(const Reflected& data, const ReflectionContext& context) const
    {
        auto underlyingValue = context.unreflect<T>(data);
        return NESStrongType<T, Tag, invalid, initial>{underlyingValue};
    }
};

/// Reflector for NESStrongStringType - reflects the underlying string
template <typename Tag, StringLiteral Invalid>
struct Reflector<NESStrongStringType<Tag, Invalid>>
{
    Reflected operator()(const NESStrongStringType<Tag, Invalid>& data) const { return reflect(data.getRawValue()); }
};

/// Unreflector for NESStrongStringType - unreflects to string and constructs strong type
template <typename Tag, StringLiteral Invalid>
struct Unreflector<NESStrongStringType<Tag, Invalid>>
{
    NESStrongStringType<Tag, Invalid> operator()(const Reflected& data, const ReflectionContext& context) const
    {
        auto underlyingValue = context.unreflect<std::string>(data);
        return NESStrongStringType<Tag, Invalid>{std::move(underlyingValue)};
    }
};

/// Reflector for NESStrongUUIDType - reflects the underlying UUID as a string
template <typename Tag>
struct Reflector<NESStrongUUIDType<Tag>>
{
    Reflected operator()(const NESStrongUUIDType<Tag>& data) const { return reflect(data.getRawValue()); }
};

/// Unreflector for NESStrongUUIDType - unreflects from string and constructs strong type
template <typename Tag>
struct Unreflector<NESStrongUUIDType<Tag>>
{
    NESStrongUUIDType<Tag> operator()(const Reflected& data, const ReflectionContext& context) const
    {
        auto underlyingValue = context.unreflect<std::string>(data);
        return NESStrongUUIDType<Tag>{std::move(underlyingValue)};
    }
};

}
