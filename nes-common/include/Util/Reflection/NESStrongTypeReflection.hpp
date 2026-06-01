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
#include <Util/Reflection/ReflectionCore.hpp>

/**
 * Adds NESStrongType overloads for the reflection-based serialization.
 * This allows Reflector/Unreflector to serialize and deserialize identifiers
 * by mapping them to/from their underlying types.
 */
namespace NES
{

template <typename T, typename Tag, T Invalid, T Initial>
struct Reflector<NESStrongType<T, Tag, Invalid, Initial>>
{
    Reflected operator()(const NESStrongType<T, Tag, Invalid, Initial>& data, const ReflectionContext& context) const
    {
        return context.reflect(data.getRawValue());
    }
};

template <typename T, typename Tag, T Invalid, T Initial>
struct Unreflector<NESStrongType<T, Tag, Invalid, Initial>>
{
    NESStrongType<T, Tag, Invalid, Initial> operator()(const Reflected& data, const ReflectionContext& context) const
    {
        return NESStrongType<T, Tag, Invalid, Initial>{context.unreflect<T>(data)};
    }
};

template <typename Tag, StringLiteral Invalid>
struct Reflector<NESStrongStringType<Tag, Invalid>>
{
    Reflected operator()(const NESStrongStringType<Tag, Invalid>& data, const ReflectionContext& context) const
    {
        return context.reflect(data.getRawValue());
    }
};

template <typename Tag, StringLiteral Invalid>
struct Unreflector<NESStrongStringType<Tag, Invalid>>
{
    NESStrongStringType<Tag, Invalid> operator()(const Reflected& data, const ReflectionContext& context) const
    {
        return NESStrongStringType<Tag, Invalid>{context.unreflect<std::string>(data)};
    }
};

template <typename Tag>
struct Reflector<NESStrongUUIDType<Tag>>
{
    Reflected operator()(const NESStrongUUIDType<Tag>& data, const ReflectionContext& context) const
    {
        return context.reflect(data.getRawValue());
    }
};

template <typename Tag>
struct Unreflector<NESStrongUUIDType<Tag>>
{
    NESStrongUUIDType<Tag> operator()(const Reflected& data, const ReflectionContext& context) const
    {
        return NESStrongUUIDType<Tag>{context.unreflect<std::string>(data)};
    }
};

}
