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
#include <yaml-cpp/yaml.h>

/**
 * Adds NESStrongType overloads for the yaml library.
 * This allows assignements of Yaml values with identifiers. This also directly enables Identifiers to be used within the
 * Configuration classes, e.g. `ScalarOption<WorkerId>`
 */
namespace YAML
{
/// Not possible to implement via `YAML::convert<T>()` since that is called by `as_if` which default-constructs T.
/// Thus, we implement a specialized `as_if`.
template <typename T, typename Tag, T invalid, T initial>
struct as_if<NES::NESStrongType<T, Tag, invalid, initial>, void>
{
    explicit as_if(const Node& node_) : node(node_) { }

    const Node& node;

    NES::NESStrongType<T, Tag, invalid, initial> operator()() const
    {
        if (!node.m_pNode)
        {
            throw TypedBadConversion<T>(node.Mark());
        }

        return NES::NESStrongType<T, Tag, invalid, initial>{node.as<T>()};
    }
};

template <typename Tag, NES::StringLiteral invalid>
struct as_if<NES::NESStrongStringType<Tag, invalid>, void>
{
    explicit as_if(const Node& node_) : node(node_) { }

    const Node& node;

    NES::NESStrongStringType<Tag, invalid> operator()() const
    {
        if (!node.m_pNode)
        {
            throw TypedBadConversion<std::string>(node.Mark());
        }

        return NES::NESStrongStringType<Tag, invalid>{node.as<std::string>()};
    }
};

template <typename Tag>
struct as_if<NES::NESStrongUUIDType<Tag>, void>
{
    explicit as_if(const Node& node_) : node(node_) { }

    const Node& node;

    NES::NESStrongUUIDType<Tag> operator()() const
    {
        if (!node.m_pNode)
        {
            throw TypedBadConversion<std::string>(node.Mark());
        }

        return NES::NESStrongUUIDType<Tag>{node.as<std::string>()};
    }
};
}
