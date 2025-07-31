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
#include <Traits/FieldMappingTrait.hpp>

#include <cstddef>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <typeinfo>
#include <unordered_map>
#include <utility>
#include <Util/Reflection.hpp>

#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <folly/hash/Hash.h>

namespace NES
{
namespace
{
inline constexpr auto CONFIG_KEY = "fieldMapping";

}

FieldMappingTrait::FieldMappingTrait(std::unordered_map<UnqualifiedUnboundField, IdentifierList> mapping) : mapping(std::move(mapping))
{
}

std::optional<IdentifierList> FieldMappingTrait::getMapping(const UnqualifiedUnboundField& field) const
{
    const auto iter = mapping.find(field);
    if (iter == mapping.end())
    {
        return std::nullopt;
    }
    return iter->second;
}

const std::unordered_map<UnqualifiedUnboundField, IdentifierList>& FieldMappingTrait::getUnderlying() const
{
    return mapping;
}

const std::type_info& FieldMappingTrait::getType()
{
    return typeid(FieldMappingTrait);
}

std::string_view FieldMappingTrait::getName()
{
    return NAME;
}

Reflected Reflector<FieldMappingTrait>::operator()(const FieldMappingTrait& trait) const
{
    return reflect(trait.mapping);
}

FieldMappingTrait Unreflector<FieldMappingTrait>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto mapping = context.unreflect<std::unordered_map<UnqualifiedUnboundField, IdentifierList>>(reflected);
    return FieldMappingTrait{std::move(mapping)};
}

bool FieldMappingTrait::operator==(const FieldMappingTrait& other) const
{
    return this->mapping == other.mapping;
}

std::string FieldMappingTrait::explain(ExplainVerbosity) const
{
    return fmt::format(
        "FieldMappingTrait({})",
        fmt::join(
            mapping
                | std::views::transform([](const auto& pair)
                                        { return fmt::format("{} -> {}", pair.first.getFullyQualifiedName(), pair.second); }),
            ", "));
}

size_t FieldMappingTrait::hash() const
{
    /// NOLINTBEGIN(readability-magic-numbers)
    return folly::hash::commutative_hash_combine_range_generic(13, folly::hash::StdHasher{}, mapping.begin(), mapping.end());
    /// NOLINTEND(readability-magic-numbers)
}

}
