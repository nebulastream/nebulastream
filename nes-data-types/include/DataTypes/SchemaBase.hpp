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

#include <Util/Logger/Logger.hpp>


#include <algorithm>
#include <concepts>
#include <cstddef>
#include <functional>
#include <initializer_list>
#include <optional>
#include <ostream>
#include <ranges>
#include <span>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <DataTypes/SchemaBaseFwd.hpp>
#include <Identifiers/Identifier.hpp>
#include <Util/TypeTraits.hpp>
#include <bits/ranges_algo.h>
#include <fmt/base.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>

namespace NES
{


template <typename FieldType, OrderType IsOrdered>
class Schema
{
    static constexpr size_t IdListExtent
        = NES::ExtractParameter<std::remove_cvref_t<decltype(std::declval<FieldType>().getFullyQualifiedName())>>::value;
    using IdSpan = std::span<const Identifier, IdListExtent>;
    using IdList = IdentifierListBase<IdListExtent>;
    using FieldRef = FieldType;
    using FieldByNameRefsType = std::unordered_map<IdList, FieldRef>;
    using CollisionsRefType = std::unordered_map<IdList, std::vector<FieldRef>>;

    using FieldContainer = std::conditional_t<IsOrdered.ordered, std::vector<FieldType>, std::unordered_set<FieldType>>;

    static std::pair<FieldByNameRefsType, CollisionsRefType> initialize(const FieldContainer& fields)
    {
        FieldByNameRefsType fieldsByName{};
        CollisionsRefType collisions{};
        for (const auto& field : fields)
        {
            const auto& fullName = field.getFullyQualifiedName();
            for (size_t i = 0; i < std::ranges::size(fullName); i++)
            {
                IdSpan idSubSpan = IdSpan{std::ranges::begin(fullName) + i, std::ranges::size(fullName) - i};
                IdList idSubList(idSubSpan);
                if (auto existingCollisions = collisions.find(idSubList); existingCollisions == collisions.end())
                {
                    if (auto existingIdList = fieldsByName.find(idSubList); existingIdList != fieldsByName.end())
                    {
                        collisions.insert(std::pair{std::move(idSubList), std::vector<FieldRef>{existingIdList->second, field}});
                        fieldsByName.erase(existingIdList);
                    }
                    else
                    {
                        fieldsByName.insert(std::pair{std::move(idSubList), field});
                    }
                }
                else
                {
                    existingCollisions->second.push_back(field);
                }
            }
        }
        return std::pair{fieldsByName, collisions};
    }

public:
    explicit Schema(FieldContainer fields) : fields(std::move(fields))
    {
        auto [fieldsByName, collisions] = initialize(this->fields);
        if (!collisions.empty())
        {
            NES_DEBUG("Duplicate identifiers in schema: {}", fmt::join(collisions, ", "));
        }
        this->fieldsByName = fieldsByName;
        sizeInBytes = std::ranges::fold_left(
            this->fields, 0, [](size_t acc, const auto& field) { return acc + field.getDataType().getSizeInBytesWithNull(); });
    }

    template <std::ranges::input_range Range>
    requires(
        std::same_as<FieldType, std::ranges::range_value_t<Range>> && !std::same_as<Range, std::initializer_list<FieldType>>
        && !std::same_as<Range, FieldContainer> && !std::same_as<Range, Schema>)
    explicit Schema(Range&& input) noexcept : fields{std::forward<Range>(input) | std::ranges::to<FieldContainer>()}
    {
        auto [calculatedFieldsByName, collisions] = initialize(fields);
        if (!collisions.empty())
        {
            NES_DEBUG("Duplicate identifiers in schema: {}", fmt::join(collisions, ", "));
        }
        this->fieldsByName = std::move(calculatedFieldsByName);
        sizeInBytes = std::ranges::fold_left(
            this->fields, 0, [](size_t acc, const auto& field) { return acc + field.getDataType().getSizeInBytesWithNull(); });
    }

    Schema(std::initializer_list<FieldType> fields) : Schema{FieldContainer{std::move(fields)}} { }

    static std::expected<Schema, std::unordered_map<IdList, std::vector<FieldType>>> tryCreateCollisionFree(std::vector<FieldType> fields)
    requires(IsOrdered.ordered)
    {
        auto [fieldsByName, collisions] = initialize(fields);
        if (!collisions.empty())
        {
            std::unordered_map<IdList, std::vector<FieldType>> collisionMap;
            for (const auto& [idSpan, fieldRefs] : collisions)
            {
                std::vector<FieldType> fieldVec;
                fieldVec.reserve(fieldRefs.size());
                for (const auto& fieldRef : fieldRefs)
                {
                    fieldVec.push_back(fieldRef);
                }
                collisionMap.emplace(IdList{idSpan}, std::move(fieldVec));
            }
            return std::unexpected{std::move(collisionMap)};
        }
        return Schema{std::move(fields)};
    }

    static std::expected<Schema, std::unordered_map<IdList, std::vector<FieldType>>>
    tryCreateCollisionFree(const std::vector<FieldType>& fields)
    requires(!IsOrdered.ordered)
    {
        auto fieldSet = fields | std::ranges::to<std::unordered_set<FieldType>>();
        if (fieldSet.size() != fields.size())
        {
            auto collisions = fieldSet
                | std::views::transform([&fields](const auto& field) { return std::pair{field, std::ranges::count(fields, field)}; })
                | std::views::filter([](const auto& pair) { return pair.second > 1; })
                | std::views::transform(
                                  [](const auto& pair)
                                  {
                                      return std::pair{
                                          pair.first.getFullyQualifiedName(),
                                          std::views::repeat(pair.first, pair.second) | std::ranges::to<std::vector>()};
                                  })
                | std::ranges::to<std::unordered_map<IdList, std::vector<FieldType>>>();
            return std::unexpected{collisions};
        }
        auto [fieldsByName, collisions] = initialize(fieldSet);
        if (!collisions.empty())
        {
            std::unordered_map<IdList, std::vector<FieldType>> collisionMap;
            for (const auto& [idSpan, fieldRefs] : collisions)
            {
                std::vector<FieldType> fieldVec;
                fieldVec.reserve(fieldRefs.size());
                for (const auto& fieldRef : fieldRefs)
                {
                    fieldVec.push_back(fieldRef);
                }
                collisionMap.emplace(IdList{idSpan}, std::move(fieldVec));
            }
            return std::unexpected{std::move(collisionMap)};
        }
        return Schema{std::move(fieldSet)};
    }

    static std::string createCollisionString(const std::unordered_map<IdList, std::vector<FieldType>>& collisions)
    {
        return fmt::format(
            "{}",
            fmt::join(
                collisions
                    | std::views::transform(
                        [](const auto& pair)
                        {
                            return fmt::format(
                                "{} : ({})",
                                pair.first,
                                fmt::join(
                                    pair.second
                                        | std::views::transform(
                                            [](const FieldType& field) -> std::string
                                            { return fmt::format("{}:{}", field.getFullyQualifiedName(), field.getDataType()); }),
                                    ", "));
                        }),
                ", "));
    }

    Schema() = default;

    template <typename OtherFieldType>
    requires(IdListExtent == std::dynamic_extent)
    /// NOLINTNEXTLINE(google-explicit-constructor)
    Schema(const Schema<OtherFieldType, IsOrdered>& other)
        : Schema(other | std::views::transform([](const auto& field) { return FieldType{field}; }) | std::ranges::to<FieldContainer>())
    {
    }

    friend bool operator==(const Schema& lhs, const Schema& rhs) { return lhs.fields == rhs.fields; }

    friend std::ostream& operator<<(std::ostream& os, const Schema& obj)
    {
        return os << fmt::format(
                   "Schema<{},{},{}>: (fields: ({})",
                   IdListExtent,
                   typeid(FieldType).name(),
                   IsOrdered.ordered,
                   fmt::join(obj.fields, ", "));
    }

    [[nodiscard]] std::optional<FieldType> operator[](const IdList& name) const
    {
        auto iter = fieldsByName.find(name);
        if (iter == fieldsByName.end())
        {
            return std::nullopt;
        }
        return iter->second;
    }

    [[nodiscard]] std::optional<FieldType> operator[](const size_t index) const
    requires(IsOrdered.ordered)
    {
        if (index < std::ranges::size(fields))
        {
            return fields.at(index);
        }
        return std::nullopt;
    }

    [[nodiscard]] std::optional<FieldType> getFieldByName(const IdList& fieldName) const { return (*this)[fieldName]; }

    bool contains(const IdList& qualifiedFieldName) const { return fieldsByName.contains(qualifiedFieldName); }

    bool contains(const FieldType& field) const
    {
        if (const auto iter = fieldsByName.find(field.getFullyQualifiedName()); iter != fieldsByName.end())
        {
            return iter->second == field;
        }
        return false;
    }

    std::vector<IdList> getUniqueFieldNames() const
    {
        auto namesView = this->fields | std::views::transform([](const FieldType& field) { return field.getFullyQualifiedName(); });
        return {namesView.begin(), namesView.end()};
    }

    [[nodiscard]] size_t getSizeInBytes() const { return sizeInBytes; }

    [[nodiscard]] auto begin() const -> decltype(std::declval<FieldContainer>().cbegin()) { return fields.cbegin(); }

    [[nodiscard]] auto end() const -> decltype(std::declval<FieldContainer>().cend()) { return fields.cend(); }

    [[nodiscard]] auto size() const -> decltype(std::declval<FieldContainer>().size()) { return fields.size(); }

private:
    FieldContainer fields;
    FieldByNameRefsType fieldsByName;
    size_t sizeInBytes{};
};

template <typename FieldType, OrderType IsOrdered>
struct Reflector<Schema<FieldType, IsOrdered>>
{
    Reflected operator()(const Schema<FieldType, IsOrdered>& schema) const { return reflect(schema | std::ranges::to<std::vector>()); }
};

template <typename FieldType, OrderType IsOrdered>
struct Unreflector<Schema<FieldType, IsOrdered>>
{
    Schema<FieldType, IsOrdered> operator()(const Reflected& data, const ReflectionContext& context) const
    {
        return context.unreflect<std::vector<FieldType>>(data) | std::ranges::to<Schema<FieldType, IsOrdered>>();
    }
};

}

/// NOLINTBEGIN(cert-dcl58-cpp)
template <typename FieldType, NES::OrderType IsOrdered>
struct std::hash<NES::Schema<FieldType, IsOrdered>>
{
    using SchemaAlias = NES::Schema<FieldType, IsOrdered>;

    size_t operator()(const SchemaAlias& schema)
    {
        if constexpr (IsOrdered.ordered)
        {
            return folly::hash::hash_range(schema.begin(), schema.end());
        }
        else
        {
            return folly::hash::commutative_hash_combine_range(schema.begin(), schema.end());
        }
    }
};

/// NOLINTEND(cert-dcl58-cpp)

template <typename FieldType, NES::OrderType IsOrdered>
struct fmt::formatter<NES::Schema<FieldType, IsOrdered>> : fmt::ostream_formatter
{
};
