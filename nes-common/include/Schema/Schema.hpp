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

#include <Identifiers/Identifier.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/TypeTraits.hpp>
#include <fmt/base.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>
#include <nameof.hpp>

namespace NES
{

/// This class represents a container of fields, where we calculate for every field by which names its addressable unambiguously.
/// Other than that, its interface is what you would expect of an std::vector or std::unordered_set of the specified field type.
/// That includes using it like a range, both consuming it as one or constructing it with std::ranges::to.
/// If you want to ensure that your schema has no name collisions, use tryCreateCollisionFree().
/// You can use any type as a field type, as long as it has a method getFullyQualifiedName() that returns a QualifiedIdentifierBase<N>.
template <typename FieldType, OrderType IsOrdered>
class Schema
{
    static constexpr size_t IdListExtent
        = NES::ExtractParameter<std::remove_cvref_t<decltype(std::declval<FieldType>().getFullyQualifiedName())>>::value;
    using IdList = QualifiedIdentifierBase<IdListExtent>;
    using FieldByNameType = std::unordered_map<IdList, FieldType>;
    using CollisionsType = std::unordered_map<IdList, std::vector<FieldType>>;

    using FieldContainer = std::conditional_t<IsOrdered.ordered, std::vector<FieldType>, std::unordered_multiset<FieldType>>;

    static std::pair<FieldByNameType, CollisionsType> initialize(const FieldContainer& fields);

public:
    Schema() = default;

    explicit Schema(FieldContainer fields);

    template <std::ranges::input_range Range>
    requires(
        std::same_as<FieldType, std::ranges::range_value_t<Range>> && !std::same_as<Range, std::initializer_list<FieldType>>
        && !std::same_as<Range, FieldContainer> && !std::same_as<Range, Schema>)
    explicit Schema(Range&& input);

    Schema(std::initializer_list<FieldType> fields);

    template <typename OtherFieldType>
    requires(IdListExtent == std::dynamic_extent)
    /// NOLINTNEXTLINE(google-explicit-constructor)
    Schema(const Schema<OtherFieldType, IsOrdered>& other);

    static std::expected<Schema, std::unordered_map<IdList, std::vector<FieldType>>> tryCreateCollisionFree(std::vector<FieldType> fields)
    requires(IsOrdered.ordered);

    static std::expected<Schema, std::unordered_map<IdList, std::vector<FieldType>>>
    tryCreateCollisionFree(const std::vector<FieldType>& fields)
    requires(!IsOrdered.ordered);

    static std::string createCollisionString(const std::unordered_map<IdList, std::vector<FieldType>>& collisions);

    /// Defined inline as hidden friends so that overload resolution can apply Schema's converting
    /// constructor (e.g. comparing Schema<UnboundFieldBase<1>> with Schema<UnboundFieldBase<dynamic_extent>>).
    /// A templated free-function operator== would require both sides to deduce the same FieldType
    /// and would fail those mixed comparisons.
    friend bool operator==(const Schema& lhs, const Schema& rhs) { return lhs.fields == rhs.fields; }

    friend std::ostream& operator<<(std::ostream& os, const Schema& obj)
    {
        return os << fmt::format("Schema<{},{}>: (fields: ({})", NAMEOF_TYPE(FieldType), IsOrdered.ordered, fmt::join(obj.fields, ", "));
    }

    [[nodiscard]] std::optional<FieldType> operator[](const IdList& name) const;

    [[nodiscard]] std::optional<FieldType> operator[](size_t index) const
    requires(IsOrdered.ordered);

    [[nodiscard]] std::optional<FieldType> getFieldByName(const IdList& fieldName) const;

    [[nodiscard]] bool contains(const IdList& qualifiedFieldName) const;

    [[nodiscard]] bool contains(const FieldType& field) const;

    [[nodiscard]] std::unordered_set<IdList> getUniqueFieldNames() const;

    [[nodiscard]] size_t getSizeInBytes() const;

    [[nodiscard]] auto begin() const -> decltype(std::declval<FieldContainer>().cbegin());

    [[nodiscard]] auto end() const -> decltype(std::declval<FieldContainer>().cend());

    [[nodiscard]] auto size() const -> decltype(std::declval<FieldContainer>().size());

private:
    FieldContainer fields;
    FieldByNameType fieldsByName;
    size_t sizeInBytes{};
};

template <typename FieldType, OrderType IsOrdered>
auto Schema<FieldType, IsOrdered>::initialize(const FieldContainer& fields) -> std::pair<FieldByNameType, CollisionsType>
{
    FieldByNameType fieldsByName{};
    CollisionsType collisions{};
    for (const auto& field : fields)
    {
        const auto& fullName = field.getFullyQualifiedName();
        for (size_t i = 0; i < std::ranges::size(fullName); i++)
        {
            auto idSubSpan = std::span<const Identifier, IdListExtent>{std::ranges::begin(fullName) + i, std::ranges::size(fullName) - i};
            IdList idSubList(idSubSpan);
            if (auto existingCollisions = collisions.find(idSubList); existingCollisions == collisions.end())
            {
                if (auto existingIdList = fieldsByName.find(idSubList); existingIdList != fieldsByName.end())
                {
                    collisions.insert(std::pair{std::move(idSubList), std::vector<FieldType>{existingIdList->second, field}});
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

template <typename FieldType, OrderType IsOrdered>
Schema<FieldType, IsOrdered>::Schema(FieldContainer fields) : fields(std::move(fields))
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

template <typename FieldType, OrderType IsOrdered>
template <std::ranges::input_range Range>
requires(
    std::same_as<FieldType, std::ranges::range_value_t<Range>> && !std::same_as<Range, std::initializer_list<FieldType>>
    && !std::same_as<Range, typename Schema<FieldType, IsOrdered>::FieldContainer> && !std::same_as<Range, Schema<FieldType, IsOrdered>>)
Schema<FieldType, IsOrdered>::Schema(Range&& input) : fields{std::forward<Range>(input) | std::ranges::to<FieldContainer>()}
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

template <typename FieldType, OrderType IsOrdered>
Schema<FieldType, IsOrdered>::Schema(std::initializer_list<FieldType> fields) : Schema{FieldContainer{std::move(fields)}}
{
}

template <typename FieldType, OrderType IsOrdered>
template <typename OtherFieldType>
requires(Schema<FieldType, IsOrdered>::IdListExtent == std::dynamic_extent)
Schema<FieldType, IsOrdered>::Schema(const Schema<OtherFieldType, IsOrdered>& other)
    : Schema(other | std::views::transform([](const auto& field) { return FieldType{field}; }) | std::ranges::to<FieldContainer>())
{
}

template <typename FieldType, OrderType IsOrdered>
auto Schema<FieldType, IsOrdered>::tryCreateCollisionFree(std::vector<FieldType> fields)
    -> std::expected<Schema, std::unordered_map<IdList, std::vector<FieldType>>>
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

template <typename FieldType, OrderType IsOrdered>
auto Schema<FieldType, IsOrdered>::tryCreateCollisionFree(const std::vector<FieldType>& fields)
    -> std::expected<Schema, std::unordered_map<IdList, std::vector<FieldType>>>
requires(!IsOrdered.ordered)
{
    auto fieldMultiSet = fields | std::ranges::to<FieldContainer>();
    auto [fieldsByName, collisions] = initialize(fieldMultiSet);
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
    return Schema{std::move(fieldMultiSet)};
}

template <typename FieldType, OrderType IsOrdered>
std::string Schema<FieldType, IsOrdered>::createCollisionString(const std::unordered_map<IdList, std::vector<FieldType>>& collisions)
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

template <typename FieldType, OrderType IsOrdered>
std::optional<FieldType> Schema<FieldType, IsOrdered>::operator[](const IdList& name) const
{
    auto iter = fieldsByName.find(name);
    if (iter == fieldsByName.end())
    {
        return std::nullopt;
    }
    return iter->second;
}

template <typename FieldType, OrderType IsOrdered>
std::optional<FieldType> Schema<FieldType, IsOrdered>::operator[](const size_t index) const
requires(IsOrdered.ordered)
{
    if (index < std::ranges::size(fields))
    {
        return fields.at(index);
    }
    return std::nullopt;
}

template <typename FieldType, OrderType IsOrdered>
std::optional<FieldType> Schema<FieldType, IsOrdered>::getFieldByName(const IdList& fieldName) const
{
    return (*this)[fieldName];
}

template <typename FieldType, OrderType IsOrdered>
bool Schema<FieldType, IsOrdered>::contains(const IdList& qualifiedFieldName) const
{
    return fieldsByName.contains(qualifiedFieldName);
}

template <typename FieldType, OrderType IsOrdered>
bool Schema<FieldType, IsOrdered>::contains(const FieldType& field) const
{
    return std::ranges::contains(fields, field);
}

template <typename FieldType, OrderType IsOrdered>
auto Schema<FieldType, IsOrdered>::getUniqueFieldNames() const -> std::unordered_set<IdList>
{
    auto namesView = this->fields | std::views::transform([](const FieldType& field) { return field.getFullyQualifiedName(); });
    return {namesView.begin(), namesView.end()};
}

template <typename FieldType, OrderType IsOrdered>
size_t Schema<FieldType, IsOrdered>::getSizeInBytes() const
{
    return sizeInBytes;
}

template <typename FieldType, OrderType IsOrdered>
auto Schema<FieldType, IsOrdered>::begin() const -> decltype(std::declval<FieldContainer>().cbegin())
{
    return fields.cbegin();
}

template <typename FieldType, OrderType IsOrdered>
auto Schema<FieldType, IsOrdered>::end() const -> decltype(std::declval<FieldContainer>().cend())
{
    return fields.cend();
}

template <typename FieldType, OrderType IsOrdered>
auto Schema<FieldType, IsOrdered>::size() const -> decltype(std::declval<FieldContainer>().size())
{
    return fields.size();
}

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

template <typename FieldType>
auto getOrderedFieldNames(const Schema<FieldType, Ordered>& schema)
{
    return schema | std::views::transform([](const auto& field) { return field.getFullyQualifiedName(); }) | std::ranges::to<std::vector>();
}

}

/// NOLINTBEGIN(cert-dcl58-cpp)
template <typename FieldType, NES::OrderType IsOrdered>
struct std::hash<NES::Schema<FieldType, IsOrdered>>
{
    using SchemaAlias = NES::Schema<FieldType, IsOrdered>;

    size_t operator()(const SchemaAlias& schema) const noexcept
    {
        if constexpr (IsOrdered.ordered)
        {
            return folly::hash::hash_range(schema.begin(), schema.end());
        }
        else
        {
            return folly::hash::commutative_hash_combine_range_generic(0, std::hash<FieldType>{}, schema.begin(), schema.end());
        }
    }
};

/// NOLINTEND(cert-dcl58-cpp)

/// Opt out of the generic range formatter from <fmt/ranges.h> so that the
/// Schema-specific ostream_formatter below is the only viable partial
/// specialization. Without this, both formatters match formatter<Schema, char>
/// (Schema is iterable and its element type is fmt::formattable), neither is
/// more specialized, and fmt falls back to the primary template — whose
/// default constructor is deleted, making Schema un-formattable.
template <typename FieldType, NES::OrderType IsOrdered, typename Char>
struct fmt::range_format_kind<NES::Schema<FieldType, IsOrdered>, Char>
    : std::integral_constant<fmt::range_format, fmt::range_format::disabled>
{
};

template <typename FieldType, NES::OrderType IsOrdered>
struct fmt::formatter<NES::Schema<FieldType, IsOrdered>> : fmt::ostream_formatter
{
};
