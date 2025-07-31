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

#include "DataTypeProvider.hpp"
#include "ErrorHandling.hpp"


#include <cstddef>
#include <functional>
#include <numeric>
#include <ostream>
#include <span>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/SchemaBaseFwd.hpp>
#include <Identifiers/Identifier.hpp>
#include <Util/Logger/Formatter.hpp>
#include <fmt/ranges.h>
#include <folly/Hash.h>
#include "Util/TypeTraits.hpp"

namespace NES
{

///If it is a schema for bound field, id list extent must be dynamic since in the future we'd be implicitly storing relation aliases
template <typename FieldType, bool Ordered>
// requires (FieldConcept<FieldType, IdListExtent>)
class SchemaBase
{
    static constexpr size_t IdListExtent = NES::ExtractParameter<std::remove_cvref_t<decltype(std::declval<FieldType>().getFullyQualifiedName())>>::value;
    // static constexpr size_t IdListExtent = detail::extract<FieldType, IdListExtent>(FieldType{});
    // using FieldType = FieldTypeTemplate<IdListExtent>;
    using IdSpan = std::span<const Identifier, IdListExtent>;
    using IdList = IdentifierListBase<IdListExtent>;
    using FieldRef = std::reference_wrapper<const FieldType>;
    // using FieldByNameRefsType = std::unordered_map<IdSpan, FieldRef, std::hash<IdSpan>, IdentifierList::SpanEquals<IdListExtent>>;
    using FieldByNameRefsType = std::unordered_map<IdList, FieldRef>;
    // using CollisionsRefType
    //     = std::unordered_map<IdSpan, std::vector<FieldRef>, std::hash<IdSpan>, IdentifierList::SpanEquals<IdListExtent>>;
    using CollisionsRefType = std::unordered_map<IdList, std::vector<FieldRef>>;

    using FieldContainer = std::conditional_t<Ordered, std::vector<FieldType>, std::unordered_set<FieldType>>;

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
                IdList idSubList{idSubSpan};
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
    // explicit SchemaBase(std::initializer_list<UnboundFieldBase<MaxIdentifierSize>> fields);
    explicit SchemaBase(FieldContainer fields) : fields(std::move(fields))
    {
        auto [fieldsByName, collisions] = initialize(this->fields);
        if (!collisions.empty())
        {
            NES_DEBUG("Duplicate identifiers in schema: {}", fmt::join(collisions, ", "));
        }
        this->fieldsByName = fieldsByName;
        /*| std::views::transform(
                                 [](const auto& pair)
                                 {
                                     return std::pair<const IdentifierListBase<IdListExtent>, FieldRef>{
                                         IdentifierListBase<IdListExtent>{pair.first}, pair.second};
                                 })*/
        // | std::ranges::to<const std::unordered_map>();
        sizeInBytes = std::ranges::fold_left(
            this->fields, 0, [](size_t acc, const auto& field) { return acc + field.getDataType().getSizeInBytes(); });
    }

    template <std::ranges::input_range Range>
    requires(
        std::same_as<FieldType, std::ranges::range_value_t<Range>> && !std::same_as<Range, std::initializer_list<FieldType>>
        && !std::same_as<Range, FieldContainer> && !std::same_as<Range, SchemaBase>)
    explicit SchemaBase(Range&& input) noexcept : fields{input | std::ranges::to<FieldContainer>()}
    {
        auto [fieldsByName, collisions] = initialize(fields);
        if (!collisions.empty())
        {
            NES_DEBUG("Duplicate identifiers in schema: {}", fmt::join(collisions, ", "));
        }
        this->fieldsByName = fieldsByName;
        // | std::views::transform([](const auto& pair) { return std::pair{IdentifierListBase<IdListExtent>{pair.first, pair.second}; })
        // | std::ranges::to<std::unordered_map>();
        sizeInBytes = std::ranges::fold_left(
            this->fields, 0, [](size_t acc, const auto& field) { return acc + field.getDataType().getSizeInBytes(); });
    }

    SchemaBase(std::initializer_list<FieldType> fields) : SchemaBase{FieldContainer{std::move(fields)}} { }

    SchemaBase(const SchemaBase& other) : SchemaBase(other.fields) { }

    SchemaBase(SchemaBase&& other) noexcept = default;

    SchemaBase& operator=(const SchemaBase& other)
    {
        if (this == &other)
        {
            return *this;
        }
        fields = other.fields;
        auto [fieldsByName, collisions] = initialize(fields);
        if (!collisions.empty())
        {
            NES_DEBUG("Duplicate identifiers in schema: {}", fmt::join(collisions, ", "));
        }
        this->fieldsByName = fieldsByName;
        // | std::views::transform([](const auto& pair) { return std::pair{IdentifierListBase<IdListExtent>{pair.first, pair.second}; })
        // | std::ranges::to<std::unordered_map>();
        sizeInBytes = std::ranges::fold_left(
            this->fields, 0, [](size_t acc, const auto& field) { return acc + field.getDataType().getSizeInBytes(); });
        return *this;
    }

    SchemaBase& operator=(SchemaBase&& other) noexcept = default;

    static std::expected<SchemaBase, std::unordered_map<IdList, std::vector<FieldType>>>
    tryCreateCollisionFree(std::vector<FieldType> fields)
    requires(Ordered)
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
                    fieldVec.push_back(fieldRef.get());
                }
                collisionMap.emplace(IdList{idSpan}, std::move(fieldVec));
            }
            return std::unexpected{std::move(collisionMap)};
        }
        return SchemaBase{std::move(fields)};
    }

    static std::expected<SchemaBase, std::unordered_map<IdList, std::vector<FieldType>>>
    tryCreateCollisionFree(const std::vector<FieldType>& fields)
    requires(!Ordered)
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
                    fieldVec.push_back(fieldRef.get());
                }
                collisionMap.emplace(IdList{idSpan}, std::move(fieldVec));
            }
            return std::unexpected{std::move(collisionMap)};
        }
        return SchemaBase{std::move(fieldSet)};
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

    // explicit SchemaBase(const std::initializer_list<UnboundFieldBase<IdListExtent>> fields) : SchemaBase{std::vector(fields)}
    // {
    // }

    SchemaBase() = default;

    template <typename OtherFieldType>
    requires(IdListExtent == std::dynamic_extent)
    explicit SchemaBase(const SchemaBase<OtherFieldType, Ordered>& other)
        : SchemaBase(other | std::views::transform([](const auto& field) { return FieldType{field}; }) | std::ranges::to<FieldContainer>())
    {
    }

    friend bool operator==(const SchemaBase& lhs, const SchemaBase& rhs) { return lhs.fields == rhs.fields; }

    friend std::ostream& operator<<(std::ostream& os, const SchemaBase& obj)
    {
        return os << fmt::format(
                   "SchemaBase<{},{},{}>: (fields: ({})", IdListExtent, typeid(FieldType).name(), Ordered, fmt::join(obj.fields, ", "));
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
    requires(Ordered)
    {
        if (index < std::ranges::size(fields))
        {
            return fields.at(index);
        }
        return std::nullopt;
    }

    [[nodiscard]] std::optional<FieldType> getFieldByName(const IdList& fieldName) const { return (*this)[fieldName]; }

    bool contains(const IdList& qualifiedFieldName) const { return fieldsByName.contains(qualifiedFieldName); }

    std::vector<IdList> getUniqueFieldNames() const
    {
        auto namesView = this->fields | std::views::transform([](const FieldType& field) { return field.getLastName(); });
        return {namesView.begin(), namesView.end()};
    }

    size_t getSizeInBytes() const { return sizeInBytes; }

    [[nodiscard]] auto begin() const -> decltype(std::declval<FieldContainer>().cbegin()) { return fields.cbegin(); }

    [[nodiscard]] auto end() const -> decltype(std::declval<FieldContainer>().cend()) { return fields.cend(); }

    [[nodiscard]] auto size() const -> decltype(std::declval<FieldContainer>().size()) { return fields.size(); }

private:
    FieldContainer fields;
    FieldByNameRefsType fieldsByName;
    size_t sizeInBytes;
};

}

template <typename FieldType, bool Ordered>
struct std::hash<NES::SchemaBase<FieldType, Ordered>>
{
    using SchemaAlias = NES::SchemaBase<FieldType, Ordered>;

    size_t operator()(const SchemaAlias& schema)
    {
        if constexpr (Ordered)
        {
            return folly::hash::hash_range(schema.begin(), schema.end());
        }
        else
        {
            return folly::hash::commutative_hash_combine_range(schema.begin(), schema.end());
        }
    }
};

template <typename FieldType, bool Ordered>
struct fmt::formatter<NES::SchemaBase<FieldType, Ordered>> : fmt::ostream_formatter
{
};
