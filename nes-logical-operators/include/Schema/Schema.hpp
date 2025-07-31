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

#include <Identifiers/Identifier.hpp>


#include <concepts>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <optional>
#include <ostream>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Schema/Field.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Ranges.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

// class Schema
// {
// public:
// private:
//     using FieldRef = std::reference_wrapper<const Field>;
//
//     //take const reference to make sure that the spans don't dangle
//     std::pair<std::unordered_map<Identifier, FieldRef>, std::unordered_map<Identifier, std::vector<FieldRef>>> static initializeFields(
//         const std::unordered_set<Field>& fields) noexcept
//     {
//         std::unordered_map<Identifier, FieldRef> fieldsByName{};
//         std::unordered_map<Identifier, std::vector<FieldRef>> collisions{};
//         for (const auto& field : fields)
//         {
//             const auto& name = field.getLastName();
//             auto collisionIter = collisions.find(name);
//             if (collisionIter == collisions.end())
//             {
//                 if (auto existingIdList = fieldsByName.find(name); existingIdList != fieldsByName.end())
//                 {
//                     collisions.insert(std::pair{name, std::vector<FieldRef>{existingIdList->second, field}});
//                     fieldsByName.erase(existingIdList);
//                 }
//                 else
//                 {
//                     fieldsByName.insert(std::pair{name, field});
//                 }
//             }
//             else
//             {
//                 collisionIter->second.push_back(field);
//             }
//         }
//         if (!collisions.empty())
//         {
//             NES_DEBUG(
//                 "Duplicate identifiers in schema: {}",
//                 fmt::join(collisions | std::views::transform([](const auto& pair) { return pair.first; }), ", "));
//         }
//         return std::pair{std::move(fieldsByName), std::move(collisions)};
//     }
//
//     struct Private
//     {
//     };
//
//     Schema(Private, std::unordered_set<Field> fields, std::unordered_map<IdentifierList, FieldRef> nameToFields) noexcept
//         : fields(std::move(fields)), nameToField(std::move(nameToFields))
//     {
//     }
//
//
// public:
//     /// Enum to identify the memory layout in which we want to represent the schema physically.
//     struct QualifiedFieldName
//     {
//         explicit QualifiedFieldName(std::string streamName, std::string fieldName)
//             : streamName(std::move(streamName)), fieldName(std::move(fieldName))
//         {
//             PRECONDITION(not this->streamName.empty(), "Cannot create a QualifiedFieldName with an empty field name");
//             PRECONDITION(not this->fieldName.empty(), "Cannot create a QualifiedFieldName with an empty field name");
//         }
//
//         std::string streamName;
//         std::string fieldName;
//     };
//
//     Schema() = default;
//     Schema(const Schema& other) = default;
//     Schema(Schema&& other) noexcept = default;
//     Schema& operator=(const Schema& other) = default;
//     Schema& operator=(Schema&& other) noexcept = default;
//
//     Schema(std::initializer_list<Field> fields);
//     explicit Schema(std::unordered_set<Field> fields);
//
//     //template overloading on input ranges for fields and schemas which require separate handling
//     //Since std::initializer_list also implements std::input_range we need to make sure we don't overload it and exclude it
//     //While we are implementing them in the header,
//     //we should explicitly instantiate as many of the used specializations as possible at the end of the cpp to reduce compile times
//     template <std::ranges::input_range Range>
//     requires(
//         std::same_as<Field, std::ranges::range_value_t<Range>> && !std::same_as<Range, std::initializer_list<Field>>
//         && !std::same_as<Range, std::vector<Field>> && !std::same_as<Range, Schema>)
//     explicit Schema(Range&& input) noexcept : fields{input | std::ranges::to<std::unordered_set>()}
//     {
//         auto [fieldsByName, collisions] = initializeFields(this->fields);
//         nameToField
//             = fieldsByName
//             | std::views::transform(
//                   [](auto pair) { return std::make_pair<IdentifierList, FieldRef>(IdentifierList{pair.first}, std::move(pair.second)); })
//             | std::ranges::to<std::unordered_map<IdentifierList, FieldRef>>();
//     }
//
//     template <std::ranges::input_range Range>
//     requires(std::same_as<Field, std::ranges::range_value_t<Range>> && !std::same_as<Range, std::initializer_list<Field>>)
//     static std::expected<Schema, std::unordered_map<IdentifierList, std::vector<Field>>> tryCreateCollisionFree(Range input) noexcept
//     {
//         auto fields = input | std::ranges::to<std::unordered_set>();
//         auto [fieldsByName, collisions] = initializeFields(fields);
//         if (collisions.size() > 0)
//         {
//             return std::unexpected{
//                 std::views::all(collisions)
//                 | std::views::transform(
//                     [](auto& pair)
//                     {
//                         return std::make_pair(
//                             IdentifierList{pair.first},
//                             pair.second | std::views::transform([](auto& ref) { return ref.get(); })
//                                 | std::ranges::to<std::vector<Field>>());
//                     })
//                 | std::ranges::to<std::unordered_map<IdentifierList, std::vector<Field>>>()};
//         }
//         auto nameToField = fieldsByName
//             | std::views::transform([](auto pair) { return std::pair{IdentifierList{pair.first}, pair.second}; })
//             | std::ranges::to<std::unordered_map<IdentifierList, FieldRef>>();
//         return Schema(Private{}, std::move(fields), std::move(nameToField));
//     }
//
//     ~Schema() = default;
//
//     bool operator==(const Schema& other) const;
//     friend std::ostream& operator<<(std::ostream& os, const Schema& schema);
//
//     /// @brief Returns the attribute field based on a qualified or unqualified field name.
//     /// If an unqualified field name is given (e.g., `getFieldByName("fieldName")`), the function will match attribute fields with any source name.
//     /// If a qualified field name is given (e.g., `getFieldByName("source$fieldName")`), the entire qualified field must match.
//     /// Note that this function does not return a field with an ambiguous field name.
//     ///
//     /// Note: we currently don't support relation aliases, so passing IdentifierLists
//     [[nodiscard]] std::optional<Field> getFieldByName(const IdentifierList& fieldName) const;
//
//     bool contains(const IdentifierList& qualifiedFieldName) const;
//
//     std::vector<IdentifierList> getUniqueFieldNames() const&;
//     [[nodiscard]] const std::unordered_set<Field>& getFields() const;
//
//     auto begin() const -> decltype(std::declval<const std::unordered_set<Field>>().cbegin()) { return fields.cbegin(); }
//
//     auto end() const -> decltype(std::declval<const std::unordered_set<Field>>().cend()) { return fields.cend(); }
//
//     size_t size() const;
//
//     static std::string createCollisionString(const std::unordered_map<IdentifierList, std::vector<Field>>& collisions);
//
//     /// TODO template this method over IdListExtent when adding either named relations or compound types
//     SchemaBase<UnboundFieldBase<1>, false> unbind() const
//     {
//         return SchemaBase{*this | std::views::transform(Field::unbinder()) | std::ranges::to<std::vector>()};
//     }
//
//     static Schema bind(LogicalOperator logicalOperator, SchemaBase<UnboundFieldBase<1>, false> unboundSchema);
//
// private:
//     /// This vector of fields does not imply an order of fields in the schema, it is just used to maintain stable references for the name aliases
//     std::unordered_set<Field> fields;
//     /// References to unordered set entries are stable until the element is erased, which this class doesn't permit anyway
//     std::unordered_map<IdentifierList, FieldRef> nameToField;
// };
//
// inline size_t Schema::size() const
// {
//     return fields.size();
// }

using
Schema = SchemaBase<Field, false>;
static_assert(std::ranges::range<Schema>);
static_assert(std::same_as<std::ranges::range_value_t<Schema>, Field>);

inline SchemaBase<UnboundFieldBase<1>, false> unbind(const Schema& schema)
{
    return schema | std::views::transform(Field::unbinder()) | std::ranges::to<SchemaBase<UnboundFieldBase<1>, false>>();
}

template <bool Ordered>
Schema bind(const LogicalOperator& logicalOperator, const SchemaBase<UnboundFieldBase<1>, Ordered>& unboundSchema);



}

FMT_OSTREAM(NES::Schema);
