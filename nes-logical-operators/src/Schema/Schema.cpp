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

#include <concepts>
#include <cstddef>
#include <initializer_list>
#include <iostream>
#include <memory>
#include <numeric>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include "Operators/LogicalOperator.hpp"

namespace NES
{
// Schema::Schema(std::unordered_set<Field> fields) : fields(std::move(fields))
// {
//     auto [fieldsByName, collisions] = initializeFields(this->fields);
//     nameToField = fieldsByName
//         | std::views::transform([](auto& pair)
//                                 { return std::pair<IdentifierList, FieldRef>{IdentifierList{pair.first}, pair.second}; })
//         | std::ranges::to<std::unordered_map<IdentifierList, FieldRef>>();
// }
//
// Schema::Schema(std::initializer_list<Field> fields) : fields(std::move(fields))
// {
//     auto [fieldsByName, collisions] = initializeFields(this->fields);
//     nameToField = fieldsByName
//         | std::views::transform([](auto pair)
//                                 { return std::pair<IdentifierList, FieldRef>{IdentifierList{pair.first}, pair.second}; })
//         | std::ranges::to<std::unordered_map<IdentifierList, FieldRef>>();
// }
//
//
// std::optional<Field> Schema::getFieldByName(const IdentifierList& fieldName) const
// {
//     if (const auto found = nameToField.find(fieldName); found != nameToField.end())
//     {
//         return found->second.get();
//     }
//     return std::nullopt;
// }
//
//
// std::ostream& operator<<(std::ostream& os, const Schema& schema)
// {
//     os << fmt::format("Schema(fields({}))", fmt::join(schema.fields, ","));
//     return os;
// }
//
// const std::unordered_set<Field>& Schema::getFields() const
// {
//     return fields;
// }
//
// bool Schema::contains(const IdentifierList& qualifiedFieldName) const
// {
//     return nameToField.contains(qualifiedFieldName);
// }
//
// bool Schema::operator==(const Schema& other) const
// {
//     return this->fields == other.fields;
// }
//
// std::vector<IdentifierList> Schema::getUniqueFieldNames() const&
// {
//     auto namesView = this->fields | std::views::transform([](const Field& field) { return field.getLastName(); });
//     return {namesView.begin(), namesView.end()};
// }
//
// std::string Schema::createCollisionString(const std::unordered_map<IdentifierList, std::vector<Field>>& collisions)
// {
//     return fmt::format(
//         "{}",
//         fmt::join(
//             collisions
//                 | std::views::transform(
//                     [](const auto& pair)
//                     {
//                         return fmt::format(
//                             "{} : ({})",
//                             pair.first,
//                             fmt::join(
//                                 pair.second
//                                     | std::views::transform(
//                                         [](const Field& field) -> std::string { return fmt::format("{}", field.getProducedBy().getId()); }),
//                                 ", "));
//                     }),
//             ", "));
// }
//
// // Schema::operator SchemaBase<UnboundFieldBase<1>, false>() const
// // {
// //     return SchemaBase{
// //         *this | std::views::transform([](const Field& field) { return UnboundFieldBase<1>{field.getLastName(), field.getDataType()}; })
// //         | std::ranges::to<std::vector>()};
// // }
//
// Schema Schema::bind(LogicalOperator logicalOperator, SchemaBase<UnboundFieldBase<1>, false> unboundSchema)
// {
//     return unboundSchema
//         | std::views::transform([&logicalOperator](const auto& unboundField)
//                                 { return Field{logicalOperator, unboundField.getName(), unboundField.getDataType()}; })
//         | std::ranges::to<Schema>();
// }
//
//
// template Schema::Schema(const std::vector<Field>& input) noexcept;

template <>
Schema bind(const LogicalOperator& logicalOperator, const SchemaBase<UnboundFieldBase<1>, true>& unboundSchema)
{
    return unboundSchema | std::views::transform(Field::binder(logicalOperator)) | std::ranges::to<Schema>();
}

template <>
Schema bind(const LogicalOperator& logicalOperator, const SchemaBase<UnboundFieldBase<1>, false>& unboundSchema)
{
    return unboundSchema | std::views::transform(Field::binder(logicalOperator)) | std::ranges::to<Schema>();
}

// template Schema bind<true>(const LogicalOperator&, const SchemaBase<UnboundFieldBase<1>, true>&);
// template Schema bind<false>(const LogicalOperator&, const SchemaBase<UnboundFieldBase<1>, false>&);

}
