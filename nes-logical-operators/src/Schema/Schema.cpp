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


//
// template <std::ranges::input_range Range>
// requires(std::same_as<Schema::Field, std::ranges::range_value_t<Range>>)
// Schema::Schema(Private _, const Range& input) noexcept
// template <std::ranges::input_range Range>
//     requires (std::same_as<Schema::Field, std::ranges::range_value_t<Range>> && !std::same_as<Range, std::initializer_list<Schema::Field>>)
// Schema::Schema(const Range& input) noexcept : Schema(Private{}, input)
// {
// }
//
// template <std::ranges::input_range Range>
//     requires (std::same_as<Schema, std::ranges::range_value_t<Range>> && !std::same_as<Range, std::initializer_list<Schema>>)
//     Schema::Schema(const Range& input) noexcept : Schema(Private{}, input | std::views::transform(&Schema::getFields) | std::views::join)
// {
//     // fields =  | ranges::to<std::vector<Field>>();
//     // auto enumerated = std::vector<std::pair<Field, size_t>>{};
//     // enumerated.reserve(std::ranges::size(fields));
//     // for (size_t i = 0; i < std::ranges::size(fields); ++i)
//     // {
//     //     enumerated.push_back({this->fields[i], i});
//     // }
//     // auto [fieldsByName, collisions] = initializeFields(enumerated);
//     // nameToField = fieldsByName | std::views::transform([](auto pair) { return std::pair{IdentifierList{pair.first}, pair.second}; })
//     //     | ranges::to<std::unordered_map<IdentifierList, size_t>>();
//     // currentPrefix = findCommonPrefix(fields);
// }


Schema::Schema(std::vector<Field> fields) noexcept : fields(std::move(fields))
{
    auto [fieldsByName, collisions] = initializeFields(this->fields);
    nameToField = fieldsByName
        | std::views::transform([](auto pair)
                                { return std::make_pair<IdentifierList, size_t>(IdentifierList{pair.first}, std::move(pair.second)); })
        | std::ranges::to<std::unordered_map<IdentifierList, size_t>>();
}

Schema::Schema(std::initializer_list<Field> fields) noexcept : fields(std::move(fields))
{
    auto [fieldsByName, collisions] = initializeFields(this->fields);
    nameToField = fieldsByName
        | std::views::transform([](auto pair)
                                { return std::make_pair<IdentifierList, size_t>(IdentifierList{pair.first}, std::move(pair.second)); })
        | std::ranges::to<std::unordered_map<IdentifierList, size_t>>();
}

// Schema Schema::addField(const IdentifierList& name, const DataType::Type type)
// {
//     DataType dataType{type};
//     sizeOfSchemaInBytes += dataType.getSizeInBytes();
//     fields.emplace_back(Field{std::move(name), std::move(dataType)});
//     auto enumerated = std::vector<std::pair<Field, size_t>>{};
//     enumerated.reserve(std::ranges::size(fields));
//     for (size_t i = 0; i < std::ranges::size(fields); ++i)
//     {
//         enumerated.push_back({this->fields[i], i});
//     }
//     auto [fieldsByName, collisions] = initializeFields(enumerated);
//     nameToField = fieldsByName | std::views::transform([](auto pair) { return std::pair{IdentifierList{pair.first}, pair.second}; })
//         | std::ranges::to<std::unordered_map<IdentifierList, size_t>>();
//     currentPrefix = findCommonPrefix(fields);
//     return *this;
// }


std::optional<Field> Schema::getFieldByName(const IdentifierList& fieldName) const
{
    if (const auto found = nameToField.find(fieldName); found != nameToField.end())
    {
        return fields[found->second];
    }
    return std::nullopt;
}

// Field Schema::getFieldAt(const size_t index) const
// {
//     if (index < fields.size())
//     {
//         return fields[index];
//     }
//     throw FieldNotFound("field with index {}  does not exist", std::to_string(index));
// }

std::ostream& operator<<(std::ostream& os, const Schema& schema)
{
    os << fmt::format("Schema(fields({}))", fmt::join(schema.fields, ","));
    return os;
}

// IdentifierList Schema::getSourceNameQualifier() const
// {
//     return currentPrefix;
// }

// std::string Schema::getQualifierNameForSystemGeneratedFieldsWithSeparator() const
// {
//     if (const auto qualifierName = getQualifierNameForSystemGeneratedFields(); qualifierName.has_value())
//     {
//         return qualifierName.value() + ATTRIBUTE_NAME_SEPARATOR;
//     }
//     return ATTRIBUTE_NAME_SEPARATOR;
// }
const std::vector<Field>& Schema::getFields() const
{
    return fields;
}

// std::optional<IdentifierList> Schema::getQualifierNameForSystemGeneratedFields() const
// {
//     if (std::ranges::empty(currentPrefix))
//     {
//         return std::nullopt;
//     }
//     return currentPrefix;
// }
//
bool Schema::contains(const IdentifierList& qualifiedFieldName) const
{
    return nameToField.contains(qualifiedFieldName);
}

std::vector<IdentifierList> Schema::getUniqueFieldNames() const&
{
    auto namesView = this->fields | std::views::transform([](const Field& field) { return field.getLastName(); });
    return {namesView.begin(), namesView.end()};
}

std::string Schema::createCollisionString(const std::unordered_map<IdentifierList, std::vector<Field>>& collisions)
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
                                        [](const Field& field) -> std::string { return fmt::format("{}", field.getProducedBy().getId()); }),
                                ", "));
                    }),
            ", "));
}

Schema::operator UnboundSchema() const
{
    return UnboundSchema{
        *this | std::views::transform([](const Field& field) { return UnboundField{field.getLastName(), field.getDataType()}; })
        | std::ranges::to<std::vector>()};
}

template Schema::Schema(const std::vector<Field>& input) noexcept;

}
