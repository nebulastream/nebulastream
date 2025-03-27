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

#include <cstddef>
#include <initializer_list>
#include <iostream>
#include <memory>
#include <numeric>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <SerializableDataType.pb.h>


namespace NES
{
Schema::Field::Field(IdentifierList name, DataType dataType) : name(std::move(name)), dataType(std::move(dataType))
{
}

std::ostream& operator<<(std::ostream& os, const Schema::Field& field)
{
    return os << fmt::format("Field(name: {}, DataType: {})", field.name.toString(), field.dataType);
}


using IdSpan = std::span<const Identifier>;
//take const reference to make sure that the spans don't dangle
template <std::ranges::input_range Range>
std::pair<
    std::unordered_map<IdSpan, size_t, std::hash<IdSpan>, IdentifierList::SpanEquals>,
    std::unordered_map<
        IdSpan,
        std::vector<size_t>,
        std::hash<IdSpan>,
        IdentifierList::SpanEquals>> static initializeFields(Range& fields) noexcept
{
    std::unordered_map<IdSpan, size_t, std::hash<IdSpan>, IdentifierList::SpanEquals> fieldsByName{};
    std::unordered_map<IdSpan, std::vector<size_t>, std::hash<IdSpan>, IdentifierList::SpanEquals> collisions{};

    for (std::pair<Schema::Field, size_t>& field : fields)
    {
        IdentifierList& fullName = field.first.name;
        for (size_t i = 0; i < std::ranges::size(fullName); i++)
        {
            IdSpan idSubSpan = std::span{std::ranges::begin(fullName) + i, std::ranges::size(fullName)};
            if (auto existingCollisions = collisions.find(idSubSpan); existingCollisions == collisions.end())
            {
                if (auto existingIdList = fieldsByName.find(idSubSpan); existingIdList != fieldsByName.end())
                {
                    collisions.insert(std::pair<IdSpan, std::vector<size_t>>{idSubSpan, std::vector{field.second}});
                    fieldsByName.erase(existingIdList);
                }
                else
                {
                    fieldsByName.insert(std::pair{idSubSpan, field.second});
                }
            }
            else
            {
                existingCollisions->second.push_back(field.second);
            }
        }
    }
    return {fieldsByName, collisions};
}

static std::optional<IdentifierList> findCommonPrefix(
    size_t numFields, std::unordered_map<IdSpan, std::vector<size_t>, std::hash<IdSpan>, IdentifierList::SpanEquals> collisions)
{
    //maybe we can remove the limitation on the identifier list length
    auto prefixCandidates = collisions
        | std::views::filter([numFields](auto pair)
                             { return std::ranges::size(pair.first) == 1 && std::ranges::size(pair.second) == numFields; })
        | std::views::transform([](auto pair) { return pair.first; }) | ranges::to<std::vector<IdSpan>>();
    if (std::ranges::size(prefixCandidates) == 0)
    {
        NES_DEBUG("Did not find a common prefix for schema");
        return std::nullopt;
    }
    if (std::ranges::size(prefixCandidates) > 1)
    {
        NES_DEBUG("Found more then one common prefix for schema");
        return std::nullopt;
    }
    return IdentifierList{prefixCandidates[0]};
}
Schema::Schema(const MemoryLayoutType memoryLayoutType)

    : memoryLayoutType(memoryLayoutType) { };


Schema::Schema(std::initializer_list<Field> fields) noexcept : fields(fields)
{
    auto enumerated = std::vector<std::pair<Field, size_t>>{};
    enumerated.reserve(std::ranges::size(fields));
    for (size_t i = 0; i < std::ranges::size(fields); ++i)
    {
        enumerated.push_back({this->fields[i], i});
    }
    auto [fieldsByName, collisions] = initializeFields(enumerated);
    nameToField = fieldsByName | std::views::transform([](auto pair) { return std::pair{IdentifierList{pair.first}, pair.second}; })
        | ranges::to<std::unordered_map<IdentifierList, size_t>>();
    currentPrefix = findCommonPrefix(std::ranges::size(fields), collisions);
}

Schema::Schema(std::initializer_list<Schema> schemata) noexcept
{
    fields = schemata | std::views::transform(&Schema::getFields) | std::views::join | ranges::to<std::vector<Field>>();
    auto enumerated = std::vector<std::pair<Field, size_t>>{};
    enumerated.reserve(std::ranges::size(fields));
    for (size_t i = 0; i < std::ranges::size(fields); ++i)
    {
        enumerated.push_back({this->fields[i], i});
    }
    auto [fieldsByName, collisions] = initializeFields(enumerated);
    nameToField = fieldsByName | std::views::transform([](auto pair) { return std::pair{IdentifierList{pair.first}, pair.second}; })
        | ranges::to<std::unordered_map<IdentifierList, size_t>>();
    currentPrefix = findCommonPrefix(std::ranges::size(fields), collisions);
}
Schema Schema::addField(const IdentifierList& name, const DataType& dataType)
{
    return addField(std::move(name), dataType.type);
}
Schema Schema::addField(const IdentifierList& name, const DataType::Type type)
{
    DataType dataType{type};
    sizeOfSchemaInBytes += dataType.getSizeInBytes();
    fields.emplace_back(Field{std::move(name), std::move(dataType)});
    nameToField.emplace(fields.back().name, fields.size() - 1);
    return *this;
}

/// No need to repopulate nameToField, since the key does not change
void Schema::replaceTypeOfField(const IdentifierList& name, DataType type)
{
    if (const auto fieldIdx = nameToField.find(name); fieldIdx != nameToField.end())
    {
        sizeOfSchemaInBytes -= fields.at(fieldIdx->second).dataType.getSizeInBytes();
        sizeOfSchemaInBytes += type.getSizeInBytes();
        fields.at(fieldIdx->second).dataType = std::move(type);
    }
    NES_WARNING("Could not find field with name '{}'", name.toString());
}

std::optional<Schema::Field> Schema::getFieldByName(const IdentifierList& fieldName) const
{
    if (const auto found = nameToField.find(fieldName); found != nameToField.end())
    {
        return fields[found->second];
    }
    return std::nullopt;
}

Schema::Field Schema::getFieldAt(const size_t index) const
{
    if (index < fields.size())
    {
        return fields[index];
    }
    throw FieldNotFound("field with index {}  does not exist", std::to_string(index));
}

std::ostream& operator<<(std::ostream& os, const Schema& schema)
{
    os << fmt::format(
        "Schema(fields({}), size in bytes: {}, layout type: {})",
        fmt::join(schema.fields, ","),
        schema.sizeOfSchemaInBytes,
        magic_enum::enum_name(schema.memoryLayoutType));
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
const std::vector<Schema::Field>& Schema::getFields() const
{
    return fields;
}
size_t Schema::getNumberOfFields() const
{
    return fields.size();
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
IdentifierList Schema::getCommonPrefix() const
{
    return currentPrefix;
}

std::vector<IdentifierList> Schema::getUniqueFieldNames() const&
{
    auto namesView = this->fields | std::views::transform([](const Field& field) { return field.name; });
    return {namesView.begin(), namesView.end()};
}
void Schema::assignToFields(const Schema& otherSchema)
{
    this->fields = otherSchema.fields;
    this->nameToField = otherSchema.nameToField;
    this->sizeOfSchemaInBytes = otherSchema.sizeOfSchemaInBytes;
}
void Schema::appendFieldsFromOtherSchema(const Schema& otherSchema)
{
    this->fields.reserve(this->fields.size() + otherSchema.fields.size());
    this->nameToField.reserve(this->fields.size() + otherSchema.fields.size());
    for (const auto& otherField : otherSchema.fields)
    {
        this->fields.emplace_back(otherField);
        this->nameToField.emplace(otherField.name, this->fields.size() - 1);
    }
    this->sizeOfSchemaInBytes += otherSchema.sizeOfSchemaInBytes;
}

void Schema::renameField(const IdentifierList& oldFieldName, const IdentifierList newFieldName)
{
    if (auto fieldToRename = nameToField.extract(oldFieldName); not fieldToRename.empty())
    {
        fields.at(fieldToRename.mapped()).name = newFieldName;
        fieldToRename.key() = newFieldName;
        nameToField.insert(std::move(fieldToRename));
    }
}
size_t Schema::getSizeOfSchemaInBytes() const
{
    return sizeOfSchemaInBytes;
}

bool Schema::hasFields() const
{
    return not fields.empty();
}

}
