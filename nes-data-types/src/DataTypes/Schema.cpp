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
#include <iostream>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

Schema::Field::Field(std::string name, DataType dataType) : name(std::move(name)), dataType(std::move(dataType))
{
}

std::ostream& operator<<(std::ostream& os, const Schema::Field& field)
{
    return os << fmt::format("Field(name: {}, DataType: {})", field.name, field.dataType);
}

Schema::Schema(const MemoryLayoutType memoryLayoutType) : memoryLayoutType(memoryLayoutType) { };

Schema Schema::addField(std::string name, const DataType& dataType)
{
    return addField(std::move(name), dataType.type);
}
Schema Schema::addField(std::string name, const DataType::Type type)
{
    DataType dataType{type};
    sizeOfSchemaInBytes += dataType.getSizeInBytes();
    fields.emplace_back(std::move(name), std::move(dataType));
    nameToField.emplace(fields.back().name, fields.size() - 1);
    return *this;
}

/// No need to repopulate nameToField, since the key does not change
bool Schema::replaceTypeOfField(const std::string& name, DataType type)
{
    if (const auto fieldIdx = nameToField.find(name); fieldIdx != nameToField.end())
    {
        sizeOfSchemaInBytes -= fields.at(fieldIdx->second).dataType.getSizeInBytes();
        sizeOfSchemaInBytes += type.getSizeInBytes();
        fields.at(fieldIdx->second).dataType = std::move(type);
        return true;
    }
    NES_WARNING("Could not find field with name '{}'", name);
    return false;
}

std::optional<Schema::Field> Schema::getFieldByName(const std::string& fieldName) const
{
    /// Check if nameToField contains fully qualified name
    if (const auto field = nameToField.find(fieldName); field != nameToField.end())
    {
        return fields.at(field->second);
    }

    ///Iterate over all fields and look for field which fully qualified name
    std::vector<Field> matchedFields;
    for (const auto& field : fields)
    {
        if (auto fullyQualifiedFieldName = field.name; fieldName.length() <= fullyQualifiedFieldName.length())
        {
            ///Check if the field name ends with the input field name
            const auto startingPos = fullyQualifiedFieldName.length() - fieldName.length();
            const auto fieldWithoutQualifier = fullyQualifiedFieldName.substr(startingPos, fieldName.length());
            if (fieldWithoutQualifier == fieldName)
            {
                matchedFields.emplace_back(field);
            }
        }
    }
    ///Check how many matching fields were found log an ERROR
    if (not matchedFields.empty())
    {
        return matchedFields.front();
    }
    NES_WARNING("Schema: field with name {} does not exist", fieldName);
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

std::string Schema::getQualifierNameForSystemGeneratedFieldsWithSeparator() const
{
    if (const auto qualifierName = getSourceNameQualifier(); qualifierName.has_value())
    {
        return qualifierName.value() + ATTRIBUTE_NAME_SEPARATOR;
    }
    /// TODO #764: make sure that there is always a qualifier
    throw CannotInferStamp("Could not find qualifier for schema: {}", *this);
}

const std::vector<Schema::Field>& Schema::getFields() const
{
    return fields;
}

size_t Schema::getNumberOfFields() const
{
    return fields.size();
}

std::optional<std::string> Schema::getSourceNameQualifier() const
{
    if (fields.empty())
    {
        NES_ERROR("A schema is not allowed to be empty when a qualifier is requested");
        return std::nullopt;
    }
    return fields.front().name.substr(0, fields.front().name.find(ATTRIBUTE_NAME_SEPARATOR));
}

bool Schema::contains(const std::string& qualifiedFieldName) const
{
    return nameToField.contains(qualifiedFieldName);
}

std::vector<std::string> Schema::getFieldNames() const
{
    auto namesView = this->fields | std::views::transform([](const Field& field) { return field.name; });
    return {namesView.begin(), namesView.end()};
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

bool Schema::renameField(const std::string& oldFieldName, const std::string_view newFieldName)
{
    if (auto fieldToRename = nameToField.extract(oldFieldName))
    {
        fields.at(fieldToRename.mapped()).name = newFieldName;
        fieldToRename.key() = newFieldName;
        nameToField.insert(std::move(fieldToRename));
        return true;
    }
    return false;
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
