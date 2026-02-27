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

#include <DataTypes/Schema.hpp>

#include <cstddef>
#include <iostream>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Reflection.hpp>
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

std::string Schema::Field::getUnqualifiedName() const
{
    const auto separatorPosition = name.find(Schema::ATTRIBUTE_NAME_SEPARATOR);
    if (separatorPosition == std::string::npos)
    {
        return name;
    }
    return name.substr(separatorPosition + 1);
}

Schema Schema::addField(std::string name, const DataType& dataType)
{
    return addField(std::move(name), dataType.type);
}

Schema Schema::addField(std::string name, const DataType::Type type)
{
    DataType dataType{type};
    sizeOfSchemaInBytes += dataType.getSizeInBytes();
    fields.emplace_back(std::move(name), std::move(dataType));
    nameToField.insert_or_assign(fields.back().name, fields.size() - 1);
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

    ///Iterate over all fields and look for fields with fully qualified name
    std::vector<Field> matchingFields;

    for (const auto& field : fields)
    {
        if (auto fullyQualifiedFieldName = field.name; fieldName.length() <= fullyQualifiedFieldName.length())
        {
            const auto separatorPos = fullyQualifiedFieldName.find(ATTRIBUTE_NAME_SEPARATOR);
            if (separatorPos == std::string::npos)
            {
                continue;
            }

            if (const auto fieldWithoutQualifier = fullyQualifiedFieldName.substr(separatorPos + 1); fieldWithoutQualifier == fieldName)
            {
                matchingFields.emplace_back(field);
            }
        }
    }

    if (matchingFields.empty())
    {
        return std::nullopt;
    }
    if (matchingFields.size() > 1)
    {
        NES_WARNING("Ambiguous field name {}. Returning first found field {}", fieldName, matchingFields.front());
    }
    return matchingFields.front();
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
    os << fmt::format("Schema(fields({}), size in bytes: {})", fmt::join(schema.fields, ","), schema.sizeOfSchemaInBytes);
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
        this->nameToField.insert_or_assign(otherField.name, this->fields.size() - 1);
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

Schema withoutSourceQualifier(const Schema& input)
{
    Schema withoutPrefix{};
    auto stripPrefix = [](const std::string& name)
    {
        if (const auto pos = name.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR); pos != std::string::npos)
        {
            return name.substr(pos + 1);
        }
        return name;
    };

    for (const auto& field : input.getFields())
    {
        auto nameWithoutPrefix = stripPrefix(field.name);
        if (withoutPrefix.contains(nameWithoutPrefix))
        {
            throw FieldAlreadyExists("Removing source prefixes would cause duplicated fields. Field `{}` is not unique.", field.name);
        }
        withoutPrefix.addField(std::move(nameWithoutPrefix), field.dataType);
    }


    return withoutPrefix;
}

bool Schema::hasFields() const
{
    return not fields.empty();
}

auto Schema::begin() const -> decltype(std::declval<std::vector<Field>>().cbegin())
{
    return fields.cbegin();
}

auto Schema::end() const -> decltype(std::declval<std::vector<Field>>().cend())
{
    return fields.cend();
}

Reflected Reflector<Schema::Field>::operator()(const Schema::Field& field) const
{
    return reflect(detail::ReflectedField{.name = field.name, .type = field.dataType});
}

Schema::Field Unreflector<Schema::Field>::operator()(const Reflected& rfl) const
{
    auto [name, type] = unreflect<detail::ReflectedField>(rfl);
    return Schema::Field{std::move(name), type};
}

Reflected Reflector<Schema>::operator()(const Schema& schema) const
{
    return reflect(detail::ReflectedSchema{.fields = schema.getFields()});
}

Schema Unreflector<Schema>::operator()(const Reflected& rfl) const
{
    auto [fields] = unreflect<detail::ReflectedSchema>(rfl);
    Schema schema{};
    for (const auto& field : fields)
    {
        schema.addField(field.name, field.dataType);
    }
    return schema;
}

}
