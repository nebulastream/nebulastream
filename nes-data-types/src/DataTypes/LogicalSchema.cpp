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

#include <DataTypes/LogicalSchema.hpp>

#include <cstddef>
#include <optional>
#include <ostream>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/LogicalType.hpp>
#include <DataTypes/Schema.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>

namespace NES
{

LogicalSchema::Field::Field(std::string name, LogicalType logicalType) : name(std::move(name)), logicalType(std::move(logicalType))
{
}

std::ostream& operator<<(std::ostream& os, const LogicalSchema::Field& field)
{
    return os << fmt::format("LogicalField(name: {}, LogicalType: {})", field.name, field.logicalType);
}

std::string LogicalSchema::Field::getUnqualifiedName() const
{
    const auto separatorPosition = name.find(LogicalSchema::ATTRIBUTE_NAME_SEPARATOR);
    if (separatorPosition == std::string::npos)
    {
        return name;
    }
    return name.substr(separatorPosition + 1);
}

LogicalSchema LogicalSchema::addField(std::string name, LogicalType logicalType)
{
    fields.emplace_back(std::move(name), std::move(logicalType));
    nameToField.emplace(fields.back().name, fields.size() - 1);
    return *this;
}

bool LogicalSchema::replaceTypeOfField(const std::string& name, LogicalType logicalType)
{
    if (const auto fieldIdx = nameToField.find(name); fieldIdx != nameToField.end())
    {
        fields.at(fieldIdx->second).logicalType = std::move(logicalType);
        return true;
    }
    NES_WARNING("Could not find field with name '{}'", name);
    return false;
}

std::optional<LogicalSchema::Field> LogicalSchema::getFieldByName(const std::string& fieldName) const
{
    if (const auto field = nameToField.find(fieldName); field != nameToField.end())
    {
        return fields.at(field->second);
    }

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

LogicalSchema::Field LogicalSchema::getFieldAt(const size_t index) const
{
    if (index < fields.size())
    {
        return fields[index];
    }
    throw FieldNotFound("field with index {}  does not exist", std::to_string(index));
}

std::ostream& operator<<(std::ostream& os, const LogicalSchema& schema)
{
    return os << fmt::format("LogicalSchema(fields({}))", fmt::join(schema.fields, ","));
}

std::string LogicalSchema::getQualifierNameForSystemGeneratedFieldsWithSeparator() const
{
    if (const auto qualifierName = getSourceNameQualifier(); qualifierName.has_value())
    {
        return qualifierName.value() + ATTRIBUTE_NAME_SEPARATOR;
    }
    throw CannotInferStamp("Could not find qualifier for logical schema: {}", *this);
}

const std::vector<LogicalSchema::Field>& LogicalSchema::getFields() const
{
    return fields;
}

size_t LogicalSchema::getNumberOfFields() const
{
    return fields.size();
}

std::optional<std::string> LogicalSchema::getSourceNameQualifier() const
{
    if (fields.empty())
    {
        NES_ERROR("A logical schema is not allowed to be empty when a qualifier is requested");
        return std::nullopt;
    }
    return fields.front().name.substr(0, fields.front().name.find(ATTRIBUTE_NAME_SEPARATOR));
}

bool LogicalSchema::contains(const std::string& qualifiedFieldName) const
{
    return nameToField.contains(qualifiedFieldName);
}

std::vector<std::string> LogicalSchema::getFieldNames() const
{
    auto namesView = this->fields | std::views::transform([](const Field& field) { return field.name; });
    return {namesView.begin(), namesView.end()};
}

void LogicalSchema::appendFieldsFromOtherSchema(const LogicalSchema& otherSchema)
{
    this->fields.reserve(this->fields.size() + otherSchema.fields.size());
    this->nameToField.reserve(this->fields.size() + otherSchema.fields.size());
    for (const auto& otherField : otherSchema.fields)
    {
        this->fields.emplace_back(otherField);
        this->nameToField.emplace(otherField.name, this->fields.size() - 1);
    }
}

bool LogicalSchema::renameField(const std::string& oldFieldName, const std::string_view newFieldName)
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

bool LogicalSchema::hasFields() const
{
    return not fields.empty();
}

LogicalSchema LogicalSchema::fromSchema(const Schema& schema)
{
    LogicalSchema lifted;
    for (const auto& field : schema.getFields())
    {
        lifted.addField(field.name, LogicalType::fromPhysical(field.dataType));
    }
    return lifted;
}

Schema LogicalSchema::toPrimitiveSchema() const
{
    Schema primitive;
    for (const auto& field : fields)
    {
        const auto physical = field.logicalType.toPhysical();
        INVARIANT(
            physical.has_value(),
            "LogicalSchema::toPrimitiveSchema requires every field to have a primitive logical type, "
            "but field '{}' has non-primitive logical type {}. Compound types must be expanded by the "
            "lowering phase before this conversion.",
            field.name,
            field.logicalType);
        primitive.addField(field.name, physical.value());
    }
    return primitive;
}

auto LogicalSchema::begin() const -> decltype(std::declval<std::vector<Field>>().cbegin())
{
    return fields.cbegin();
}

auto LogicalSchema::end() const -> decltype(std::declval<std::vector<Field>>().cend())
{
    return fields.cend();
}

LogicalSchema withoutSourceQualifier(const LogicalSchema& input)
{
    LogicalSchema withoutPrefix{};
    auto stripPrefix = [](const std::string& name)
    {
        if (const auto pos = name.find_last_of(LogicalSchema::ATTRIBUTE_NAME_SEPARATOR); pos != std::string::npos)
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
        withoutPrefix.addField(std::move(nameWithoutPrefix), field.logicalType);
    }
    return withoutPrefix;
}

Reflected Reflector<LogicalSchema::Field>::operator()(const LogicalSchema::Field& field) const
{
    return reflect(detail::ReflectedLogicalField{.name = field.name, .logicalType = field.logicalType});
}

LogicalSchema::Field Unreflector<LogicalSchema::Field>::operator()(const Reflected& rfl) const
{
    auto [name, logicalType] = unreflect<detail::ReflectedLogicalField>(rfl);
    return LogicalSchema::Field{std::move(name), std::move(logicalType)};
}

Reflected Reflector<LogicalSchema>::operator()(const LogicalSchema& schema) const
{
    return reflect(detail::ReflectedLogicalSchema{.fields = schema.getFields()});
}

LogicalSchema Unreflector<LogicalSchema>::operator()(const Reflected& rfl) const
{
    auto [fields] = unreflect<detail::ReflectedLogicalSchema>(rfl);
    LogicalSchema schema{};
    for (auto& field : fields)
    {
        schema.addField(std::move(field.name), std::move(field.logicalType));
    }
    return schema;
}

}
