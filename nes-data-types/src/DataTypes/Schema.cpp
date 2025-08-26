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

#include <algorithm>
#include <cstddef>
#include <functional>
#include <iostream>
#include <iterator>
#include <optional>
#include <ranges>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
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

Schema withoutSourceQualifier(const Schema& input)
{
    Schema withoutPrefix{};
    withoutPrefix.memoryLayoutType = input.memoryLayoutType;
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

SchemaDiff SchemaDiff::of(const Schema& leftSchema, const Schema& rightSchema)
{
    const std::multiset expectedResultFields(leftSchema.begin(), leftSchema.end());
    const std::multiset actualResultFields(rightSchema.begin(), rightSchema.end());

    SchemaDiff diff;

    /// Difference by name
    std::ranges::set_difference(
        expectedResultFields,
        actualResultFields,
        std::back_inserter(diff.leftFields),
        std::less{},
        &Schema::Field::name,
        &Schema::Field::name);

    std::ranges::set_difference(
        actualResultFields,
        expectedResultFields,
        std::back_inserter(diff.rightFields),
        std::less{},
        &Schema::Field::name,
        &Schema::Field::name);

    /// Find Intersection by Field name. This has to be done twice to get both fields from the left and the right schema.
    std::multiset<Schema::Field> nameIntersectionLeft;
    std::ranges::set_intersection(
        expectedResultFields,
        actualResultFields,
        std::inserter(nameIntersectionLeft, nameIntersectionLeft.end()),
        std::less{},
        &Schema::Field::name,
        &Schema::Field::name);

    std::multiset<Schema::Field> nameIntersectionRight;
    std::ranges::set_intersection(
        actualResultFields,
        expectedResultFields,
        std::inserter(nameIntersectionRight, nameIntersectionRight.end()),
        std::less{},
        &Schema::Field::name,
        &Schema::Field::name);

    /// Find difference comparing all properties of a schema field, not just the name.
    std::vector<Schema::Field> modifiedFieldsLeft;
    std::vector<Schema::Field> modifiedFieldsRight;
    std::ranges::set_difference(nameIntersectionLeft, nameIntersectionRight, std::back_inserter(modifiedFieldsLeft));
    std::ranges::set_difference(nameIntersectionRight, nameIntersectionLeft, std::back_inserter(modifiedFieldsRight));
    INVARIANT(modifiedFieldsLeft.size() == modifiedFieldsRight.size(), "Both set differences should return the same number of items");

    diff.fieldsWithMissmatch.reserve(modifiedFieldsLeft.size());
    for (size_t i = 0; i < modifiedFieldsLeft.size(); ++i)
    {
        diff.fieldsWithMissmatch.emplace_back(modifiedFieldsLeft[i], modifiedFieldsRight[i]);
    }

    return diff;
}

bool SchemaDiff::isDifferent() const
{
    return !leftFields.empty() || !rightFields.empty() || !fieldsWithMissmatch.empty();
}

std::ostream& operator<<(std::ostream& os, const SchemaDiff& diff)
{
    os << "SchemaDiff {\n";

    if (!diff.leftFields.empty())
    {
        os << "  Additional fields on the left (" << diff.leftFields.size() << "):\n";
        for (const auto& field : diff.leftFields)
        {
            os << "    + " << field << "\n";
        }
    }

    if (!diff.rightFields.empty())
    {
        os << "  Additional fields on the right (" << diff.rightFields.size() << "):\n";
        for (const auto& field : diff.rightFields)
        {
            os << "    - " << field << "\n";
        }
    }

    if (!diff.fieldsWithMissmatch.empty())
    {
        os << "  Fields with type missmatch (" << diff.fieldsWithMissmatch.size() << "):\n";
        for (const auto& [expected, actual] : diff.fieldsWithMissmatch)
        {
            os << "    ~ " << expected.name << ": " << expected.dataType << " -> " << actual.dataType << "\n";
        }
    }

    if (diff.leftFields.empty() && diff.rightFields.empty() && diff.fieldsWithMissmatch.empty())
    {
        os << "  No differences found\n";
    }

    os << "}";
    return os;
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

}
