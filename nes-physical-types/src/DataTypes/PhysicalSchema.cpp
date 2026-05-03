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

#include <DataTypes/PhysicalSchema.hpp>

#include <cstddef>
#include <optional>
#include <ostream>
#include <ranges>
#include <string>
#include <utility>
#include <vector>
#include <DataTypes/LogicalType.hpp>
#include <DataTypes/LogicalTypeBridge.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaLowering.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>

namespace NES
{

std::ostream& operator<<(std::ostream& os, const PhysicalSchema::Field& field)
{
    return os << fmt::format("Field(name: {}, LogicalType: {})", field.name, field.logicalType);
}

std::string PhysicalSchema::Field::getUnqualifiedName() const
{
    const auto separatorPosition = name.find(PhysicalSchema::ATTRIBUTE_NAME_SEPARATOR);
    if (separatorPosition == std::string::npos)
    {
        return name;
    }
    return name.substr(separatorPosition + 1);
}

PhysicalSchema::PhysicalSchema(std::vector<Field> fields) : fields(std::move(fields))
{
}

std::ostream& operator<<(std::ostream& os, const PhysicalSchema& schema)
{
    return os << fmt::format("PhysicalSchema(fields({}))", fmt::join(schema.fields, ","));
}

bool PhysicalSchema::hasFields() const
{
    return not fields.empty();
}

std::size_t PhysicalSchema::getNumberOfFields() const
{
    return fields.size();
}

const std::vector<PhysicalSchema::Field>& PhysicalSchema::getFields() const
{
    return fields;
}

std::vector<std::string> PhysicalSchema::getFieldNames() const
{
    auto namesView = fields | std::views::transform([](const Field& field) { return field.name; });
    return {namesView.begin(), namesView.end()};
}

std::optional<PhysicalSchema::Field> PhysicalSchema::getFieldByName(const std::string& fieldName) const
{
    for (const auto& field : fields)
    {
        if (field.name == fieldName)
        {
            return field;
        }
    }

    std::vector<Field> matchingFields;
    for (const auto& field : fields)
    {
        if (auto qualifiedName = field.name; fieldName.length() <= qualifiedName.length())
        {
            const auto separatorPos = qualifiedName.find(ATTRIBUTE_NAME_SEPARATOR);
            if (separatorPos == std::string::npos)
            {
                continue;
            }
            if (qualifiedName.substr(separatorPos + 1) == fieldName)
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

PhysicalSchema::Field PhysicalSchema::getFieldAt(const std::size_t index) const
{
    if (index < fields.size())
    {
        return fields[index];
    }
    throw FieldNotFound("field with index {}  does not exist", std::to_string(index));
}

bool PhysicalSchema::contains(const std::string& qualifiedFieldName) const
{
    for (const auto& field : fields)
    {
        if (field.name == qualifiedFieldName)
        {
            return true;
        }
    }
    return false;
}

std::optional<std::string> PhysicalSchema::getSourceNameQualifier() const
{
    if (fields.empty())
    {
        NES_ERROR("A schema is not allowed to be empty when a qualifier is requested");
        return std::nullopt;
    }
    return fields.front().name.substr(0, fields.front().name.find(ATTRIBUTE_NAME_SEPARATOR));
}

std::string PhysicalSchema::getQualifierNameForSystemGeneratedFieldsWithSeparator() const
{
    if (const auto qualifierName = getSourceNameQualifier(); qualifierName.has_value())
    {
        return qualifierName.value() + ATTRIBUTE_NAME_SEPARATOR;
    }
    throw CannotInferStamp("Could not find qualifier for schema: {}", *this);
}

PhysicalSchema lower(const Schema& logical)
{
    const auto flat = lowerSchema(logical);
    std::vector<PhysicalSchema::Field> fields;
    fields.reserve(flat.getNumberOfFields());
    for (const auto& field : flat.getFields())
    {
        fields.push_back(PhysicalSchema::Field{.name = field.name, .logicalType = field.logicalType});
    }
    return PhysicalSchema{std::move(fields)};
}

std::size_t physicalTupleByteSize(const PhysicalSchema& schema)
{
    std::size_t total = 0;
    for (const auto& field : schema.getFields())
    {
        total += toPhysical(field.logicalType).value().getSizeInBytesWithNull();
    }
    return total;
}

}
