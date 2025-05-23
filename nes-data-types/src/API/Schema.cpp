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

#include <algorithm>
#include <iostream>
#include <memory>
#include <optional>
#include <ranges>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Ranges.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/BasicTypes.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>

namespace NES
{

Schema::Schema(const MemoryLayoutType layoutType) : layoutType(layoutType) { };

size_t Schema::getFieldCount() const
{
    return fields.size();
}

Schema::Schema(const Schema& schema, const MemoryLayoutType layoutType) : layoutType(layoutType)
{
    copyFields(schema);
}

uint64_t Schema::getSchemaSizeInBytes() const
{
    uint64_t size = 0;
    const auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    for (const auto& field : fields)
    {
        const auto type = physicalDataTypeFactory.getPhysicalType(field.getDataType());
        size += type->size();
    }
    return size;
}

Schema Schema::copyFields(const Schema& otherSchema)
{
    for (const AttributeField& attribute : otherSchema.fields)
    {
        fields.push_back(attribute);
    }
    return *this;
}

Schema& Schema::addField(const AttributeField& attribute)
{
    fields.push_back(attribute);
    return *this;
}

Schema& Schema::addField(const std::string& name, const BasicType& type)
{
    return addField(name, DataTypeProvider::provideBasicType(type));
}

Schema& Schema::addField(const std::string& name, std::shared_ptr<DataType> type)
{
    return addField(AttributeField(name, std::move(type)));
}

bool Schema::removeField(const AttributeField& field)
{
    return std::erase_if(fields, [&](const AttributeField& fieldInSchema) { return fieldInSchema.getName() == field.getName(); }) > 0;
}

void Schema::replaceField(const std::string& name, const std::shared_ptr<DataType>& type)
{
    for (auto& field : fields)
    {
        if (field.getName() == name)
        {
            field = AttributeField(name, type);
            return;
        }
    }
}

std::optional<AttributeField> Schema::getFieldByName(const std::string& fieldName) const
{
    PRECONDITION(not fields.empty(), "Tried to get a field from a schema that has no fields.");

    /// If the fieldName is fully qualified, we can directly search for the field
    if (fieldName.find(ATTRIBUTE_NAME_SEPARATOR) != std::string::npos)
    {
        if (const auto it = std::ranges::find(fields, fieldName, &AttributeField::getName); it != fields.end())
        {
            return *it;
        }
        NES_WARNING("Schema: field with name {} does not exist", fieldName);
        return std::nullopt;
    }

    /// Fieldname is not qualified, we need to check for ambiguous field names.
    /// Iterates over all fields and checks if the field name matches the input field name without any qualifier.
    /// This means that if either the fieldName or the field name in the schema contains a qualifier, everything before the qualifier is ignored.
    std::vector<AttributeField> potentialMatches;
    for (const auto& field : fields)
    {
        /// Removing potential qualifiers from the field name and the input field name
        const auto fieldWithoutQualifier = field.getName().substr(field.getName().find(ATTRIBUTE_NAME_SEPARATOR) + 1);
        const auto fieldNameWithoutQualifier = fieldName.substr(fieldName.find(ATTRIBUTE_NAME_SEPARATOR) + 1);

        if (fieldWithoutQualifier == fieldNameWithoutQualifier)
        {
            potentialMatches.emplace_back(field);
        }
    }

    /// If there exists a single potential match, return it
    /// If there exists no potential match, return an empty optional
    if (potentialMatches.size() == 1)
    {
        return potentialMatches[0];
    }
    if (potentialMatches.empty())
    {
        return std::nullopt;
    }

    /// For potential matches with ambiguous field names, we must filter some matches out.
    /// We do this by checking if the field name contains a qualifier.
    /// If this is the case, we can filter out all fields that are not 100% equal to the input field name.
    /// Otherwise, we return the first field and log a warning.
    if (fieldName.find(ATTRIBUTE_NAME_SEPARATOR) != std::string::npos)
    {
        potentialMatches = potentialMatches | std::views::filter([&fieldName](const auto& field) { return field.getName() == fieldName; })
            | std::ranges::to<std::vector>();
    }

    /// Check how many matching fields were found and raise appropriate exception
    if (not potentialMatches.empty())
    {
        if (potentialMatches.size() > 1)
        {
            NES_WARNING("Schema: Found ambiguous field with name {}", fieldName);
        }
        return potentialMatches[0];
    }
    NES_WARNING("Schema: field with name {} does not exist", fieldName);
    return std::nullopt;
}

AttributeField Schema::getFieldByIndex(const size_t index) const
{
    if (index < fields.size())
    {
        return fields[index];
    }
    throw FieldNotFound("field with index {}  does not exist", std::to_string(index));
}

bool Schema::operator==(const Schema& other) const
{
    if (other.fields.size() != fields.size())
    {
        return false;
    }
    for (const auto& fieldAttribute : fields)
    {
        auto otherFieldAttribute = other.getFieldByName(fieldAttribute.getName());
        if (!(otherFieldAttribute && otherFieldAttribute.has_value() && fieldAttribute.isEqual(otherFieldAttribute.value())))
        {
            return false;
        }
    }
    return true;
}

bool Schema::hasSameTypes(const Schema& other) const
{
    if (other.fields.size() != fields.size())
    {
        return false;
    }
    for (size_t fieldCount = 0; fieldCount < fields.size(); ++fieldCount)
    {
        if (*fields[0].getDataType() != *other.fields[0].getDataType())
        {
            return false;
        }
    }
    return true;
}

std::string Schema::toString(const std::string& prefix, const std::string& sep, const std::string& suffix) const
{
    std::stringstream stringstream;
    uint64_t i = 1;
    stringstream << prefix;
    for (const auto& field : fields)
    {
        if (i == fields.size())
        {
            stringstream << field.toString() << suffix;
        }
        else
        {
            stringstream << field.toString() << sep;
        }
        i++;
    }
    return stringstream.str();
}

std::string Schema::getSourceNameQualifier() const
{
    if (fields.empty())
    {
        return "Unnamed Source";
    }
    return fields.front().getName().substr(0, fields.front().getName().find(ATTRIBUTE_NAME_SEPARATOR));
}

std::string Schema::getQualifierNameForSystemGeneratedFieldsWithSeparator() const
{
    return getQualifierNameForSystemGeneratedFields() + ATTRIBUTE_NAME_SEPARATOR;
}

std::string Schema::getQualifierNameForSystemGeneratedFields() const
{
    if (!fields.empty())
    {
        return fields.front().getName().substr(0, fields.front().getName().find(ATTRIBUTE_NAME_SEPARATOR));
    }
    NES_ERROR("Schema::getQualifierNameForSystemGeneratedFields: a schema is not allowed to be empty when a qualifier is "
              "requested");
    return "";
}

bool Schema::contains(const std::string& fieldName) const
{
    for (const auto& field : this->fields)
    {
        NES_TRACE("contain compare field={} with other={}", field.getName(), fieldName);
        if (field.getName() == fieldName)
        {
            return true;
        }
    }
    return false;
}

void Schema::clear()
{
    fields.clear();
}

Schema::MemoryLayoutType Schema::getLayoutType() const
{
    return layoutType;
}

void Schema::setLayoutType(const Schema::MemoryLayoutType layoutType)
{
    Schema::layoutType = layoutType;
}

std::vector<std::string> Schema::getFieldNames() const
{
    std::vector<std::string> fieldNames;
    fieldNames.reserve(fields.size());
    for (const auto& attribute : fields)
    {
        fieldNames.emplace_back(attribute.getName());
    }
    return fieldNames;
}

bool Schema::empty() const
{
    return fields.empty();
}

Schema Schema::updateSourceName(const std::string& srcName)
{
    for (auto& field : fields)
    {
        auto currName = Util::splitWithStringDelimiter<std::string>(field.getName(), ATTRIBUTE_NAME_SEPARATOR);
        std::ostringstream newName;
        newName << srcName;
        if (srcName.find(ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
        {
            newName << ATTRIBUTE_NAME_SEPARATOR;
        }
        newName << currName.back();
        field.setName(newName.str());
    }
    return *this;
}

}
