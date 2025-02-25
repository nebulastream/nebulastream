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

#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <vector>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/BasicTypes.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>

namespace NES
{

Schema::Schema(const MemoryLayoutType layoutType) : layoutType(layoutType) {};

size_t Schema::getFieldCount() const
{
    return fields.size();
}

Schema::Schema(const Schema& schema, const MemoryLayoutType layoutType) : layoutType(layoutType)
{
    copyFields(schema);
}

/* Return size of one row of schema in bytes. */
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

Schema Schema::addField(const AttributeField& attribute)
{
    fields.push_back(attribute);
    return *this;
}

Schema Schema::addField(const std::string& name, std::unique_ptr<DataType> data)
{
    return addField(name, DataTypeProvider::provideBasicType(type));
}

void Schema::removeField(const AttributeField& field)
{
    std::erase_if(fields, [&](const AttributeField& f) { return f.getName() == field.getName(); });
}

void Schema::replaceField(const std::string& name, std::shared_ptr<DataType> type)
{
    for (auto& field : fields)
    {
        if (field.getName() == name)
        {
            field = AttributeField(name, std::move(type));
            return;
        }
    }
}

std::optional<AttributeField> Schema::getFieldByName(const std::string& fieldName) const
{
    PRECONDITION(not fields.empty(), "Tried to get a field from a schema that has no fields.");

    ///Iterate over all fields and look for field which fully qualified name
    std::vector<AttributeField> matchedFields;
    for (const auto& field : fields)
    {
        if (auto fullyQualifiedFieldName = field.getName(); fieldName.length() <= fullyQualifiedFieldName.length())
        {
            ///Check if the field name ends with the input field name
            const auto startingPos = fullyQualifiedFieldName.length() - fieldName.length();
            const auto found = fullyQualifiedFieldName.compare(startingPos, fieldName.length(), fieldName);
            if (found == 0)
            {
                matchedFields.push_back(field);
            }
        }
    }
    ///Check how many matching fields were found and raise appropriate exception
    if (not matchedFields.empty())
    {
        if (matchedFields.size() > 1)
        {
            NES_WARNING("Schema: Found ambiguous field with name {}", fieldName);
        }
        return matchedFields[0];
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

std::string Schema::toString(const std::string& prefix, const std::string& sep, const std::string& suffix) const
{
    std::stringstream ss;
    uint64_t i = 1;
    ss << prefix;
    for (const auto& f : fields)
    {
        if (i == fields.size())
        {
            ss << f.toString() << suffix;
        }
        else
        {
            ss << f.toString() << sep;
        }
        i++;
    }
    return ss.str();
}

std::string Schema::getSourceNameQualifier() const
{
    if (fields.empty())
    {
        return "Unnamed Source";
    }
    return fields[0].getName().substr(0, fields[0].getName().find(ATTRIBUTE_NAME_SEPARATOR));
}

std::string Schema::getQualifierNameForSystemGeneratedFieldsWithSeparator() const
{
    return getQualifierNameForSystemGeneratedFields() + ATTRIBUTE_NAME_SEPARATOR;
}

std::string Schema::getQualifierNameForSystemGeneratedFields() const
{
    if (!fields.empty())
    {
        return fields[0].getName().substr(0, fields[0].getName().find(ATTRIBUTE_NAME_SEPARATOR));
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
