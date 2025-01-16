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
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
namespace NES
{

Schema::Schema(MemoryLayoutType layoutType) : layoutType(layoutType) {};

SchemaPtr Schema::create(MemoryLayoutType layoutType)
{
    return std::make_shared<Schema>(layoutType);
}

size_t Schema::getFieldCount() const
{
    return fields.size();
}

Schema::Schema(const SchemaPtr& schema, MemoryLayoutType layoutType) : layoutType(layoutType)
{
    copyFields(schema);
}

SchemaPtr Schema::copy() const
{
    return std::make_shared<Schema>(*this);
}

/* Return size of one row of schema in bytes. */
uint64_t Schema::getSchemaSizeInBytes() const
{
    uint64_t size = 0;
    const auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    for (auto const& field : fields)
    {
        auto const type = physicalDataTypeFactory.getPhysicalType(field->getDataType());
        size += type->size();
    }
    return size;
}

SchemaPtr Schema::copyFields(const SchemaPtr& otherSchema)
{
    for (const AttributeFieldPtr& attribute : otherSchema->fields)
    {
        fields.push_back(attribute->deepCopy());
    }
    return copy();
}

SchemaPtr Schema::addField(const AttributeFieldPtr& attribute)
{
    if (attribute)
    {
        fields.push_back(attribute->deepCopy());
    }
    return copy();
}

///TODO #473: investigate if we can remove this method
SchemaPtr Schema::addField(const std::string& name, const BasicType& type)
{
    return addField(name, DataTypeFactory::createType(type));
}

SchemaPtr Schema::addField(const std::string& name, DataTypePtr data)
{
    return addField(AttributeField::create(name, data));
}

void Schema::removeField(const AttributeFieldPtr& field)
{
    std::erase_if(fields, [&](const AttributeFieldPtr& f) { return f->getName() == field->getName(); });
}

void Schema::replaceField(const std::string& name, const DataTypePtr& type)
{
    for (auto& field : fields)
    {
        if (field->getName() == name)
        {
            field = AttributeField::create(name, type);
            return;
        }
    }
}

std::optional<AttributeFieldPtr> Schema::getFieldByName(const std::string& fieldName) const
{
    PRECONDITION(not fields.empty(), "Tried to get a field from a schema that has no fields.");

    ///Iterate over all fields and look for field which fully qualified name
    std::vector<AttributeFieldPtr> matchedFields;
    for (const auto& field : fields)
    {
        if (auto fullyQualifiedFieldName = field->getName(); fieldName.length() <= fullyQualifiedFieldName.length())
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

AttributeFieldPtr Schema::getFieldByIndex(size_t index) const
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
    for (auto const& fieldAttribute : fields)
    {
        auto otherFieldAttribute = other.getFieldByName(fieldAttribute->getName());
        if (!(otherFieldAttribute && otherFieldAttribute.has_value() && fieldAttribute->isEqual(otherFieldAttribute.value())))
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
            ss << f->toString() << suffix;
        }
        else
        {
            ss << f->toString() << sep;
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
    return fields[0]->getName().substr(0, fields[0]->getName().find(ATTRIBUTE_NAME_SEPARATOR));
}

std::string Schema::getQualifierNameForSystemGeneratedFieldsWithSeparator() const
{
    return getQualifierNameForSystemGeneratedFields() + ATTRIBUTE_NAME_SEPARATOR;
}

std::string Schema::getQualifierNameForSystemGeneratedFields() const
{
    if (!fields.empty())
    {
        return fields[0]->getName().substr(0, fields[0]->getName().find(ATTRIBUTE_NAME_SEPARATOR));
    }
    NES_ERROR("Schema::getQualifierNameForSystemGeneratedFields: a schema is not allowed to be empty when a qualifier is "
              "requested");
    return "";
}

bool Schema::contains(const std::string& fieldName) const
{
    for (const auto& field : this->fields)
    {
        NES_TRACE("contain compare field={} with other={}", field->getName(), fieldName);
        if (field->getName() == fieldName)
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

void Schema::setLayoutType(Schema::MemoryLayoutType layoutType)
{
    Schema::layoutType = layoutType;
}

std::vector<std::string> Schema::getFieldNames() const
{
    std::vector<std::string> fieldNames;
    /// Check if the size of the fields vector is within a reasonable range
    INVARIANT(fields.size() < (1U ^ 32), "Schema is corrupted: unreasonable number of fields.");
    for (const auto& attribute : fields)
    {
        fieldNames.emplace_back(attribute->getName());
    }
    return fieldNames;
}

bool Schema::empty() const
{
    return fields.empty();
}

SchemaPtr Schema::updateSourceName(const std::string& srcName) const
{
    for (const auto& field : fields)
    {
        auto currName = Util::splitWithStringDelimiter<std::string>(field->getName(), ATTRIBUTE_NAME_SEPARATOR);
        std::ostringstream newName;
        newName << srcName;
        if (srcName.find(ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
        {
            newName << ATTRIBUTE_NAME_SEPARATOR;
        }
        newName << currName.back();
        field->setName(newName.str());
    }
    return copy();
}

}
