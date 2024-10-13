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

#include <Configurations/Coordinator/SchemaType.hpp>

#include <iostream>
#include <stdexcept>
#include <utility>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
namespace NES
{

/// TODO #386 REMOVE THIS FUNCTION AFTER REFACTORING
bool startsWith(const std::string& fullString, std::string_view ending)
{
    return (fullString.rfind(ending, 0) == 0);
}

Schema::Schema(MemoryLayoutType layoutType) : layoutType(layoutType) {};

SchemaPtr Schema::create(MemoryLayoutType layoutType)
{
    return std::make_shared<Schema>(layoutType);
}

uint64_t Schema::getSize() const
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
    /// TODO #386 if we introduce a physical schema.
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
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
        fields.push_back(attribute->copy());
    }
    return copy();
}

SchemaPtr Schema::addField(const AttributeFieldPtr& attribute)
{
    if (attribute)
    {
        fields.push_back(attribute->copy());
    }
    return copy();
}

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
    auto it = fields.begin();
    while (it != fields.end())
    {
        if (it->get()->getName() == field->getName())
        {
            fields.erase(it);
            break;
        }
        it++;
    }
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

AttributeFieldPtr Schema::get(const std::string& fieldName) const
{
    /// This does not work for fields with the same name but different qualifiers
    /// The whole class is a little bit broken. There are several methods that have quite similar names and do similar things.
    /// Additionally, it is not clear when and how to use what method to interact with the schema and the underlying attribute fields.
    PRECONDITION(not fields.empty(), "Tried to get a field from a schema that has no fields.");
    auto fieldNameToSearchFor = fieldName;
    if (fields[0]->getName().find(ATTRIBUTE_NAME_SEPARATOR) != std::string::npos
        && fieldName.find(ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        fieldNameToSearchFor = getQualifierNameForSystemGeneratedFields() + ATTRIBUTE_NAME_SEPARATOR + fieldNameToSearchFor;
    }


    if (auto fieldFound = std::ranges::find(fields, fieldNameToSearchFor, &AttributeField::getName); fieldFound != fields.end())
    {
        return *fieldFound;
    }

    throw FieldNotFound("field {}  does not exist", fieldName);
}

AttributeFieldPtr Schema::get(uint32_t index)
{
    if (index < (uint32_t)fields.size())
    {
        return fields[index];
    }
    NES_FATAL_ERROR("Schema: No field in the schema with the id {}", index);
    throw FieldNotFound("field {}  does not exist", std::to_string(index));
}

bool Schema::equals(const SchemaPtr& schema, bool considerOrder)
{
    if (schema->fields.size() != fields.size())
    {
        return false;
    }
    if (considerOrder)
    {
        for (auto i = static_cast<decltype(fields)::size_type>(0); i < fields.size(); ++i)
        {
            if (!(fields.at(i)->isEqual((schema->fields).at(i))))
            {
                return false;
            }
        }
        return true;
    }
    for (auto const& fieldAttribute : fields)
    {
        auto otherFieldAttribute = schema->getField(fieldAttribute->getName());
        if (!(otherFieldAttribute && otherFieldAttribute->isEqual(fieldAttribute)))
        {
            return false;
        }
    }
    return true;
}

bool Schema::hasEqualTypes(const SchemaPtr& otherSchema)
{
    auto otherFields = otherSchema->fields;
    /// Check if the number of fields is same or not
    if (otherFields.size() != fields.size())
    {
        return false;
    }

    ///Iterate over all fields and check in both schemas the same index that they have is the same attribute type
    for (uint32_t i = 0; i < fields.size(); i++)
    {
        auto thisField = fields.at(i);
        auto otherField = otherFields.at(i);
        if (!thisField->getDataType()->equals(otherField->getDataType()))
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

AttributeFieldPtr createField(const std::string& name, BasicType type)
{
    return AttributeField::create(name, DataTypeFactory::createType(type));
}

AttributeFieldPtr createField(const std::string& name, DataTypePtr type)
{
    return AttributeField::create(name, type);
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

uint64_t Schema::getIndex(const std::string& fieldName) const
{
    int i = 0;
    bool found = false;
    for (const auto& field : this->fields)
    {
        if (startsWith(field->getName(), fieldName))
        {
            found = true;
            break;
        }
        i++;
    }
    if (found)
    {
        return i;
    }
    return -1;
}

AttributeFieldPtr Schema::getField(const std::string& fieldName) const
{
    ///Check if the field name is with fully qualified name
    auto stringToMatch = fieldName;
    if (stringToMatch.find(ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        ///Add only attribute name separator
        ///caution: adding the fully qualified name may result in undesired behavior
        ///E.g: if schema contains car$speed and truck$speed and user wants to check if attribute speed is present then
        ///system should throw invalid field exception
        stringToMatch = ATTRIBUTE_NAME_SEPARATOR + fieldName;
    }

    ///Iterate over all fields and look for field which fully qualified name
    std::vector<AttributeFieldPtr> matchedFields;
    for (auto& field : fields)
    {
        auto fullyQualifiedFieldName = field->getName();
        if (stringToMatch.length() <= fullyQualifiedFieldName.length())
        {
            ///Check if the field name ends with the input field name
            auto startingPos = fullyQualifiedFieldName.length() - stringToMatch.length();
            auto found = fullyQualifiedFieldName.compare(startingPos, stringToMatch.length(), stringToMatch);
            if (found == 0)
            {
                matchedFields.push_back(field);
            }
        }
    }
    ///Check how many matching fields were found and raise appropriate exception
    if (matchedFields.size() == 1)
    {
        return matchedFields[0];
    }
    if (matchedFields.size() > 1)
    {
        ///        throw InvalidFieldException("Schema: Found ambiguous field with name " + fieldName);
        ///TODO #386 workaround we choose the first one to join we will replace this in issue #1543
        return matchedFields[0];
    }
    return nullptr;
}

void Schema::clear()
{
    fields.clear();
}

std::string Schema::getLayoutTypeAsString() const
{
    switch (this->layoutType)
    {
        case Schema::MemoryLayoutType::ROW_LAYOUT:
            return "ROW_LAYOUT";
        case Schema::MemoryLayoutType::COLUMNAR_LAYOUT:
            return "COL_LAYOUT";
    }
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
    ///TODO #386 size can be corrupted if schema gets corrupted (18446741141687656808 fields) #4049
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
