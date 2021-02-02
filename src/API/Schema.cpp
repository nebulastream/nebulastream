/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <stdexcept>

#include <API/Schema.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Exceptions/InvalidFieldException.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {

Schema::Schema() {}

SchemaPtr Schema::create() { return std::make_shared<Schema>(); }

uint64_t Schema::getSize() const { return fields.size(); }

Schema::Schema(const SchemaPtr query) { copyFields(query); }

SchemaPtr Schema::copy() const { return std::make_shared<Schema>(*this); }

/* Return size of one row of schema in bytes. */
uint64_t Schema::getSchemaSizeInBytes() const {
    uint64_t size = 0;
    // todo if we introduce a physical schema.
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    for (auto const& field : fields) {
        size += physicalDataTypeFactory.getPhysicalType(field->getDataType())->size();
    }
    return size;
}

SchemaPtr Schema::copyFields(SchemaPtr otherSchema) {
    for (AttributeFieldPtr attribute : otherSchema->fields) {
        fields.push_back(attribute->copy());
    }
    return copy();
}

SchemaPtr Schema::addField(AttributeFieldPtr attribute) {
    if (attribute) {
        fields.push_back(attribute->copy());
    }
    return copy();
}

SchemaPtr Schema::addField(const std::string& name, const BasicType& type) {
    return addField(name, DataTypeFactory::createType(type));
}

SchemaPtr Schema::addField(const std::string& name, DataTypePtr data) { return addField(AttributeField::create(name, data)); }

void Schema::removeField(AttributeFieldPtr field) {
    auto it = fields.begin();
    while (it != fields.end()) {
        if (it->get()->name == field->name) {
            fields.erase(it);
            break;
        }
        it++;
    }
}

void Schema::replaceField(const std::string& name, DataTypePtr type) {
    for (auto i = 0; i < fields.size(); i++) {
        if (fields[i]->name == name) {
            fields[i] = AttributeField::create(name, type);
            return;
        }
    }
}

AttributeFieldPtr Schema::get(const std::string& fieldName) {
    for (auto& field : fields) {
        if (field->name == fieldName) {
            return field;
        }
    }
    NES_FATAL_ERROR("Schema: No field in the schema with the identifier " << fieldName);
    throw std::invalid_argument("field " + fieldName + " does not exist");
}

AttributeFieldPtr Schema::get(uint32_t index) {
    if (index < (uint32_t) fields.size()) {
        return fields[index];
    }
    NES_FATAL_ERROR("Schema: No field in the schema with the id " << index);
    throw std::invalid_argument("field id " + std::to_string(index) + " does not exist");
}

bool Schema::equals(SchemaPtr schema, bool considerOrder) {
    if (schema->fields.size() != fields.size()) {
        return false;
    }
    if (considerOrder) {
        for (int i = 0; i < fields.size(); i++) {
            if (!(fields.at(i)->isEqual((schema->fields).at(i)))) {
                return false;
            }
        }
        return true;
    } else {
        for (AttributeFieldPtr fieldAttribute : fields) {
            auto otherFieldAttribute = schema->hasFieldName(fieldAttribute->name);
            if (!(otherFieldAttribute && otherFieldAttribute->isEqual(fieldAttribute))) {
                return false;
            }
        }
        return true;
    }
}

bool Schema::hasEqualTypes(SchemaPtr otherSchema) {
    auto otherFields = otherSchema->fields;
    //Check if the number of filed are same or not
    if (otherFields.size() != fields.size()) {
        return false;
    }

    //Iterate over all fields and check in both schema at same index that they have same attribute type
    for (uint32_t i = 0; i < fields.size(); i++) {
        auto thisField = fields.at(i);
        auto otherField = otherFields.at(i);
        if (!thisField->dataType->isEquals(otherField->dataType)) {
            return false;
        }
    }
    return true;
}

const std::string Schema::toString() const {
    std::stringstream ss;
    for (auto& f : fields) {
        ss << f->toString() << " ";
    }
    return ss.str();
}

AttributeFieldPtr createField(std::string name, BasicType type) {
    return AttributeField::create(name, DataTypeFactory::createType(type));
};

bool Schema::contains(const std::string& fieldName) {
    for (const auto& field : this->fields) {
        if (UtilityFunctions::startsWith(field->name, fieldName)) {
            return true;
        }
    }
    return false;
}

uint64_t Schema::getIndex(const std::string& fieldName) {
    int i = 0;
    bool found = false;
    for (const auto& field : this->fields) {
        if (UtilityFunctions::startsWith(field->name, fieldName)) {
            found = true;
            break;
        }
        i++;
    }
    if (found) {
        return i;
    } else {
        return -1;
    }
}

AttributeFieldPtr Schema::hasFieldName(const std::string& fieldName) {

    //Check if the field name is with fully qualified name
    auto stringToMatch = fieldName;
    if (fieldName.find(ATTRIBUTE_NAME_SEPARATOR) == std::string::npos) {
        //Add only attribute name separator
        //caution: adding the fully qualified name may result in undesired behavior
        //E.g: if schema contains car$speed and truck$speed and user wants to check if attribute speed is present then
        //system should throw invalid field exception
        stringToMatch = ATTRIBUTE_NAME_SEPARATOR + fieldName;
    }

    //Iterate over all fields and look for field which fully qualified name
    std::vector<AttributeFieldPtr> matchedFields;
    for (auto& field : fields) {
        std::string& fullyQualifiedFieldName = field->name;
        if (stringToMatch.length() <= fullyQualifiedFieldName.length()) {
            //Check if the field name ends with the input field name
            auto startingPos = fullyQualifiedFieldName.length() - stringToMatch.length();
            auto found = fullyQualifiedFieldName.compare(startingPos, stringToMatch.length(), stringToMatch);
            if (found == 0) {
                matchedFields.push_back(field);
            }
        }
    }
    //Check how many matching fields were found and raise appropriate exception
    if (matchedFields.size() == 1) {
        return matchedFields[0];
    } else if (matchedFields.size() > 1) {
        NES_ERROR("Schema: Found ambiguous field with name " + fieldName);
        throw InvalidFieldException("Schema: Found ambiguous field with name " + fieldName);
    }
    return nullptr;
}

void Schema::clear() { fields.clear(); }

}// namespace NES
