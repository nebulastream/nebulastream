#include <iostream>
#include <stdexcept>

#include <API/Schema.hpp>
#include <Util/Logger.hpp>
#include <DataTypes/DataTypeFactory.hpp>

namespace NES {

Schema::Schema() {
}

SchemaPtr Schema::create() {
    return std::make_shared<Schema>();
}

size_t Schema::getSize() const {
    return fields.size();
}

Schema::Schema(const SchemaPtr query) {
    copyFields(query);
}

SchemaPtr Schema::copy() const {
    return std::make_shared<Schema>(*this);
}

/* Return size of one row of schema in bytes. */
size_t Schema::getSchemaSizeInBytes() const {
    size_t size = 0;
    for (auto const& field : fields) {
        size += field->getFieldSize();
    }
    return size;
}

SchemaPtr Schema::copyFields(SchemaPtr otherSchema) {
    for (AttributeFieldPtr attr : otherSchema->fields) {
        this->fields.push_back(attr);
    }
    return copy();
}

SchemaPtr Schema::addField(AttributeFieldPtr field) {
    if (field)
        fields.push_back(field);
    return copy();
}

SchemaPtr Schema::addField(const std::string& name, const BasicType& type) {
    return addField(name, DataTypeFactory::createType(type));
}


SchemaPtr Schema::addField(const std::string& name, DataTypePtr data) {
    return addField(AttributeField::create(name, data));
}

void Schema::replaceField(const std::string& name, DataTypePtr type) {
    for (auto i = 0; i < fields.size(); i++) {
        if (fields[i]->name == name) {
            fields[i] = AttributeField::create(name, type);
            return;
        }
    }
}

bool Schema::has(const std::string& fieldName) {
    for (auto& field : fields) {
        if (field->name == fieldName) {
            return true;
        }
    }
    return false;
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
        for (AttributeFieldPtr attr : fields) {
            if (!(schema->has(attr->name) && schema->get(attr->name)->isEqual(attr))) {
                return false;
            }
        }
        return true;
    }
}

const std::string Schema::toString() const {
    std::stringstream ss;
    for (auto& f : fields) {
        ss << f->toString() << " ";
    }
    return ss.str();
}

AttributeFieldPtr createField(std::string name, BasicType type){
    return AttributeField::create(name, DataTypeFactory::createType(type));
};
}// namespace NES
