
#include <API/Types/AttributeField.hpp>
#include <DataTypes/DataType.hpp>
#include <sstream>

namespace NES {

AttributeField::AttributeField(const std::string& _name, DataTypePtr _data_type) : name(_name), dataType(_data_type) {
    // assert(data_type!=nullptr);
}

AttributeField::AttributeField(const std::string& _name)
    : name(_name), dataType(nullptr) {
}

AttributeFieldPtr AttributeField::create(std::string _name, DataTypePtr _data_type) {
    return std::make_shared<AttributeField>(_name, _data_type);
}
uint32_t AttributeField::getFieldSize() const {
    //return dataType->getSizeBytes();
    return 0;
}

bool AttributeField::hasType() const {
    return dataType != nullptr;
}

DataTypePtr AttributeField::getDataType() const { return dataType; }

const std::string AttributeField::toString() const {

    std::stringstream ss;
    ss << name << ":" << dataType->toString();
    return ss.str();
}

const AttributeFieldPtr AttributeField::copy() const {
    return std::make_shared<AttributeField>(*this);
}

bool AttributeField::isEqual(AttributeFieldPtr attr) {
    if (!attr)
        return false;
    return (attr->name == name) && (attr->dataType->isEquals(attr->getDataType()));
}

const AttributeFieldPtr createField(const std::string name, DataTypePtr type) {
    return std::make_shared<AttributeField>(name, type);
}
}// namespace NES
