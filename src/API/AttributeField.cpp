
#include <API/AttributeField.hpp>
#include <DataTypes/DataType.hpp>
#include <sstream>

namespace NES {

AttributeField::AttributeField(const std::string& _name, DataTypePtr _data_type) : name(_name), dataType(_data_type) {}

AttributeFieldPtr AttributeField::create(std::string _name, DataTypePtr _data_type) {
    return std::make_shared<AttributeField>(_name, _data_type);
}

DataTypePtr AttributeField::getDataType() const { return dataType; }

const std::string AttributeField::toString() const {
    std::stringstream ss;
    ss << name << ":" << dataType->toString();
    return ss.str();
}

bool AttributeField::isEqual(AttributeFieldPtr attr) {
    if (!attr)
        return false;
    return (attr->name == name) && (attr->dataType->isEquals(attr->getDataType()));
}

}// namespace NES
