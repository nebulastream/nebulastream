
#include <API/Types/AttributeField.hpp>
#include <QueryCompiler/CCodeGenerator/Statement.hpp>
#include <QueryCompiler/CodeExpression.hpp>
namespace NES {

AttributeField::AttributeField(const std::string& _name, DataTypePtr _data_type) : name(_name), data_type(_data_type) {
    // assert(data_type!=nullptr);
}

AttributeField::AttributeField(const std::string& _name)
    : name(_name), data_type(nullptr) {
}

AttributeField::AttributeField(const std::string& _name, const BasicType& _type)
    : name(_name), data_type(createDataType(_type)) {
}

AttributeField::AttributeField(const std::string& _name, uint32_t _size)
    : name(_name), data_type(createDataTypeVarChar(_size)) {
}
uint32_t AttributeField::getFieldSize() const { return data_type->getSizeBytes(); }

bool AttributeField::hasType() const {
    return data_type != nullptr;
}

DataTypePtr AttributeField::getDataType() const { return data_type; }

const std::string AttributeField::toString() const {

    std::stringstream ss;
    ss << name << ":" << data_type->toString();
    return ss.str();
}

const AttributeFieldPtr AttributeField::copy() const {
    return std::make_shared<AttributeField>(*this);
}

bool AttributeField::isEqual(const AttributeField& attr) {
    return (attr.name == name) && (attr.data_type->isEqual(attr.getDataType()));
}

bool AttributeField::isEqual(AttributeFieldPtr attr) {
    if (!attr)
        return false;
    return (attr->name == name) && (attr->data_type->isEqual(attr->getDataType()));
}

AttributeField::AttributeField(const AttributeField& other)
    : name(other.name), data_type(other.data_type) {
}

AttributeField& AttributeField::operator=(const AttributeField& other) {
    if (this != &other) {
        this->name = other.name;
        this->data_type = other.data_type->copy();
    }
    return *this;
}

const AttributeFieldPtr createField(const std::string name, const BasicType& type) {
    AttributeFieldPtr ptr = std::make_shared<AttributeField>(name, type);
    return ptr;
}

const AttributeFieldPtr createField(const std::string name, uint32_t size) {
    AttributeFieldPtr ptr = std::make_shared<AttributeField>(name, size);
    return ptr;
}

const AttributeFieldPtr createField(const std::string name, DataTypePtr type) {
    AttributeFieldPtr ptr = std::make_shared<AttributeField>(name, type);
    return ptr;
}
}// namespace NES
