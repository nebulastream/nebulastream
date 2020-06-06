
#include <QueryCompiler/DataTypes/AnonymousUserDefinedDataType.hpp>

namespace NES {

AnonymousUserDefinedDataType::AnonymousUserDefinedDataType(const std::string name) : name(name) {}

ValueTypePtr AnonymousUserDefinedDataType::getDefaultInitValue() const {
    return ValueTypePtr();
}

ValueTypePtr AnonymousUserDefinedDataType::getNullValue() const {
    return ValueTypePtr();
}

uint32_t AnonymousUserDefinedDataType::getSizeBytes() const {
    /* assume a 64 bit architecture, each pointer is 8 bytes */
    return -1;
}

const std::string AnonymousUserDefinedDataType::toString() const {
    return std::string("STRUCT ") + name;
}

const std::string AnonymousUserDefinedDataType::convertRawToString(void* data) const {
    return "";
}

const CodeExpressionPtr AnonymousUserDefinedDataType::getTypeDefinitionCode() const {
    return std::make_shared<CodeExpression>(name);
}

const CodeExpressionPtr AnonymousUserDefinedDataType::getCode() const {
    return std::make_shared<CodeExpression>(name);
}

const CodeExpressionPtr AnonymousUserDefinedDataType::getDeclCode(const std::string& identifier) const {
    std::stringstream str;
    str << " " << identifier;
    return combine(getCode(), std::make_shared<CodeExpression>(str.str()));
}

bool AnonymousUserDefinedDataType::isArrayDataType() const {
    return false;
}

bool AnonymousUserDefinedDataType::isCharDataType() const {
    return false;
}

const DataTypePtr AnonymousUserDefinedDataType::copy() const {
    return std::make_shared<AnonymousUserDefinedDataType>(*this);
}

bool AnonymousUserDefinedDataType::isEqual(DataTypePtr ptr) const {
    std::shared_ptr<AnonymousUserDefinedDataType> temp = std::dynamic_pointer_cast<AnonymousUserDefinedDataType>(ptr);
    if (temp)
        return isEqual(temp);
    return false;
}

// \todo: isEqual for structType
const bool AnonymousUserDefinedDataType::isEqual(std::shared_ptr<AnonymousUserDefinedDataType> btr) {
    return true;
}

bool AnonymousUserDefinedDataType::operator==(const DataType& _rhs) const {
    try {
        auto rhs = dynamic_cast<const NES::AnonymousUserDefinedDataType&>(_rhs);
        return name == rhs.name;
    } catch (...) {
        return false;
    }
}

AnonymousUserDefinedDataType::~AnonymousUserDefinedDataType() {}

const DataTypePtr createAnonymUserDefinedType(const std::string name) {
    return std::make_shared<AnonymousUserDefinedDataType>(name);
}

}// namespace NES