
#include <QueryCompiler/DataTypes/UserDefinedDataType.hpp>

namespace NES {

UserDefinedDataType::UserDefinedDataType(const StructDeclaration& decl) : declaration(decl) {}

ValueTypePtr UserDefinedDataType::getDefaultInitValue() const {
    return ValueTypePtr();
}

ValueTypePtr UserDefinedDataType::getNullValue() const {
    return ValueTypePtr();
}

uint32_t UserDefinedDataType::getSizeBytes() const {
    /* assume a 64 bit architecture, each pointer is 8 bytes */
    return declaration.getTypeSizeInBytes();
}
const std::string UserDefinedDataType::toString() const {
    return std::string("STRUCT ") + declaration.getTypeName();
}

const std::string UserDefinedDataType::convertRawToString(void* data) const {
    return "";
}

const CodeExpressionPtr UserDefinedDataType::getTypeDefinitionCode() const {
    return std::make_shared<CodeExpression>(declaration.getCode());
}

const CodeExpressionPtr UserDefinedDataType::getCode() const {
    return std::make_shared<CodeExpression>(declaration.getTypeName());
}

const CodeExpressionPtr UserDefinedDataType::getDeclCode(const std::string& identifier) const {
    return getCode();
}

bool UserDefinedDataType::isArrayDataType() const {
    return false;
}

bool UserDefinedDataType::isCharDataType() const {
    return false;
}

const DataTypePtr UserDefinedDataType::copy() const {
    return std::make_shared<UserDefinedDataType>(*this);
}

bool UserDefinedDataType::isEqual(DataTypePtr ptr) const {
    std::shared_ptr<UserDefinedDataType> temp = std::dynamic_pointer_cast<UserDefinedDataType>(ptr);
    if (temp)
        return isEqual(temp);
    return false;
}

// \todo: isEqual for structType
const bool UserDefinedDataType::isEqual(std::shared_ptr<UserDefinedDataType> btr) {
    return true;
}

bool UserDefinedDataType::operator==(const DataType& _rhs) const {
    assert(0);
}

UserDefinedDataType::~UserDefinedDataType() {}

const DataTypePtr createUserDefinedType(const StructDeclaration& decl) {
    return std::make_shared<UserDefinedDataType>(decl);
}

}// namespace NES