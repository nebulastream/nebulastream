
#include <assert.h>
#include <QueryCompiler/DataTypes/UserDefinedDataType.hpp>
namespace NES {

UserDefinedDataType::UserDefinedDataType(const StructDeclaration& decl) : declaration(decl) {}



const CodeExpressionPtr UserDefinedDataType::getTypeDefinitionCode() const {
    return std::make_shared<CodeExpression>(declaration.getCode());
}

const CodeExpressionPtr UserDefinedDataType::getCode() const {
    return std::make_shared<CodeExpression>(declaration.getTypeName());
}

UserDefinedDataType::~UserDefinedDataType() {}


CodeExpressionPtr NES::UserDefinedDataType::generateCode() {
    return NES::CodeExpressionPtr();
}
CodeExpressionPtr UserDefinedDataType::getDeclCode(std::string identifier) {
    return getCode();
}
}// namespace NES