
#include <QueryCompiler/GeneratableTypes/UserDefinedDataType.hpp>
namespace NES {

UserDefinedDataType::UserDefinedDataType(const StructDeclaration& decl) : declaration(decl) {}

const CodeExpressionPtr UserDefinedDataType::getTypeDefinitionCode() const {
    return std::make_shared<CodeExpression>(declaration.getCode());
}

const CodeExpressionPtr UserDefinedDataType::getCode() const {
    return std::make_shared<CodeExpression>(declaration.getTypeName());
}

UserDefinedDataType::~UserDefinedDataType() {}

CodeExpressionPtr UserDefinedDataType::getDeclarationCode(std::string) {
    return getCode();
}
}// namespace NES