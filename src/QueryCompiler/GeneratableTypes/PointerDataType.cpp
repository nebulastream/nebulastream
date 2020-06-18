#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/GeneratableTypes/PointerDataType.hpp>

namespace NES {

PointerDataType::PointerDataType(GeneratableDataTypePtr baseType) : GeneratableDataType(), baseType(baseType) {
}

const CodeExpressionPtr PointerDataType::getCode() const {
    return std::make_shared<CodeExpression>(baseType->getCode()->code_ + "*");
}

const CodeExpressionPtr PointerDataType::getTypeDefinitionCode() const {
    return baseType->getTypeDefinitionCode();
}

CodeExpressionPtr PointerDataType::getDeclarationCode(std::string identifier) {
    return std::make_shared<CodeExpression>(baseType->getCode()->code_ + "* " + identifier);
}

}// namespace NES