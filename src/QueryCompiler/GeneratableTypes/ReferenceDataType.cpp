#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/GeneratableTypes/ReferenceDataType.hpp>

namespace NES {

ReferenceDataType::ReferenceDataType(GeneratableDataTypePtr baseType) : GeneratableDataType(), baseType(baseType) {}

const CodeExpressionPtr ReferenceDataType::getCode() const {
    return std::make_shared<CodeExpression>(baseType->getCode()->code_ + "&");
}

CodeExpressionPtr ReferenceDataType::getDeclCode(std::string identifier) {
    return std::make_shared<CodeExpression>(baseType->getCode()->code_ + "& " + identifier);
}
const CodeExpressionPtr ReferenceDataType::getTypeDefinitionCode() const {
    return baseType->getTypeDefinitionCode();
}


}// namespace NES