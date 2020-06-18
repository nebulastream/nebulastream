#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/GeneratableTypes/ReferenceDataType.hpp>

namespace NES {

ReferenceDataType::ReferenceDataType(GeneratableDataTypePtr baseType) : GeneratableDataType(), baseType(baseType) {}


const CodeExpressionPtr ReferenceDataType::getCode() const {
    return std::make_shared<CodeExpression>(baseType->generateCode()->code_ + "&");
}
CodeExpressionPtr ReferenceDataType::generateCode() {
    return NES::CodeExpressionPtr();
}
CodeExpressionPtr ReferenceDataType::getDeclCode(std::string identifier) {
    return std::make_shared<CodeExpression>(baseType->generateCode()->code_ + "& " + identifier);
}
const CodeExpressionPtr ReferenceDataType::getTypeDefinitionCode() const {
    return baseType->getTypeDefinitionCode();
}

//const CodeExpressionPtr ReferenceDataType::getTypeDefinitionCode() const {
    //return baseType->getTypeDefinitionCode();
  //  }

}// namespace NES