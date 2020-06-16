#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/DataTypes/ReferenceDataType.hpp>

namespace NES {

ReferenceDataType::ReferenceDataType(GeneratableDataTypePtr baseType) : GeneratableDataType(), baseType(baseType) {}

const CodeExpressionPtr ReferenceDataType::getDeclCode(const std::string& identifier) const {
    return std::make_shared<CodeExpression>(baseType->generateCode()->code_ + "& " + identifier);
}

const CodeExpressionPtr ReferenceDataType::getCode() const {
    return std::make_shared<CodeExpression>(baseType->generateCode()->code_ + "&");
}
CodeExpressionPtr ReferenceDataType::generateCode() {
    return NES::CodeExpressionPtr();
}

//const CodeExpressionPtr ReferenceDataType::getTypeDefinitionCode() const {
    //return baseType->getTypeDefinitionCode();
  //  }

}// namespace NES