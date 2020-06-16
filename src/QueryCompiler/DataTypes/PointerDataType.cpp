#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/DataTypes/PointerDataType.hpp>

namespace NES {

PointerDataType::PointerDataType(GeneratableDataTypePtr baseType): GeneratableDataType(), baseType(baseType) {
}

const CodeExpressionPtr PointerDataType::getDeclCode(const std::string& identifier) const {
    return std::make_shared<CodeExpression>(baseType->generateCode()->code_ + "* " + identifier);
}

const CodeExpressionPtr PointerDataType::getCode() const {
    return std::make_shared<CodeExpression>(baseType->generateCode()->code_ + "*");
}

const CodeExpressionPtr PointerDataType::getTypeDefinitionCode() const {
    //return baseType->getTypeDefinitionCode();
    //
    }
CodeExpressionPtr PointerDataType::generateCode() {
    return std::make_shared<CodeExpression>(baseType->generateCode()->code_ + "*");
}

}// namespace NES