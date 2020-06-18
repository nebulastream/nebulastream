
#include <QueryCompiler/GeneratableTypes/AnonymousUserDefinedDataType.hpp>
#include <QueryCompiler/GeneratedCode.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <sstream>
namespace NES {

AnonymousUserDefinedDataType::AnonymousUserDefinedDataType(const std::string name) : name(name) {}


const CodeExpressionPtr AnonymousUserDefinedDataType::getTypeDefinitionCode() const {
    return std::make_shared<CodeExpression>(name);
}

CodeExpressionPtr AnonymousUserDefinedDataType::getDeclCode(std::string identifier) {
    std::stringstream str;
    str << " " << identifier;
    return combine(getCode(), std::make_shared<CodeExpression>(str.str()));
}


AnonymousUserDefinedDataType::~AnonymousUserDefinedDataType() {}
const CodeExpressionPtr AnonymousUserDefinedDataType::getCode() const {
    return  std::make_shared<CodeExpression>(name);
}

}// namespace NES