
#include <QueryCompiler/DataTypes/AnonymousUserDefinedDataType.hpp>
#include <QueryCompiler/GeneratedCode.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <sstream>
namespace NES {

AnonymousUserDefinedDataType::AnonymousUserDefinedDataType(const std::string name) : name(name) {}


const CodeExpressionPtr AnonymousUserDefinedDataType::getTypeDefinitionCode() const {
    return std::make_shared<CodeExpression>(name);
}

CodeExpressionPtr AnonymousUserDefinedDataType::generateCode()  {
    return std::make_shared<CodeExpression>(name);
}

const CodeExpressionPtr AnonymousUserDefinedDataType::getDeclCode(const std::string& identifier) {
    std::stringstream str;
    str << " " << identifier;
    return combine(generateCode(), std::make_shared<CodeExpression>(str.str()));
}


AnonymousUserDefinedDataType::~AnonymousUserDefinedDataType() {}

}// namespace NES