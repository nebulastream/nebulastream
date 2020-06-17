
#include <DataTypes/DataTypeFactory.hpp>
#include <DataTypes/PhysicalTypes/ArrayPhysicalType.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/DataTypes/ArrayGeneratableType.hpp>
#include <QueryCompiler/DataTypes/GeneratableDataType.hpp>
#include <memory>
#include <sstream>
namespace NES {

ArrayGeneratableType::ArrayGeneratableType(ArrayPhysicalTypePtr type,
                                           GeneratableDataTypePtr component) : type(type), component(component) {}

const CodeExpressionPtr ArrayGeneratableType::getTypeDefinitionCode() const {
    return std::make_shared<CodeExpression>("");
}

CodeExpressionPtr ArrayGeneratableType::generateCode() {}

const CodeExpressionPtr ArrayGeneratableType::getCode() const {
    std::stringstream str;
    str << "[" << type->getLength() << "]";

    return combine(component->getCode(), std::make_shared<CodeExpression>(str.str()));
}

CodeExpressionPtr ArrayGeneratableType::getDeclCode(std::string identifier) {
    CodeExpressionPtr ptr;
    if (identifier != "") {
        ptr = component->getCode();
        ptr = combine(ptr, std::make_shared<CodeExpression>(" " + identifier));
        ptr = combine(ptr, std::make_shared<CodeExpression>("["));
        ptr = combine(ptr, std::make_shared<CodeExpression>(std::to_string(type->getLength())));
        ptr = combine(ptr, std::make_shared<CodeExpression>("]"));
    } else {
        ptr = component->getCode();
    }
    return ptr;
}

}// namespace NES