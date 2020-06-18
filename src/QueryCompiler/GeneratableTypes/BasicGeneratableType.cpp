
#include <DataTypes/PhysicalTypes/BasicPhysicalType.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/GeneratableTypes/BasicGeneratableType.hpp>
#include <Util/Logger.hpp>
#include <memory>
#include <sstream>
namespace NES {

BasicGeneratableType::BasicGeneratableType(BasicPhysicalTypePtr type) : GeneratableDataType(), type(type) {}

const CodeExpressionPtr BasicGeneratableType::getTypeDefinitionCode() const {
    // A basic type need no type definition.
    return std::make_shared<CodeExpression>("");
}

const CodeExpressionPtr BasicGeneratableType::getCode() const {

    switch (type->getNativeType()) {
        case BasicPhysicalType::INT_8: return std::make_shared<CodeExpression>("int8_t");
        case BasicPhysicalType::UINT_8: return std::make_shared<CodeExpression>("uint8_t");
        case BasicPhysicalType::INT_16: return std::make_shared<CodeExpression>("int16_t");
        case BasicPhysicalType::UINT_16: return std::make_shared<CodeExpression>("uint16_t");
        case BasicPhysicalType::INT_32: return std::make_shared<CodeExpression>("int32_t");
        case BasicPhysicalType::UINT_32: return std::make_shared<CodeExpression>("uint32_t");
        case BasicPhysicalType::INT_64: return std::make_shared<CodeExpression>("int64_t");
        case BasicPhysicalType::UINT_64: return std::make_shared<CodeExpression>("uint64_t");
        case BasicPhysicalType::FLOAT: return std::make_shared<CodeExpression>("float");
        case BasicPhysicalType::DOUBLE: return std::make_shared<CodeExpression>("double");
        case BasicPhysicalType::BOOLEAN: return std::make_shared<CodeExpression>("bool");
        case BasicPhysicalType::CHAR: return std::make_shared<CodeExpression>("char");
    }
    NES_THROW_RUNTIME_ERROR("Basic generatable type: it was not possible to generate code for this type: ");
}// namespace NES

CodeExpressionPtr BasicGeneratableType::getDeclarationCode(std::string identifier) {
    std::stringstream str;
    str << " " << identifier;
    return combine(getCode(), std::make_shared<CodeExpression>(str.str()));
}

}// namespace NES