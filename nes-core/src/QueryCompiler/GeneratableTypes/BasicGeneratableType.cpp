/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <QueryCompiler/CodeGenerator/CodeExpression.hpp>
#include <QueryCompiler/GeneratableTypes/BasicGeneratableType.hpp>
#include <Util/Logger.hpp>
#include <memory>
#include <sstream>
#include <utility>
namespace NES::QueryCompilation {
BasicGeneratableType::BasicGeneratableType(BasicPhysicalTypePtr type) : type(std::move(type)) {}

CodeExpressionPtr BasicGeneratableType::getTypeDefinitionCode() const {
    // A basic type need no type definition.
    return std::make_shared<CodeExpression>("");
}

CodeExpressionPtr BasicGeneratableType::getCode() const {

    switch (type->nativeType) {
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
    NES_THROW_RUNTIME_ERROR("BasicGeneratableType: it was not possible to generate code for this type: " + type->toString());
    return nullptr;
}

CodeExpressionPtr BasicGeneratableType::getDeclarationCode(std::string identifier) const {
    std::stringstream str;
    str << " " << identifier;
    return combine(getCode(), std::make_shared<CodeExpression>(str.str()));
}
}// namespace NES::QueryCompilation