/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/ArrayPhysicalType.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/ConstantExpressionStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/FunctionCallStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeGenerator/CodeExpression.hpp>
#include <QueryCompiler/CompilerTypesFactory.hpp>
#include <QueryCompiler/GeneratableTypes/ArrayGeneratableType.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
#include <memory>
#include <sstream>
namespace NES {
namespace QueryCompilation {
ArrayGeneratableType::ArrayGeneratableType(ArrayPhysicalTypePtr type, GeneratableDataTypePtr component)
    : type(type), component(component) {}

const CodeExpressionPtr ArrayGeneratableType::getTypeDefinitionCode() const { return std::make_shared<CodeExpression>(""); }

const CodeExpressionPtr ArrayGeneratableType::getCode() const {
    std::stringstream str;
    str << "NES::QueryCompilation::Array<" << component->getCode()->code_ << ", " << type->length << "> ";
    return std::make_shared<CodeExpression>(str.str());
}

CodeExpressionPtr ArrayGeneratableType::getDeclarationCode(std::string identifier) const {
    CodeExpressionPtr ptr;
    if (identifier != "") {
        return combine(getCode(), std::make_shared<CodeExpression>(std::move(identifier)));
    } else {
        ptr = component->getCode();
    }
    return ptr;
}
}// namespace QueryCompilation
}// namespace NES
