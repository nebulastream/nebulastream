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

#include <QueryCompiler/CodeGenerator/CodeExpression.hpp>
#include <QueryCompiler/GeneratableTypes/UserDefinedDataType.hpp>
namespace NES::QueryCompilation {
UserDefinedDataType::UserDefinedDataType(const StructDeclaration& decl) : declaration(decl) {}

CodeExpressionPtr UserDefinedDataType::getTypeDefinitionCode() const {
    return std::make_shared<CodeExpression>(declaration.getCode());
}

CodeExpressionPtr UserDefinedDataType::getCode() const { return std::make_shared<CodeExpression>(declaration.getTypeName()); }

CodeExpressionPtr UserDefinedDataType::getDeclarationCode(std::string) const {
    std::stringstream str;
    str << " " << identifier;
    return combine(getCode(), std::make_shared<CodeExpression>(str.str()));
}
}// namespace NES::QueryCompilation