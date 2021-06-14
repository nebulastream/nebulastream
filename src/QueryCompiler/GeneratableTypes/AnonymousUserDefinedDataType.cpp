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
#include <QueryCompiler/CodeGenerator/GeneratedCode.hpp>
#include <QueryCompiler/GeneratableTypes/AnonymousUserDefinedDataType.hpp>
#include <sstream>
namespace NES::QueryCompilation {
AnonymousUserDefinedDataType::AnonymousUserDefinedDataType(std::string const& name) : name(name) {}

CodeExpressionPtr AnonymousUserDefinedDataType::getTypeDefinitionCode() const { return std::make_shared<CodeExpression>(name); }

CodeExpressionPtr AnonymousUserDefinedDataType::getDeclarationCode(std::string identifier) const {
    std::stringstream str;
    str << " " << identifier;
    return combine(getCode(), std::make_shared<CodeExpression>(str.str()));
}

CodeExpressionPtr AnonymousUserDefinedDataType::getCode() const { return std::make_shared<CodeExpression>(name); }
}// namespace NES::QueryCompilation