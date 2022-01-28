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

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/VariableDeclaration.hpp>
#include <QueryCompiler/CodeGenerator/CodeExpression.hpp>
#include <QueryCompiler/CodeGenerator/GeneratedCode.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableTypesFactory.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableValueType.hpp>
#include <Util/Logger.hpp>
#include <memory>
#include <utility>

namespace NES::QueryCompilation {
GeneratableDataTypePtr VariableDeclaration::getType() const { return type_; }
std::string VariableDeclaration::getIdentifierName() const { return identifier_; }

Code VariableDeclaration::getTypeDefinitionCode() const {
    CodeExpressionPtr code = type_->getTypeDefinitionCode();
    if (code) {
        return code->code_;
    }
    return Code();
}

Code VariableDeclaration::getCode() const {
    std::stringstream str;
    str << type_->getDeclarationCode(identifier_)->code_;
    if (init_value_) {
        auto valueType = NES::QueryCompilation::GeneratableTypesFactory::createValueType(init_value_);
        str << " = " << valueType->getCodeExpression()->code_;
    }
    return str.str();
}

CodeExpressionPtr VariableDeclaration::getIdentifier() const { return std::make_shared<CodeExpression>(identifier_); }

GeneratableDataTypePtr VariableDeclaration::getDataType() const { return type_; }

DeclarationPtr VariableDeclaration::copy() const { return std::make_shared<VariableDeclaration>(*this); }

VariableDeclaration::VariableDeclaration(GeneratableDataTypePtr type, std::string identifier, ValueTypePtr value)
    : type_(std::move(type)), identifier_(std::move(identifier)), init_value_(std::move(value)) {}

VariableDeclaration::VariableDeclaration(const VariableDeclaration& var_decl)
    : type_(var_decl.type_), identifier_(var_decl.identifier_), init_value_(var_decl.init_value_) {}

VariableDeclaration
VariableDeclaration::create(const GeneratableDataTypePtr& type, const std::string& identifier, ValueTypePtr value) {
    if (!type) {
        NES_ERROR("DataTypePtr type is nullptr!");
    }
    return VariableDeclaration(type, identifier, std::move(value));
}
VariableDeclaration VariableDeclaration::create(DataTypePtr type, const std::string& identifier, ValueTypePtr value) {
    auto typeFactory = GeneratableTypesFactory();
    return VariableDeclaration(typeFactory.createDataType(std::move(type)), identifier, std::move(value));
}
}// namespace NES::QueryCompilation
