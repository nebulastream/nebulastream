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

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/StructDeclaration.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/VariableDeclaration.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableTypesFactory.hpp>
#include <Util/Logger.hpp>
#include <utility>

namespace NES::QueryCompilation {
StructDeclaration StructDeclaration::create(const std::string& type_name, const std::string& variable_name) {
    return StructDeclaration(type_name, variable_name);
}

GeneratableDataTypePtr StructDeclaration::getType() const {
    return NES::QueryCompilation::GeneratableTypesFactory::createUserDefinedType(*this);
}

std::string StructDeclaration::getIdentifierName() const { return variable_name_; }

Code StructDeclaration::getTypeDefinitionCode() const {
    std::stringstream expr;
    expr << "struct " << type_name_ << "{" << std::endl;
    for (const auto& decl : decls_) {
        expr << decl->getCode() << ";" << std::endl;
    }
    expr << "}";
    return expr.str();
}

Code StructDeclaration::getCode() const {
    std::stringstream expr;
    expr << "struct ";
    if (packed_struct_) {
        expr << "__attribute__((packed)) ";
//        expr << "alignas(16) ";
    }
    expr << type_name_ << "{" << std::endl;
    for (const auto& decl : decls_) {
        expr << decl->getCode() << ";" << std::endl;
    }
    expr << "}";
    expr << variable_name_;
    return expr.str();
}

uint32_t StructDeclaration::getTypeSizeInBytes() {
    NES_ERROR("Called unimplemented function!");
    return 0;
}

std::string StructDeclaration::getTypeName() const { return type_name_; }

DeclarationPtr StructDeclaration::copy() const { return std::make_shared<StructDeclaration>(*this); }

DeclarationPtr StructDeclaration::getField(const std::string& field_name) const {
    for (auto&& decl : decls_) {
        if (decl->getIdentifierName() == field_name) {
            return decl;
        }
    }
    return DeclarationPtr();
}

bool StructDeclaration::containsField(const std::string& field_name, const DataTypePtr&) const {
    for (auto&& decl : decls_) {
        // todo fix equals && decl->getType()->isEqual(dataType)
        if (decl->getIdentifierName() == field_name) {
            return true;
        }
    }
    return false;
}

StructDeclaration& StructDeclaration::addField(const Declaration& decl) {
    DeclarationPtr decl_p = decl.copy();
    if (decl_p) {
        decls_.push_back(decl_p);
    }
    return *this;
}

StructDeclaration& StructDeclaration::makeStructCompact() {
    packed_struct_ = true;
    return *this;
}

StructDeclaration::StructDeclaration(std::string type_name, std::string variable_name)
    : type_name_(std::move(type_name)), variable_name_(std::move(variable_name)), packed_struct_(false) {}

VariableDeclaration StructDeclaration::getVariableDeclaration(const std::string& field_name) const {
    DeclarationPtr decl = getField(field_name);
    if (!decl) {
        NES_ERROR("Error during Code Generation: Field '" << field_name << "' does not exist in struct '" << getTypeName()
                                                          << "'");
        NES_THROW_RUNTIME_ERROR("Error during Code Generation");
    }
    return VariableDeclaration::create(decl->getType(), decl->getIdentifierName());
}

}// namespace NES::QueryCompilation