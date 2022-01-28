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

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/ClassDeclaration.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Definitions/ClassDefinition.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Definitions/ConstructorDefinition.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Definitions/FunctionDefinition.hpp>
#include <QueryCompiler/GeneratableTypes/AnonymousUserDefinedDataType.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableTypesFactory.hpp>
#include <QueryCompiler/GeneratableTypes/UserDefinedDataType.hpp>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>

namespace NES::QueryCompilation {
ClassDeclaration::ClassDeclaration(ClassDefinitionPtr classDefinition) : classDefinition(std::move(classDefinition)) {}

ClassDeclarationPtr ClassDeclaration::create(const ClassDefinitionPtr& classDefinition) {
    return std::make_shared<ClassDeclaration>(classDefinition);
}

GeneratableDataTypePtr ClassDeclaration::getType() const {
    return NES::QueryCompilation::GeneratableTypesFactory::createAnonymusDataType(classDefinition->name);
}
std::string ClassDeclaration::getIdentifierName() const { return ""; }

Code ClassDeclaration::getTypeDefinitionCode() const { return Code(); }

Code ClassDeclaration::getCode() const {
    std::stringstream classCode;
    classCode << "class " << classDefinition->name;
    classCode << generateBaseClassNames();
    classCode << "{";

    if (!classDefinition->publicConstructors.empty()) {
        classCode << "public:";
        classCode << generateConstructors(classDefinition->publicConstructors);
    }

    if (!classDefinition->publicFunctions.empty()) {
        classCode << "public:";
        classCode << generateFunctions(classDefinition->publicFunctions);
    }

    if (!classDefinition->privateFunctions.empty()) {
        classCode << "private:";
        classCode << generateFunctions(classDefinition->privateFunctions);
    }

    classCode << "};";
    return classCode.str();
}

std::string ClassDeclaration::generateFunctions(std::vector<FunctionDefinitionPtr>& functions) {
    std::stringstream classCode;
    for (const auto& function : functions) {
        auto functionDeclaration = function->getDeclaration();
        classCode << functionDeclaration->getCode();
    }
    return classCode.str();
}

std::string ClassDeclaration::generateConstructors(std::vector<ConstructorDefinitionPtr>& ctors) {
    std::stringstream classCode;
    for (const auto& ctor : ctors) {
        auto functionDeclaration = ctor->getDeclaration();
        classCode << functionDeclaration->getCode();
    }
    return classCode.str();
}

std::string ClassDeclaration::generateBaseClassNames() const {
    std::stringstream classCode;
    auto baseClasses = classDefinition->baseClasses;
    if (!baseClasses.empty()) {
        classCode << ":";
        for (auto i{0ul}; i < baseClasses.size() - 1; ++i) {
            classCode << " public " << baseClasses[i] << ",";
        }
        classCode << " public " << baseClasses[baseClasses.size() - 1];
    }
    return classCode.str();
}

DeclarationPtr ClassDeclaration::copy() const { return std::make_shared<ClassDeclaration>(*this); }
}// namespace NES::QueryCompilation