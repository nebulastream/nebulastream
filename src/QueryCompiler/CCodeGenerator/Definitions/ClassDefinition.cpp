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

#include <QueryCompiler/CCodeGenerator/Declarations/ClassDeclaration.hpp>
#include <QueryCompiler/CCodeGenerator/Definitions/ClassDefinition.hpp>
#include <QueryCompiler/CCodeGenerator/Definitions/FunctionDefinition.hpp>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>
namespace NES {

ClassDefinition::ClassDefinition(std::string name) : name(std::move(name)) {}

ClassDefinitionPtr ClassDefinition::create(std::string name) {
    return std::make_shared<ClassDefinition>(std::move(name));
}

void ClassDefinition::addMethod(Visibility visibility,
                                FunctionDefinitionPtr function) {
    if (visibility == Public) {
        publicFunctions.emplace_back(function);
    } else {
        privateFunctions.emplace_back(function);
    }
}

DeclarationPtr ClassDefinition::getDeclaration() {
    std::stringstream classCode;
    classCode << "class " << name;
    classCode << generateBaseClassNames();
    classCode << "{";
    if (!publicFunctions.empty()) {
        classCode << "public:";
        classCode << generateFunctions(publicFunctions);
   }

    if (!privateFunctions.empty()) {
        classCode << "private:";
        classCode << generateFunctions(privateFunctions);
    }

    classCode << "}";

    return ClassDeclaration::create(classCode.str());
}

std::string ClassDefinition::generateFunctions(std::vector<FunctionDefinitionPtr>& functions) {
    std::stringstream classCode;
    for (const auto& function : functions) {
        auto functionDeclaration = function->getDeclaration();
        classCode << functionDeclaration->getCode();
    }
    return classCode.str();
}

std::string ClassDefinition::generateBaseClassNames() {
    std::stringstream classCode;
    if (!baseClasses.empty()) {
        classCode << ":";
        for (int i = 0; i < baseClasses.size() - 1; i++) {
            classCode << " public " << baseClasses[i] << ",";
        }
        classCode << " public " << baseClasses[baseClasses.size() - 1];
    }
    return classCode.str();
}

void ClassDefinition::addBaseClass(std::string baseClassName) {
    baseClasses.emplace_back(baseClassName);
}

}// namespace NES
