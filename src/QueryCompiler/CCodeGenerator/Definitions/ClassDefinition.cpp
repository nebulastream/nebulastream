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
#include <QueryCompiler/CCodeGenerator/Definitions/ConstructorDefinition.hpp>
#include <QueryCompiler/CCodeGenerator/Definitions/FunctionDefinition.hpp>

namespace NES {

ClassDefinition::ClassDefinition(std::string name) : name(std::move(name)) {}

ClassDefinitionPtr ClassDefinition::create(std::string name) { return std::make_shared<ClassDefinition>(std::move(name)); }

void ClassDefinition::addMethod(Visibility visibility, FunctionDefinitionPtr function) {
    if (visibility == Public) {
        publicFunctions.emplace_back(function);
    } else {
        privateFunctions.emplace_back(function);
    }
}

void ClassDefinition::addConstructor(ConstructorDefinitionPtr function) { publicConstructors.emplace_back(function); }

DeclarationPtr ClassDefinition::getDeclaration() { return ClassDeclaration::create(shared_from_this()); }

void ClassDefinition::addBaseClass(std::string baseClassName) { baseClasses.emplace_back(baseClassName); }

}// namespace NES
