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

#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_DEFINITION_CLASSDEFINITION_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_DEFINITION_CLASSDEFINITION_HPP_

#include <QueryCompiler/CCodeGenerator/CCodeGeneratorForwardRef.hpp>
#include <string>
#include <vector>

namespace NES {

/**
 * @brief Definition for a class in the generated code.
 * class NAME {
 *
 * ..... all declarations added to this namespaces.
 *
 * }
 */
class ClassDefinition : public std::enable_shared_from_this<ClassDefinition> {
  public:
    /**
     * @brief Visibility for class members
     */
    enum Visibility { Public, Private };

    explicit ClassDefinition(std::string name);

    /**
     * @brief Factory to create the class definition.
     * @param name class name
     * @return ClassDefinitionPtr
     */
    static ClassDefinitionPtr create(std::string name);

    /**
     * @brief Adds a base class to the class definition.
     * @param baseClassName
     */
    void addBaseClass(std::string baseClassName);

    /**
     * @brief Adds a method to the class definition.
     * @param visibility Visibility of the method (Public, Private)
     * @param function the function definition.
     */
    void addMethod(Visibility visibility, FunctionDefinitionPtr function);

    /**
     * @brief Adds a ctor to the class definition.
     * @param function the function definition.
     */
    void addConstructor(ConstructorDefinitionPtr function);

    /**
     * @brief creates the declaration of this class.
     * @return DeclarationPtr
     */
    DeclarationPtr getDeclaration();

  private:
    friend class ClassDeclaration;
    std::string name;
    std::vector<std::string> baseClasses;
    std::vector<FunctionDefinitionPtr> publicFunctions;
    std::vector<FunctionDefinitionPtr> privateFunctions;
    std::vector<ConstructorDefinitionPtr> publicConstructors;
};

}// namespace NES
#endif// NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_DEFINITION_CLASSDEFINITION_HPP_
