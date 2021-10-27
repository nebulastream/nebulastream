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

#ifndef NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_C_CODE_GENERATOR_DECLARATIONS_CLASS_DECLARATION_HPP_
#define NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_C_CODE_GENERATOR_DECLARATIONS_CLASS_DECLARATION_HPP_

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/Declaration.hpp>
#include <QueryCompiler/CodeGenerator/CodeGeneratorForwardRef.hpp>
#include <vector>
namespace NES::QueryCompilation {
/**
 * @brief Declaration for a class in the generated code.
 */
class ClassDeclaration : public Declaration {

  public:
    explicit ClassDeclaration(ClassDefinitionPtr classDefinition);
    static ClassDeclarationPtr create(const ClassDefinitionPtr& classDefinition);

    [[nodiscard]] GeneratableDataTypePtr getType() const override;
    [[nodiscard]] std::string getIdentifierName() const override;

    [[nodiscard]] Code getTypeDefinitionCode() const override;

    [[nodiscard]] Code getCode() const override;
    [[nodiscard]] DeclarationPtr copy() const override;

  private:
    static std::string generateFunctions(std::vector<FunctionDefinitionPtr>& functions);
    static std::string generateConstructors(std::vector<ConstructorDefinitionPtr>& ctors);
    [[nodiscard]] std::string generateBaseClassNames() const;
    ClassDefinitionPtr classDefinition;
};
}// namespace NES::QueryCompilation

#endif  // NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_C_CODE_GENERATOR_DECLARATIONS_CLASS_DECLARATION_HPP_
