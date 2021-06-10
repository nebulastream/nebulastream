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

#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_DECLARATIONS_NAMESPACEDECLARATION_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_DECLARATIONS_NAMESPACEDECLARATION_HPP_

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/Declaration.hpp>
#include <QueryCompiler/CodeGenerator/CodeGeneratorForwardRef.hpp>

namespace NES {
namespace QueryCompilation {
/**
 * @brief Declaration for a namespace in the generated code.
 */
class NamespaceDeclaration : public Declaration {
  public:
    NamespaceDeclaration(Code code);
    static NamespaceDeclarationPtr create(Code code);
    [[nodiscard]] const GeneratableDataTypePtr getType() const override;
    [[nodiscard]] const std::string getIdentifierName() const override;
    [[nodiscard]] const Code getTypeDefinitionCode() const override;
    [[nodiscard]] const Code getCode() const override;
    [[nodiscard]] const DeclarationPtr copy() const override;

  private:
    Code namespaceCode;
};
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_DECLARATIONS_NAMESPACEDECLARATION_HPP_
