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

#ifndef NES_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_DECLARATIONS_CONSTRUCTORDECLARATION_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_DECLARATIONS_CONSTRUCTORDECLARATION_HPP_

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/Declaration.hpp>
#include <QueryCompiler/CodeGenerator/CodeGeneratorForwardRef.hpp>

namespace NES {
namespace QueryCompilation {
/**
 * @brief Declaration for a function in the generated code.
 */
class ConstructorDeclaration : public Declaration {
  private:
    Code functionCode;

  public:
    explicit ConstructorDeclaration(Code code);
    static ConstructorDeclarationPtr create(const Code& code);

    [[nodiscard]] GeneratableDataTypePtr getType() const override;
    [[nodiscard]] std::string getIdentifierName() const override;

    [[nodiscard]] Code getTypeDefinitionCode() const override;

    [[nodiscard]] Code getCode() const override;
    [[nodiscard]] DeclarationPtr copy() const override;
};
}// namespace QueryCompilation
}// namespace NES

#endif  // NES_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_DECLARATIONS_CONSTRUCTORDECLARATION_HPP_
