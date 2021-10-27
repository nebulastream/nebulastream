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

#ifndef NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_C_CODE_GENERATOR_DECLARATIONS_VARIABLE_DECLARATION_HPP_
#define NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_C_CODE_GENERATOR_DECLARATIONS_VARIABLE_DECLARATION_HPP_

#include <Common/ValueTypes/ValueType.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/Declaration.hpp>
#include <QueryCompiler/CodeGenerator/CodeGeneratorForwardRef.hpp>

namespace NES::QueryCompilation {

class VariableDeclaration : public Declaration {
  public:
    VariableDeclaration(VariableDeclaration const& var_decl);

    ~VariableDeclaration() override = default;

    static VariableDeclaration create(DataTypePtr type, const std::string& identifier, ValueTypePtr value = nullptr);
    static VariableDeclaration
    create(const GeneratableDataTypePtr& type, const std::string& identifier, ValueTypePtr value = nullptr);

    [[nodiscard]] GeneratableDataTypePtr getType() const override;
    [[nodiscard]] std::string getIdentifierName() const override;

    [[nodiscard]] Code getTypeDefinitionCode() const override;

    [[nodiscard]] Code getCode() const override;

    [[nodiscard]] CodeExpressionPtr getIdentifier() const;

    [[nodiscard]] GeneratableDataTypePtr getDataType() const;

    [[nodiscard]] DeclarationPtr copy() const override;

  private:
    VariableDeclaration(GeneratableDataTypePtr type, std::string identifier, ValueTypePtr value = nullptr);
    GeneratableDataTypePtr type_;
    std::string identifier_;
    ValueTypePtr init_value_;
};

}// namespace NES::QueryCompilation

#endif  // NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_C_CODE_GENERATOR_DECLARATIONS_VARIABLE_DECLARATION_HPP_
