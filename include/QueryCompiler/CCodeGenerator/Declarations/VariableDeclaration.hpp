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

#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_DECLARATIONS_VARIABLEDECLARATION_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_DECLARATIONS_VARIABLEDECLARATION_HPP_

#include <Common/ValueTypes/ValueType.hpp>
#include <QueryCompiler/CCodeGenerator/Declarations/Declaration.hpp>

namespace NES {

class VariableDeclaration;
typedef std::shared_ptr<VariableDeclaration> VariableDeclarationPtr;

class CodeExpression;
typedef std::shared_ptr<CodeExpression> CodeExpressionPtr;

class ValueType;
typedef std::shared_ptr<ValueType> ValueTypePtr;

class GeneratableDataType;
typedef std::shared_ptr<GeneratableDataType> GeneratableDataTypePtr;

class VariableDeclaration : public Declaration {
  public:
    VariableDeclaration(const VariableDeclaration& var_decl);
    static VariableDeclaration create(DataTypePtr type, const std::string& identifier, ValueTypePtr value = nullptr);
    static VariableDeclaration create(GeneratableDataTypePtr type, const std::string& identifier, ValueTypePtr value = nullptr);

    virtual const GeneratableDataTypePtr getType() const override;
    virtual const std::string getIdentifierName() const override;

    const Code getTypeDefinitionCode() const override;

    const Code getCode() const override;

    const CodeExpressionPtr getIdentifier() const;

    const GeneratableDataTypePtr getDataType() const;

    const DeclarationPtr copy() const override;

    virtual ~VariableDeclaration() override;

  private:
    VariableDeclaration(GeneratableDataTypePtr type, const std::string& identifier, ValueTypePtr value = nullptr);
    GeneratableDataTypePtr type_;
    std::string identifier_;
    ValueTypePtr init_value_;
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_DECLARATIONS_VARIABLEDECLARATION_HPP_
