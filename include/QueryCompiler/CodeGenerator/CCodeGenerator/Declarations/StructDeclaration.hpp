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

#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_DECLARATIONS_STRUCTDECLARATION_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_DECLARATIONS_STRUCTDECLARATION_HPP_
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/Declaration.hpp>
#include <vector>
namespace NES {
namespace QueryCompilation {

class StructDeclaration : public Declaration {
  public:
    static StructDeclaration create(const std::string& type_name, const std::string& variable_name);

    [[nodiscard]] const GeneratableDataTypePtr getType() const override;

    [[nodiscard]] const std::string getIdentifierName() const override;

    [[nodiscard]] const Code getTypeDefinitionCode() const override;

    [[nodiscard]] const Code getCode() const override;

    [[nodiscard]] static uint32_t getTypeSizeInBytes();

    [[nodiscard]] const std::string getTypeName() const;

    [[nodiscard]] const DeclarationPtr copy() const override;

    [[nodiscard]] DeclarationPtr getField(const std::string& field_name) const;

    [[nodiscard]] const bool containsField(const std::string& field_name, const DataTypePtr dataType) const;

    [[nodiscard]] VariableDeclaration getVariableDeclaration(const std::string& field_name) const;

    StructDeclaration& addField(const Declaration& decl);
    StructDeclaration& makeStructCompact();

    ~StructDeclaration() override;

  private:
    StructDeclaration(std::string type_name, std::string variable_name);
    std::string type_name_;
    std::string variable_name_;
    std::vector<DeclarationPtr> decls_;
    bool packed_struct_;
};
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_DECLARATIONS_STRUCTDECLARATION_HPP_
