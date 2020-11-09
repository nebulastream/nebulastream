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

#ifndef NES_INCLUDE_QUERYCOMPILER_DATATYPES_USERDEFINEDDATATYPE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_DATATYPES_USERDEFINEDDATATYPE_HPP_

#include <QueryCompiler/CCodeGenerator/Declarations/StructDeclaration.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
#include <memory>
#include <string>
#include <utility>

namespace NES {

/**
 * @brief Generated code for structure declarations
 */
class UserDefinedDataType : public GeneratableDataType {
  public:
    UserDefinedDataType(const StructDeclaration& decl);

    /**
    * @brief Generated code for a type definition. This is mainly crucial for structures.
    * @return CodeExpressionPtr
    */
    const CodeExpressionPtr getTypeDefinitionCode() const override;

    /**
    * @brief Generates the code for the native type.
    * For instance int8_t, or uint32_t for BasicTypes or uint32_t[15] for an ArrayType.
    * @return CodeExpressionPtr
    */
    const CodeExpressionPtr getCode() const override;

    /**
    * @brief Generates the code for a type declaration with a specific identifier.
    * For instance "int8_t test", or "uint32_t test" for BasicTypes or "uint32_t test[15]" for an ArrayType.
    * @return CodeExpressionPtr
    */
    CodeExpressionPtr getDeclarationCode(std::string identifier) override;

    ~UserDefinedDataType();

  private:
    StructDeclaration declaration;
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_DATATYPES_USERDEFINEDDATATYPE_HPP_
