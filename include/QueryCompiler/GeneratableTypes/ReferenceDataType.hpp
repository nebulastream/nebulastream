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

#ifndef INCLUDE_REFEREMCEDATATYPE_HPP_
#define INCLUDE_REFEREMCEDATATYPE_HPP_

#pragma once

#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>

namespace NES {

/**
 * @brief A generatable data type which represent a reference to a typed value.
 */
class ReferenceDataType : public GeneratableDataType {
  public:
    ReferenceDataType(GeneratableDataTypePtr baseType);

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

  private:
    GeneratableDataTypePtr baseType;
};
}// namespace NES
#endif//INCLUDE_REFEREMCEDATATYPE_HPP_
