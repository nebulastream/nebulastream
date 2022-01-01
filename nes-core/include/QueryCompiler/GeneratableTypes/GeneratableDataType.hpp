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

#ifndef NES_INCLUDE_QUERY_COMPILER_GENERATABLE_TYPES_GENERATABLE_DATA_TYPE_HPP_
#define NES_INCLUDE_QUERY_COMPILER_GENERATABLE_TYPES_GENERATABLE_DATA_TYPE_HPP_

#include <QueryCompiler/CodeGenerator/CodeGeneratorForwardRef.hpp>
#include <memory>

namespace NES {
namespace QueryCompilation {

/**
 * @brief A generatable data type,
 * generate codes with regards to a particular data type.
 */
class GeneratableDataType {
  public:
    GeneratableDataType() = default;
    virtual ~GeneratableDataType() = default;

    /**
    * @brief Generates the code for the native type.
    * For instance int8_t, or uint32_t for BasicTypes or uint32_t[15] for an ArrayType.
    * @return CodeExpressionPtr
    */
    [[nodiscard]] virtual CodeExpressionPtr getCode() const = 0;

    /**
     * @brief Generated code for a type definition. This is mainly crucial for structures.
     * @return CodeExpressionPtr
     */
    [[nodiscard]] virtual CodeExpressionPtr getTypeDefinitionCode() const = 0;

    /**
    * @brief Generates the code for a type declaration with a specific identifier.
    * For instance "int8_t test", or "uint32_t test" for BasicTypes or "uint32_t test[15]" for an ArrayType.
    * @return CodeExpressionPtr
    */
    [[nodiscard]] virtual CodeExpressionPtr getDeclarationCode(std::string identifier) const = 0;

    /**
    * @brief Create copy assignment between two types.
    * @deprecated this will move to an own copy statement in the future.
    * @return CodeExpressionPtr
    */
    virtual StatementPtr getStmtCopyAssignment(const AssignmentStatement& assignmentStatement);
};
}// namespace QueryCompilation
}// namespace NES

#endif// NES_INCLUDE_QUERY_COMPILER_GENERATABLE_TYPES_GENERATABLE_DATA_TYPE_HPP_
