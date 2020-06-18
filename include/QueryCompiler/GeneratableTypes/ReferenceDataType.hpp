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
