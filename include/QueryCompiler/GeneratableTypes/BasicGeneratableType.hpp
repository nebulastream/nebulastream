#ifndef NES_INCLUDE_QUERYCOMPILER_DATATYPES_BASICGENERATABLETYPE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_DATATYPES_BASICGENERATABLETYPE_HPP_

#include <Common/DataTypes/DataType.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>

namespace NES {

class StructDeclaration;

/**
 * @brief A basic generatable data type which generates code for all BasicPhysicalTypes.
 */
class BasicGeneratableType : public GeneratableDataType {
  public:
    BasicGeneratableType(BasicPhysicalTypePtr type);

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
    BasicPhysicalTypePtr type;
};
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_DATATYPES_BASICGENERATABLETYPE_HPP_
