#ifndef NES_INCLUDE_QUERYCOMPILER_DATATYPES_ARRAYGENERATABLETYPE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_DATATYPES_ARRAYGENERATABLETYPE_HPP_

#include <DataTypes/DataType.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>

namespace NES {

class StructDeclaration;

class Array;
typedef std::shared_ptr<Array> ArrayPtr;

class ArrayPhysicalType;
typedef std::shared_ptr<ArrayPhysicalType> ArrayPhysicalTypePtr;

/**
 * @brief A generatable type that generates code for arrays.
 */
class ArrayGeneratableType : public GeneratableDataType {
  public:
    ArrayGeneratableType(ArrayPhysicalTypePtr type, GeneratableDataTypePtr component);

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

    /**
   * @brief Create copy assignment between two types.
   * @deprecated this will move to an own copy statement in the future.
   * @return CodeExpressionPtr
   */
    StatementPtr getStmtCopyAssignment(const AssignmentStatment& aParam) override;

  private:
    ArrayPhysicalTypePtr type;
    GeneratableDataTypePtr component;
};
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_DATATYPES_ARRAYGENERATABLETYPE_HPP_
