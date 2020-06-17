#ifndef NES_INCLUDE_QUERYCOMPILER_DATATYPES_ARRAYGENERATABLETYPE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_DATATYPES_ARRAYGENERATABLETYPE_HPP_

#include <QueryCompiler/DataTypes/GeneratableDataType.hpp>
#include <DataTypes/DataType.hpp>

namespace NES {

class StructDeclaration;

class Array;
typedef std::shared_ptr<Array> ArrayPtr;

class ArrayPhysicalType;
typedef std::shared_ptr<ArrayPhysicalType> ArrayPhysicalTypePtr;

class ArrayGeneratableType : public GeneratableDataType {
  public:
    ArrayGeneratableType(ArrayPhysicalTypePtr type, GeneratableDataTypePtr component);

    const CodeExpressionPtr getTypeDefinitionCode() const override;
    const CodeExpressionPtr getCode() const override;
    CodeExpressionPtr generateCode() override;
    CodeExpressionPtr getDeclCode(std::string identifier) override;

  private:
    ArrayPhysicalTypePtr type;
    GeneratableDataTypePtr component;

};
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_DATATYPES_ARRAYGENERATABLETYPE_HPP_
