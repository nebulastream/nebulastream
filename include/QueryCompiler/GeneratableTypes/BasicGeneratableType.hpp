#ifndef NES_INCLUDE_QUERYCOMPILER_DATATYPES_BASICGENERATABLETYPE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_DATATYPES_BASICGENERATABLETYPE_HPP_

#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
#include <DataTypes/DataType.hpp>

namespace NES {

class StructDeclaration;

class BasicGeneratableType : public GeneratableDataType {
  public:
    BasicGeneratableType(BasicPhysicalTypePtr type);

    const CodeExpressionPtr getTypeDefinitionCode() const;
    const CodeExpressionPtr getCode() const;
    CodeExpressionPtr generateCode() override;
    CodeExpressionPtr getDeclCode(std::string identifier) override;

  private:
    BasicPhysicalTypePtr type;
};
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_DATATYPES_BASICGENERATABLETYPE_HPP_
