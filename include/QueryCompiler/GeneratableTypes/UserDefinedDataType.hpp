#ifndef NES_INCLUDE_QUERYCOMPILER_DATATYPES_USERDEFINEDDATATYPE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_DATATYPES_USERDEFINEDDATATYPE_HPP_

#include <memory>
#include <string>

#include <QueryCompiler/CCodeGenerator/Declarations/StructDeclaration.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
#include <utility>

namespace NES {

class UserDefinedDataType : public GeneratableDataType {
  public:
    UserDefinedDataType(const StructDeclaration& decl);

    const CodeExpressionPtr getTypeDefinitionCode() const override;
    const CodeExpressionPtr getCode() const override;
    CodeExpressionPtr getDeclCode(std::string identifier) override;
    CodeExpressionPtr generateCode() override;
    ~UserDefinedDataType();

  private:
    StructDeclaration declaration;
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_DATATYPES_USERDEFINEDDATATYPE_HPP_
