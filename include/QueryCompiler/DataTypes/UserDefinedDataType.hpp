

#ifndef NES_INCLUDE_QUERYCOMPILER_DATATYPES_USERDEFINEDDATATYPE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_DATATYPES_USERDEFINEDDATATYPE_HPP_

#include <memory>
#include <string>

#include <QueryCompiler/CCodeGenerator/Declarations/StructDeclaration.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/DataTypes/GeneratableDataType.hpp>
#include <utility>

namespace NES {

class UserDefinedDataType : public GeneratableDataType {
  public:
    UserDefinedDataType(const StructDeclaration& decl);

    const CodeExpressionPtr getTypeDefinitionCode() const;
    const CodeExpressionPtr getCode() const;
    const CodeExpressionPtr getDeclCode(const std::string& identifier) const;
    CodeExpressionPtr generateCode() override;
    ~UserDefinedDataType();

  private:
    StructDeclaration declaration;
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_DATATYPES_USERDEFINEDDATATYPE_HPP_
