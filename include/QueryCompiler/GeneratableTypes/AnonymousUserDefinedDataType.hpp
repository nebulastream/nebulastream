#ifndef NES_INCLUDE_QUERYCOMPILER_DATATYPES_ANONYMOUSUSERDEFINEDDATATYPE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_DATATYPES_ANONYMOUSUSERDEFINEDDATATYPE_HPP_

#include <memory>
#include <string>

#include <QueryCompiler/CCodeGenerator/Declarations/Declaration.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
#include <utility>

namespace NES {

class AnonymousUserDefinedDataType : public GeneratableDataType {
  public:
    AnonymousUserDefinedDataType(const std::string name);

    const CodeExpressionPtr getTypeDefinitionCode() const;
    CodeExpressionPtr generateCode() override;
    CodeExpressionPtr getDeclCode(std::string identifier) override ;
    const CodeExpressionPtr getCode() const override;
    ~AnonymousUserDefinedDataType();

  private:
    const std::string name;
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_DATATYPES_ANONYMOUSUSERDEFINEDDATATYPE_HPP_
