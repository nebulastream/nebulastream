#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_DECLARATIONS_FUNCTIONDECLARATION_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_DECLARATIONS_FUNCTIONDECLARATION_HPP_

#include <QueryCompiler/CCodeGenerator/Declarations/Declaration.hpp>

namespace NES {

class FunctionDeclaration : public Declaration {
  private:
    Code function_code;

  public:
    FunctionDeclaration(Code code);

    virtual const GeneratableDataTypePtr getType() const override;
    virtual const std::string getIdentifierName() const override;

    const Code getTypeDefinitionCode() const override;

    const Code getCode() const override;
    const DeclarationPtr copy() const override;
};
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_DECLARATIONS_FUNCTIONDECLARATION_HPP_
