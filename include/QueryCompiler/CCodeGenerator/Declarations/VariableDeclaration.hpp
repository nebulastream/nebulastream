#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_DECLARATIONS_VARIABLEDECLARATION_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_DECLARATIONS_VARIABLEDECLARATION_HPP_

#include <QueryCompiler/CCodeGenerator/Declarations/Declaration.hpp>

namespace NES {

class VariableDeclaration;
typedef std::shared_ptr<VariableDeclaration> VariableDeclarationPtr;

class VariableDeclaration : public Declaration {
  public:
    VariableDeclaration(const VariableDeclaration& var_decl);
    static VariableDeclaration create(DataTypePtr type, const std::string& identifier, ValueTypePtr value = nullptr);

    virtual const DataTypePtr getType() const override;
    virtual const std::string getIdentifierName() const override;

    const Code getTypeDefinitionCode() const override;

    const Code getCode() const override;

    const CodeExpressionPtr getIdentifier() const;

    const DataTypePtr getDataType() const;

    const DeclarationPtr copy() const override;

    virtual ~VariableDeclaration() override;

  private:
    VariableDeclaration(DataTypePtr type, const std::string& identifier, ValueTypePtr value = nullptr);
    DataTypePtr type_;
    std::string identifier_;
    ValueTypePtr init_value_;
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_DECLARATIONS_VARIABLEDECLARATION_HPP_
