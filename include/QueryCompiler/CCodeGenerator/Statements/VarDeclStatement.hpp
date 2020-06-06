#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_VARDECLSTATEMENT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_VARDECLSTATEMENT_HPP_

#include <QueryCompiler/CCodeGenerator/Statements/ExpressionStatement.hpp>

namespace NES {

class VarDeclStatement : public ExpressionStatment {
  public:
    VarDeclStatement(const VariableDeclaration& var_decl);

    virtual StatementType getStamentType() const;

    virtual const CodeExpressionPtr getCode() const;

    virtual const ExpressionStatmentPtr copy() const;

    virtual ~VarDeclStatement();

  private:
    std::shared_ptr<VariableDeclaration> variableDeclaration;
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_VARDECLSTATEMENT_HPP_
