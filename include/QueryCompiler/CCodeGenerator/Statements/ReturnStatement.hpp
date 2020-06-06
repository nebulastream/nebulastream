#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_RETURNSTATEMENT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_RETURNSTATEMENT_HPP_

#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/VarRefStatement.hpp>
namespace NES {

class ReturnStatement : public Statement {
  public:
    ReturnStatement(VarRefStatement var_ref);

    StatementType getStamentType() const override;

    const CodeExpressionPtr getCode() const override;

    const StatementPtr createCopy() const override;

    ~ReturnStatement() override;

  private:
    VarRefStatement var_ref_;
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_RETURNSTATEMENT_HPP_
