#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_IFSTATEMENT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_IFSTATEMENT_HPP_
#include <QueryCompiler/CCodeGenerator/Statements/CompoundStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
namespace NES {

class IFStatement : public Statement {
  public:
    IFStatement(const Statement& cond_expr);

    IFStatement(const Statement& cond_expr, const Statement& cond_true_stmt);

    StatementType getStamentType() const override;

    const CodeExpressionPtr getCode() const override;

    const StatementPtr createCopy() const override;

    const CompoundStatementPtr getCompoundStatement();

    ~IFStatement() override;

  private:
    const StatementPtr conditionalExpression;
    CompoundStatementPtr trueCaseStatement;
};

typedef IFStatement IF;

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_IFSTATEMENT_HPP_
