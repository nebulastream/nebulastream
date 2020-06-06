#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_FORLOOPSTATEMENT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_FORLOOPSTATEMENT_HPP_
#include <QueryCompiler/CCodeGenerator/CompoundStatement.hpp>
#include <QueryCompiler/CCodeGenerator/ExpressionStatement.hpp>
namespace NES{

class ForLoopStatement : public Statement {
  public:
    ForLoopStatement(DeclarationPtr variableDeclarationPtr, ExpressionStatmentPtr condition,
                     ExpressionStatmentPtr advance,
                     const std::vector<StatementPtr>& loop_body = std::vector<StatementPtr>());

    virtual StatementType getStamentType() const;

    virtual const CodeExpressionPtr getCode() const;

    const StatementPtr createCopy() const override;

    void addStatement(StatementPtr stmt);

    const CompoundStatementPtr getCompoundStatement() ;

    virtual ~ForLoopStatement();

  private:
    DeclarationPtr varDeclaration;
    ExpressionStatmentPtr condition;
    ExpressionStatmentPtr advance;
    CompoundStatementPtr body;
};

typedef ForLoopStatement FOR;

}


#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_FORLOOPSTATEMENT_HPP_
