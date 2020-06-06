#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_COMPOUNDSTATEMENT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_COMPOUNDSTATEMENT_HPP_

#include <QueryCompiler/CCodeGenerator/Statement.hpp>

namespace NES {

class CompoundStatement : public Statement {
  public:
    CompoundStatement();

    virtual StatementType getStamentType() const override;

    virtual const CodeExpressionPtr getCode() const override;

    const StatementPtr createCopy() const override;

    void addStatement(StatementPtr stmt);

    virtual ~CompoundStatement() override;

  private:
    std::vector<StatementPtr> statements;
};

typedef std::shared_ptr<CompoundStatement> CompoundStatementPtr;

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_COMPOUNDSTATEMENT_HPP_
