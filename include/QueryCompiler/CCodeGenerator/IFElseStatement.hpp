#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_IFELSESTATEMENT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_IFELSESTATEMENT_HPP_
#include <QueryCompiler/CCodeGenerator/Statement.hpp>
namespace NES {
class IfElseStatement : public Statement {
  public:
    IfElseStatement(const Statement& cond_true, const Statement& cond_false);

    StatementType getStamentType() const override;
    const CodeExpressionPtr getCode() const override;
    const StatementPtr createCopy() const override;
    ~IfElseStatement() override;
};
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_IFELSESTATEMENT_HPP_
