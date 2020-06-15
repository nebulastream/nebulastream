#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_FUNCTIONCALLSTATEMENT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_FUNCTIONCALLSTATEMENT_HPP_
#include <QueryCompiler/CCodeGenerator/Statements/ExpressionStatement.hpp>
#include <vector>
namespace NES {

class FunctionCallStatement : public ExpressionStatment {
  public:
    FunctionCallStatement(const std::string functionname);

    virtual StatementType getStamentType() const;

    virtual const CodeExpressionPtr getCode() const;

    virtual const ExpressionStatmentPtr copy() const;

    virtual void addParameter(const ExpressionStatment& expr);
    virtual void addParameter(ExpressionStatmentPtr expr);

    virtual ~FunctionCallStatement();

  private:
    std::string functionName;
    std::vector<ExpressionStatmentPtr> expressions;
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_FUNCTIONCALLSTATEMENT_HPP_
