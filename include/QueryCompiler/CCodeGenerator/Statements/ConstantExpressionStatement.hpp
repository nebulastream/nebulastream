
#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_CONSTANTEXPRESSIONSTATEMENT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_CONSTANTEXPRESSIONSTATEMENT_HPP_

#include <QueryCompiler/CCodeGenerator/Statements/ExpressionStatement.hpp>

namespace NES {

class ConstantExpressionStatement : public ExpressionStatment {
  public:
    virtual StatementType getStamentType() const;

    virtual const CodeExpressionPtr getCode() const;

    virtual const ExpressionStatmentPtr copy() const;

    ConstantExpressionStatement(const ValueTypePtr& val);

    ConstantExpressionStatement(int32_t value);

    virtual ~ConstantExpressionStatement();

  private:
    ValueTypePtr constantValue;
};

typedef ConstantExpressionStatement Constant;

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_CONSTANTEXPRESSIONSTATEMENT_HPP_
