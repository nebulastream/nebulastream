
#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_CONSTANTEXPRESSIONSTATEMENT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_CONSTANTEXPRESSIONSTATEMENT_HPP_

#include <QueryCompiler/CCodeGenerator/ExpressionStatement.hpp>

namespace NES{

class ConstantExpressionStatement : public ExpressionStatment {
  public:

    virtual StatementType getStamentType() const;

    virtual const CodeExpressionPtr getCode() const;

    virtual const ExpressionStatmentPtr copy() const;

    ConstantExpressionStatement(const ValueTypePtr& val) ;

    ConstantExpressionStatement(const BasicType& type, const std::string& value) ;
    ConstantExpressionStatement(int32_t value);

    virtual ~ConstantExpressionStatement();

  private:
    ValueTypePtr constantValue;
};

typedef ConstantExpressionStatement Constant;

}

#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_CONSTANTEXPRESSIONSTATEMENT_HPP_
