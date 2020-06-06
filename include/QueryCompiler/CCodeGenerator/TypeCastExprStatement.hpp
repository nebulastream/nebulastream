#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_TYPECASTEXPRSTATEMENT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_TYPECASTEXPRSTATEMENT_HPP_

#include <QueryCompiler/CCodeGenerator/ExpressionStatement.hpp>

namespace NES {

class TypeCastExprStatement : public ExpressionStatment {
  public:
    StatementType getStamentType() const;

    const CodeExpressionPtr getCode() const;

    const ExpressionStatmentPtr copy() const;

    TypeCastExprStatement(const ExpressionStatment& expr, const DataTypePtr& type);

    ~TypeCastExprStatement();

  private:
    ExpressionStatmentPtr expression;
    DataTypePtr dataType;
};

typedef TypeCastExprStatement TypeCast;

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_TYPECASTEXPRSTATEMENT_HPP_
