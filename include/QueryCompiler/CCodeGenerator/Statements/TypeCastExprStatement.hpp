#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_TYPECASTEXPRSTATEMENT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_TYPECASTEXPRSTATEMENT_HPP_

#include <QueryCompiler/CCodeGenerator/Statements/ExpressionStatement.hpp>

namespace NES {

class GeneratableDataType;
typedef std::shared_ptr<GeneratableDataType> GeneratableDataTypePtr;

class TypeCastExprStatement : public ExpressionStatment {
  public:
    StatementType getStamentType() const;

    const CodeExpressionPtr getCode() const;

    const ExpressionStatmentPtr copy() const;

    TypeCastExprStatement(const ExpressionStatment& expr, GeneratableDataTypePtr type);

    ~TypeCastExprStatement();

  private:
    ExpressionStatmentPtr expression;
    GeneratableDataTypePtr dataType;
};

typedef TypeCastExprStatement TypeCast;

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_TYPECASTEXPRSTATEMENT_HPP_
