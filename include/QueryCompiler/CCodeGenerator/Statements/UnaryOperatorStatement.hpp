

#pragma once

#include <memory>
#include <string>

#include <Operators/OperatorTypes.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/ExpressionStatement.hpp>
#include <QueryCompiler/CodeExpression.hpp>

#include <API/Types/DataTypes.hpp>

namespace NES {

const CodeExpressionPtr toCodeExpression(const UnaryOperatorType& type);

class UnaryOperatorStatement : public ExpressionStatment {
  public:
    UnaryOperatorStatement(const ExpressionStatment& expr, const UnaryOperatorType& op,
                           BracketMode bracket_mode = NO_BRACKETS);

    StatementType getStamentType() const override;

    const CodeExpressionPtr getCode() const override;

    const ExpressionStatmentPtr copy() const override;

    ~UnaryOperatorStatement() override;

  private:
    ExpressionStatmentPtr expr_;
    UnaryOperatorType op_;
    BracketMode bracket_mode_;
};

UnaryOperatorStatement operator&(const ExpressionStatment& ref);

UnaryOperatorStatement operator*(const ExpressionStatment& ref);

UnaryOperatorStatement operator++(const ExpressionStatment& ref);

UnaryOperatorStatement operator--(const ExpressionStatment& ref);

UnaryOperatorStatement operator~(const ExpressionStatment& ref);

UnaryOperatorStatement operator!(const ExpressionStatment& ref);

UnaryOperatorStatement sizeOf(const ExpressionStatment& ref);

}// namespace NES
