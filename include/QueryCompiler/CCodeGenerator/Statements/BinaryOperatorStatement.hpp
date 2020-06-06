
#pragma once

#include <memory>
#include <string>

#include <API/Types/DataTypes.hpp>
#include <Operators/OperatorTypes.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/VarRefStatement.hpp>
#include <QueryCompiler/CodeExpression.hpp>

namespace NES {

const CodeExpressionPtr toCodeExpression(const BinaryOperatorType& type);

class BinaryOperatorStatement : public ExpressionStatment {
  public:
    BinaryOperatorStatement(const ExpressionStatment& lhs, const BinaryOperatorType& op, const ExpressionStatment& rhs,
                            BracketMode bracket_mode = NO_BRACKETS);

    BinaryOperatorStatement addRight(const BinaryOperatorType& op, const VarRefStatement& rhs,
                                     BracketMode bracket_mode = NO_BRACKETS);

    StatementPtr assignToVariable(const VarRefStatement& lhs);

    virtual StatementType getStamentType() const;

    virtual const CodeExpressionPtr getCode() const;

    virtual const ExpressionStatmentPtr copy() const;

    //  BinaryOperatorStatement operator [](const ExpressionStatment &ref){
    //    return BinaryOperatorStatement(*this, ARRAY_REFERENCE_OP, ref);
    //  }

    virtual ~BinaryOperatorStatement();

  private:
    ExpressionStatmentPtr lhs_;
    ExpressionStatmentPtr rhs_;
    BinaryOperatorType op_;
    BracketMode bracket_mode_;
};

/** \brief small utility operator overloads to make code generation simpler and */

BinaryOperatorStatement assign(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator==(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator!=(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator<(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator<=(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator>(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator>=(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator+(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator-(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator*(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator/(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator%(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator&&(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator||(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator&(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator|(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator^(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator<<(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

BinaryOperatorStatement operator>>(const ExpressionStatment& lhs, const ExpressionStatment& rhs);

}// namespace NES
