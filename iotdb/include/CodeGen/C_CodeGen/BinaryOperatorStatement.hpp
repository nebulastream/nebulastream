
#pragma once

#include <string>
#include <memory>

#include <Core/DataTypes.hpp>

#include <CodeGen/CodeExpression.hpp>
#include <CodeGen/C_CodeGen/Statement.hpp>

#include <Util/ErrorHandling.hpp>

namespace iotdb{

enum BinaryOperatorType {
  EQUAL_OP,
  UNEQUAL_OP,
  LESS_THEN_OP,
  LESS_THEN_EQUAL_OP,
  GREATER_THEN_OP,
  GREATER_THEN_EQUAL_OP,
  PLUS_OP,
  MINUS_OP,
  MULTIPLY_OP,
  DIVISION_OP,
  MODULO_OP,
  LOGICAL_AND_OP,
  LOGICAL_OR_OP,
  BITWISE_AND_OP,
  BITWISE_OR_OP,
  BITWISE_XOR_OP,
  BITWISE_LEFT_SHIFT_OP,
  BITWISE_RIGHT_SHIFT_OP,
  ASSIGNMENT_OP,
  ARRAY_REFERENCE_OP,
  MEMBER_SELECT_POINTER_OP,
  MEMBER_SELECT_REFERENCE_OP
};

const std::string toString(const BinaryOperatorType &type);

const CodeExpressionPtr toCodeExpression(const BinaryOperatorType &type);


class BinaryOperatorStatement : public ExpressionStatment {
public:
  BinaryOperatorStatement(const ExpressionStatment &lhs, const BinaryOperatorType &op, const ExpressionStatment &rhs,
                          BracketMode bracket_mode = NO_BRACKETS);

  BinaryOperatorStatement addRight(const BinaryOperatorType &op, const VarRefStatement &rhs,
                                   BracketMode bracket_mode = NO_BRACKETS);

  StatementPtr assignToVariable(const VarRefStatement &lhs);

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

BinaryOperatorStatement assign(const ExpressionStatment &lhs, const ExpressionStatment &rhs);

BinaryOperatorStatement operator ==(const ExpressionStatment &lhs, const ExpressionStatment &rhs);

BinaryOperatorStatement operator !=(const ExpressionStatment &lhs, const ExpressionStatment &rhs);

BinaryOperatorStatement operator <(const ExpressionStatment &lhs, const ExpressionStatment &rhs);

BinaryOperatorStatement operator <=(const ExpressionStatment &lhs, const ExpressionStatment &rhs);

BinaryOperatorStatement operator >(const ExpressionStatment &lhs, const ExpressionStatment &rhs);

BinaryOperatorStatement operator >=(const ExpressionStatment &lhs, const ExpressionStatment &rhs);

BinaryOperatorStatement operator +(const ExpressionStatment &lhs, const ExpressionStatment &rhs);

BinaryOperatorStatement operator -(const ExpressionStatment &lhs, const ExpressionStatment &rhs);

BinaryOperatorStatement operator *(const ExpressionStatment &lhs, const ExpressionStatment &rhs);

BinaryOperatorStatement operator /(const ExpressionStatment &lhs, const ExpressionStatment &rhs);

BinaryOperatorStatement operator %(const ExpressionStatment &lhs, const ExpressionStatment &rhs);

BinaryOperatorStatement operator &&(const ExpressionStatment &lhs, const ExpressionStatment &rhs);

BinaryOperatorStatement operator ||(const ExpressionStatment &lhs, const ExpressionStatment &rhs);

BinaryOperatorStatement operator &(const ExpressionStatment &lhs, const ExpressionStatment &rhs);

BinaryOperatorStatement operator |(const ExpressionStatment &lhs, const ExpressionStatment &rhs);

BinaryOperatorStatement operator ^(const ExpressionStatment &lhs, const ExpressionStatment &rhs);

BinaryOperatorStatement operator <<(const ExpressionStatment &lhs, const ExpressionStatment &rhs);

BinaryOperatorStatement operator >>(const ExpressionStatment &lhs, const ExpressionStatment &rhs);


}

